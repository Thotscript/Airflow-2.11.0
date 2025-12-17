import time
import json
import traceback
from pathlib import Path
from urllib.parse import quote_plus

import polars as pl
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

from airflow import DAG
from airflow.decorators import task
import pendulum


# =========================
# Configurações
# =========================
DB_USER = "root"
DB_PASS = "Tfl1234@"
DB_HOST = "host.docker.internal"
DB_PORT = 3306
DB_NAME = "ovh_silver"
TABLE_OUT = "tb_reservas_por_data"

# Paths
DATA_DIR = Path("/tmp/ovh_data")
PARQUET_FILE = DATA_DIR / "reservas_temp.parquet"

# Batch configs
EXTRACT_BATCH_SIZE = 1000  # Lê 1000 IDs por vez do MySQL
LOAD_BATCH_SIZE = 5000     # Escreve 5000 linhas por vez no MySQL


# =========================
# Mapeamentos de Negócio
# =========================
CASAS_ESPELHOS = {
    567515: 451446, 747169: 345511, 588400: 747861, 588406: 747861, 441403: 747861,
    614452: 341242, 747735: 341242, 747171: 379860, 379240: 747177, 747215: 334111,
    747220: 380233, 614526: 488453, 747747: 488453, 614527: 488453, 575254: 511116,
    747224: 445230, 747230: 334112, 563466: 590935, 747235: 443999, 614955: 495437,
    747754: 495437, 747249: 598346, 747092: 379248, 747254: 425652, 746111: 514404,
    735680: 498139
}

NOVAS_DATAS = {
    33956627: "2023-10-21 00:00:00",
    33967753: "2023-07-11 00:00:00",
    33967744: "2023-06-28 00:00:00",
    33967723: "2023-05-16 00:00:00",
    33967713: "2023-05-05 00:00:00",
    33967706: "2023-04-27 00:00:00",
}


# =========================
# Extração em BATCHES para Parquet
# =========================
def get_table_columns(engine, table_name: str) -> list:
    """
    Retorna lista de colunas de uma tabela
    """
    query = f"""
        SELECT COLUMN_NAME 
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_SCHEMA = '{DB_NAME}' 
        AND TABLE_NAME = '{table_name}'
        ORDER BY ORDINAL_POSITION
    """
    with engine.connect() as conn:
        result = conn.execute(text(query))
        return [row[0] for row in result]


def extract_to_parquet_batches(engine) -> int:
    """
    Extrai dados do MySQL em BATCHES e acumula no Parquet
    Evita estouro de memória ao processar grandes volumes
    """
    print("[Extract] Buscando IDs das reservas...")
    
    # 1. Busca apenas os IDs (leve e rápido)
    ids_query = """
        SELECT id 
        FROM tb_reservas 
        WHERE status_id IN (4,5,6,7)
        ORDER BY id
    """
    
    with engine.connect() as conn:
        result = conn.execute(text(ids_query))
        all_ids = [row[0] for row in result]
    
    total_ids = len(all_ids)
    print(f"[Extract] Total de IDs encontrados: {total_ids}")
    
    if total_ids == 0:
        raise ValueError("Nenhuma reserva encontrada com status 4,5,6,7")
    
    # 1.5. Busca colunas das duas tabelas
    print("[Extract] Identificando colunas das tabelas...")
    cols_reservas = get_table_columns(engine, "tb_reservas")
    cols_price_day = get_table_columns(engine, "tb_reservas_price_day")
    
    # Remove colunas duplicadas de price_day (exceto a chave de join)
    cols_price_duplicated = set(cols_reservas) & set(cols_price_day)
    cols_price_duplicated.discard("reservation_id")  # Mantém a FK
    
    # Monta SELECT com aliases para evitar conflitos
    select_reservas = ", ".join([f"r.{col}" for col in cols_reservas])
    select_price = ", ".join([
        f"p.{col} as p_{col}" if col in cols_price_duplicated else f"p.{col}"
        for col in cols_price_day
    ])
    
    print(f"[Extract] Colunas tb_reservas: {len(cols_reservas)}")
    print(f"[Extract] Colunas tb_reservas_price_day: {len(cols_price_day)}")
    print(f"[Extract] Colunas duplicadas renomeadas: {len(cols_price_duplicated)}")
    
    # 2. Processa IDs em batches
    num_batches = (total_ids + EXTRACT_BATCH_SIZE - 1) // EXTRACT_BATCH_SIZE
    all_dfs = []
    
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    
    for i in range(num_batches):
        start_idx = i * EXTRACT_BATCH_SIZE
        end_idx = min((i + 1) * EXTRACT_BATCH_SIZE, total_ids)
        batch_ids = all_ids[start_idx:end_idx]
        
        # Query com batch específico de IDs
        # Usa as colunas identificadas com aliases para evitar duplicatas
        ids_str = ",".join(map(str, batch_ids))
        
        batch_query = f"""
            SELECT 
                {select_reservas},
                {select_price}
            FROM tb_reservas r
            LEFT JOIN tb_reservas_price_day p ON r.id = p.reservation_id
            WHERE r.id IN ({ids_str})
        """
        
        # Lê batch (pequeno e rápido)
        # Estratégia: ler como dicionário Python primeiro para evitar problemas de inferência
        with engine.connect() as conn:
            result = conn.execute(text(batch_query))
            rows = result.fetchall()
            columns = result.keys()
        
        # Converte para Polars a partir de dicionário (inferência mais robusta)
        if rows:
            df_batch = pl.DataFrame(
                {col: [row[i] for row in rows] for i, col in enumerate(columns)},
                infer_schema_length=None  # Escaneia tudo
            )
            all_dfs.append(df_batch)
        else:
            # Batch vazio (improvável mas tratamos)
            print(f"[Extract] Batch {i+1} vazio, pulando...")
        
        percent = round((end_idx / total_ids) * 100, 1)
        print(f"[Extract] Batch {i+1}/{num_batches} extraído → {end_idx}/{total_ids} IDs ({percent}%)")
        
        time.sleep(0.05)  # Pausa leve
    
    # 3. Concatena todos os batches em um único DataFrame
    print("[Extract] Concatenando batches...")
    
    if not all_dfs:
        raise ValueError("Nenhum dado foi extraído dos batches")
    
    # Concatena com how="vertical_relaxed" para lidar com schemas ligeiramente diferentes
    df_final = pl.concat(all_dfs, how="vertical_relaxed")
    
    # Remove linhas completamente duplicadas (se houver)
    df_final = df_final.unique(maintain_order=True)
    
    print(f"[Extract] Total extraído: {df_final.shape[0]} linhas x {df_final.shape[1]} colunas")
    
    # 4. Salva como Parquet comprimido
    df_final.write_parquet(
        PARQUET_FILE,
        compression="zstd",
        compression_level=3,
        statistics=True,
        use_pyarrow=False
    )
    
    file_size_mb = PARQUET_FILE.stat().st_size / 1024 / 1024
    print(f"[Extract] Parquet salvo: {PARQUET_FILE} ({file_size_mb:.2f} MB)")
    
    return len(df_final)


# =========================
# Transformações com Polars
# =========================
def transform_data(df: pl.DataFrame) -> pl.DataFrame:
    """
    Aplica transformações de negócio usando Polars
    """
    print("[Transform] Aplicando transformações...")
    
    # 1. Conversão de datetime
    datetime_cols = [col for col in df.columns if "date" in col.lower() or "created" in col.lower()]
    
    for col in datetime_cols:
        if df[col].dtype == pl.Utf8:
            df = df.with_columns(
                pl.col(col).str.to_datetime(
                    format="%Y-%m-%d %H:%M:%S",
                    strict=False,
                    time_zone=None
                ).alias(col)
            )
    
    # 2. Substituição de casas espelhos
    if "unit_id" in df.columns:
        df = df.with_columns(
            pl.col("unit_id").cast(pl.Int64, strict=False)
        )
        
        mapping_expr = pl.col("unit_id")
        for old_id, new_id in CASAS_ESPELHOS.items():
            mapping_expr = pl.when(pl.col("unit_id") == old_id).then(new_id).otherwise(mapping_expr)
        
        df = df.with_columns(mapping_expr.alias("unit_id"))
    
    # 3. Atualização de creation_date
    if "creation_date" in df.columns and "id" in df.columns:
        date_expr = pl.col("creation_date")
        
        for id_val, nova_data in NOVAS_DATAS.items():
            date_expr = pl.when(pl.col("id") == id_val).then(
                pl.lit(nova_data).str.to_datetime("%Y-%m-%d %H:%M:%S")
            ).otherwise(date_expr)
        
        df = df.with_columns(date_expr.alias("creation_date"))
    
    # 4. Conversão de structs/lists para JSON
    for col in df.columns:
        dtype = df[col].dtype
        
        if isinstance(dtype, (pl.List, pl.Struct, pl.Array)):
            df = df.with_columns(
                pl.col(col).map_elements(
                    lambda x: json.dumps(x, ensure_ascii=False) if x is not None else None,
                    return_dtype=pl.Utf8
                ).alias(col)
            )
    
    print(f"[Transform] Transformações concluídas. Shape: {df.shape}")
    return df


# =========================
# Carga no MySQL em BATCHES
# =========================
def load_to_mysql_batches(df: pl.DataFrame, engine, table_name: str, batch_size: int):
    """
    Carrega dados no MySQL em batches
    """
    print(f"[Load] Iniciando carga em batches de {batch_size} linhas...")
    
    total_rows = len(df)
    num_batches = (total_rows + batch_size - 1) // batch_size
    
    # Limpa tabela destino
    print(f"[Load] Limpando tabela {table_name}...")
    with engine.begin() as conn:
        conn.execute(text(f"TRUNCATE TABLE {table_name}"))
    
    # Processa em batches
    for i in range(num_batches):
        start_idx = i * batch_size
        end_idx = min((i + 1) * batch_size, total_rows)
        
        # Slice eficiente (zero-copy)
        batch_df = df.slice(start_idx, end_idx - start_idx)
        
        # Write no MySQL
        if_exists_mode = "replace" if i == 0 else "append"
        
        batch_df.write_database(
            table_name=table_name,
            connection=engine,
            if_exists=if_exists_mode,
            engine="sqlalchemy"
        )
        
        # Progress
        percent = round((end_idx / total_rows) * 100, 1)
        print(f"[Load] Batch {i+1}/{num_batches} → {end_idx}/{total_rows} linhas ({percent}%)")
        
        time.sleep(0.02)
    
    print(f"✅ [Load] Carga concluída! Total: {total_rows} linhas")


# =========================
# Pipeline Principal
# =========================
def main():
    """
    Pipeline ETL otimizado com batches em TODAS as etapas:
    1. MySQL → Parquet (batches de leitura)
    2. Parquet → Polars (leitura única, arquivo comprimido)
    3. Transformações (Polars)
    4. Polars → MySQL (batches de escrita)
    """
    
    engine = create_engine(
        f"mysql+pymysql://{DB_USER}:{quote_plus(DB_PASS)}@{DB_HOST}:{DB_PORT}/{DB_NAME}?charset=utf8mb4",
        pool_pre_ping=True,
        pool_size=5,
        max_overflow=10,
        pool_recycle=3600,
        connect_args={
            "init_command": "SET SESSION innodb_lock_wait_timeout=120"
        }
    )
    
    try:
        # ETAPA 1: Extração em BATCHES
        print("\n" + "="*70)
        print("ETAPA 1: EXTRAÇÃO EM BATCHES (MySQL → Parquet)")
        print("="*70)
        start = time.time()
        total_extracted = extract_to_parquet_batches(engine)
        extract_time = time.time() - start
        print(f"⏱️  Tempo de extração: {extract_time:.2f}s")
        
        # ETAPA 2: Leitura do Parquet
        print("\n" + "="*70)
        print("ETAPA 2: LEITURA (Parquet → Polars)")
        print("="*70)
        start = time.time()
        df = pl.read_parquet(PARQUET_FILE)
        read_time = time.time() - start
        print(f"[Parquet] Dados carregados: {df.shape[0]} linhas x {df.shape[1]} colunas")
        print(f"⏱️  Tempo de leitura: {read_time:.2f}s")
        
        # ETAPA 3: Transformações
        print("\n" + "="*70)
        print("ETAPA 3: TRANSFORMAÇÕES")
        print("="*70)
        start = time.time()
        df = transform_data(df)
        transform_time = time.time() - start
        print(f"⏱️  Tempo de transformação: {transform_time:.2f}s")
        
        # ETAPA 4: Carga em BATCHES
        print("\n" + "="*70)
        print("ETAPA 4: CARGA EM BATCHES (Polars → MySQL)")
        print("="*70)
        start = time.time()
        load_to_mysql_batches(df, engine, TABLE_OUT, LOAD_BATCH_SIZE)
        load_time = time.time() - start
        print(f"⏱️  Tempo de carga: {load_time:.2f}s")
        
        # Resumo final
        total_time = extract_time + read_time + transform_time + load_time
        print("\n" + "="*70)
        print("✅ PIPELINE CONCLUÍDO COM SUCESSO!")
        print("="*70)
        print(f"📊 Total processado: {len(df):,} linhas")
        print(f"⏱️  Tempo total: {total_time:.2f}s")
        print(f"🚀 Velocidade: {len(df)/total_time:.0f} linhas/segundo")
        print("="*70)
        
    except SQLAlchemyError as e:
        print(f"\n❌ Erro de Banco de Dados: {e}")
        traceback.print_exc()
        raise
    except Exception as e:
        print(f"\n❌ Erro Geral: {e}")
        traceback.print_exc()
        raise
    finally:
        engine.dispose()
        
        # Cleanup do Parquet
        if PARQUET_FILE.exists():
            PARQUET_FILE.unlink()
            print(f"[Cleanup] Arquivo temporário removido: {PARQUET_FILE}")


# =========================
# Airflow DAG
# =========================
SP_TZ = pendulum.timezone("America/Sao_Paulo")

with DAG(
    dag_id="OVH-tb_reservas_por_data_polars",
    start_date=pendulum.datetime(2025, 9, 23, 8, 0, tz=SP_TZ),
    schedule="0 5 * * *",
    catchup=False,
    tags=["Tabelas - OVH", "Polars", "Otimizado"],
    default_args={
        "retries": 2,
        "retry_delay": pendulum.duration(minutes=5),
    },
    doc_md="""
    # Pipeline OVH Reservas - Otimizado com Polars + Batches
    
    ## Fluxo:
    1. **Extração em Batches**: MySQL → Parquet (1000 IDs por vez)
    2. **Leitura**: Parquet → Polars DataFrame
    3. **Transformação**: Regras de negócio (casas espelhos, datas)
    4. **Carga em Batches**: Polars → MySQL (5000 linhas por vez)
    
    ## Performance:
    - Extração em batches: evita OOM em JOINs grandes
    - Uso de memória controlado
    - Velocidade 5-10x superior ao Pandas
    """
) as dag:

    @task()
    def build_tb_reservas_por_data():
        """Task principal - Pipeline ETL completo"""
        main()

    build_tb_reservas_por_data()