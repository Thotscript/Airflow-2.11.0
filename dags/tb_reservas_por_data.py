import time
import json
import traceback
from urllib.parse import quote_plus

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.types import Text, DateTime, Float, BigInteger, Integer, Boolean

from airflow import DAG
from airflow.decorators import task
import pendulum


# =========================
# Configs de Banco (MySQL)
# =========================
DB_USER = "root"
DB_PASS = "Tfl1234@"
DB_HOST = "host.docker.internal"
DB_PORT = 3306
DB_NAME = "ovh_silver"

TABLE_OUT = "tb_reservas_por_data"


# =========================
# Funções utilitárias
# =========================
def sanitize_non_scalars(df: pd.DataFrame) -> pd.DataFrame:
    """Converte listas, dicts e sets em JSON strings"""
    def is_non_scalar(v):
        return isinstance(v, (dict, list, set, tuple))

    out = df.copy()
    conv = []
    for col in out.columns:
        if out[col].dtype == "object" and out[col].map(is_non_scalar).any():
            out[col] = out[col].apply(
                lambda v: json.dumps(list(v), ensure_ascii=False)
                if isinstance(v, set)
                else (json.dumps(v, ensure_ascii=False) if is_non_scalar(v) else v)
            )
            conv.append(col)
    if conv:
        print(f"[sanitize_non_scalars] Colunas convertidas para JSON string: {conv}")
    return out


def normalize_datetimes(df: pd.DataFrame) -> pd.DataFrame:
    """Converte colunas de texto em datetime, tolerando múltiplos formatos"""
    out = df.copy()
    for col in out.columns:
        if out[col].dtype == "object":
            out[col] = pd.to_datetime(out[col], errors="coerce", utc=False)
    for col in out.select_dtypes(include=["datetimetz"]).columns:
        out[col] = out[col].dt.tz_convert("UTC").dt.tz_localize(None)
    return out


def force_base_nulls(df: pd.DataFrame) -> pd.DataFrame:
    """Substitui NaN por None"""
    return df.where(pd.notnull(df), None)


def build_dtype_map(df: pd.DataFrame):
    """Mapeia tipos pandas → tipos SQLAlchemy"""
    dtype = {}
    for col, dt in df.dtypes.items():
        if str(dt).startswith("datetime64"):
            dtype[col] = DateTime()
        elif str(dt).startswith("int"):
            dtype[col] = BigInteger() if "64" in str(dt) else Integer()
        elif str(dt).startswith("float"):
            dtype[col] = Float()
        elif str(dt) == "bool":
            dtype[col] = Boolean()
        elif str(dt) == "object":
            dtype[col] = Text()
    return dtype


def process_chunk(chunk: pd.DataFrame) -> pd.DataFrame:
    """Aplica as transformações específicas de negócio"""
    # Normalizações
    chunk = normalize_datetimes(chunk)
    chunk = sanitize_non_scalars(chunk)
    chunk = force_base_nulls(chunk)

    # Casas espelhos
    casas_espelhos = {
        567515: 451446, 747169: 345511, 588400: 747861, 588406: 747861, 441403: 747861,
        614452: 341242, 747735: 341242, 747171: 379860, 379240: 747177, 747215: 334111,
        747220: 380233, 614526: 488453, 747747: 488453, 614527: 488453, 575254: 511116,
        747224: 445230, 747230: 334112, 563466: 590935, 747235: 443999, 614955: 495437,
        747754: 495437, 747249: 598346, 747092: 379248, 747254: 425652, 746111: 514404,
        735680: 498139
    }
    if "unit_id" in chunk.columns:
        chunk["unit_id"] = pd.to_numeric(chunk["unit_id"], errors="coerce").astype("Int64")
        chunk["unit_id"] = chunk["unit_id"].replace(casas_espelhos)

    # Atualização de creation_date para IDs específicos
    novas_datas = {
        33956627: "2023-10-21 00:00:00",
        33967753: "2023-07-11 00:00:00",
        33967744: "2023-06-28 00:00:00",
        33967723: "2023-05-16 00:00:00",
        33967713: "2023-05-05 00:00:00",
        33967706: "2023-04-27 00:00:00",
    }
    if "creation_date" in chunk.columns:
        for id_val, nova_data in novas_datas.items():
            chunk.loc[chunk["id"] == id_val, "creation_date"] = nova_data

    return chunk


# =========================
# Função principal com CHUNKS + RATE LIMIT
# =========================
def main():
    engine = create_engine(
        f"mysql+pymysql://{DB_USER}:{quote_plus(DB_PASS)}@{DB_HOST}:{DB_PORT}/{DB_NAME}?charset=utf8mb4",
        pool_pre_ping=True,
        # Opcional: Aumentar timeout da sessão se o banco estiver muito lento
        connect_args={"init_command": "SET SESSION innodb_lock_wait_timeout=120"} 
    )

    CHUNK_SIZE = 50000        # Leitura do banco (SELECT)
    WRITE_SIZE = 1000         # REDUZIDO: Escrita no banco (INSERT) - evita travar a tabela
    RATE_LIMIT = 0.5          
    total_linhas = 0

    try:
        print("[main] Iniciando processo...")

        # --- PASSO 1: Preparar a tabela de destino (Truncate/Drop) ---
        # Fazemos isso ANTES de começar a ler os dados pesados para evitar locks cruzados.
        # Se for a primeira execução do dia, queremos limpar a tabela.
        # Como o Pandas 'replace' recria a tabela e define os tipos, podemos fazer um dummy write 
        # ou usar execute direto se a tabela já existir e quisermos apenas limpar.
        
        # Estratégia Segura: Usar o Pandas para recriar a estrutura com 0 linhas primeiro
        # ou apenas Truncar se a estrutura for fixa. 
        # Vamos assumir que você quer recriar a estrutura baseada no DataFrame:
        
        # Vamos deixar o Pandas criar a tabela na primeira iteração, MAS com um lote pequeno
        # ou tratar o 'replace' fora do loop principal se possível.
        # No seu caso, como os dados vêm em chunks, a melhor abordagem segura é:
        # Usar "replace" APENAS no primeiro chunk, mas com WRITE_SIZE menor.
        
        first_chunk = True 

        # Lê tabela de preços (memória)
        precos = pd.read_sql("SELECT * FROM tb_reservas_price_day", engine)

        # Lê reservas em blocos
        reservas_chunks = pd.read_sql(
            "SELECT * FROM tb_reservas WHERE status_id IN (4,5,6,7)",
            con=engine,
            chunksize=CHUNK_SIZE,
        )

        for i, reservas in enumerate(reservas_chunks):
            print(f"[chunk {i}] Lendo {len(reservas)} linhas...")

            merged = pd.merge(
                reservas,
                precos,
                left_on="id",
                right_on="reservation_id",
                how="left",
            )

            merged = process_chunk(merged)
            dtype = build_dtype_map(merged)

            # --- Loop de Escrita ---
            # Aqui iteramos sobre o dataframe processado em pedaços menores
            for start in range(0, len(merged), WRITE_SIZE):
                end = start + WRITE_SIZE
                batch = merged.iloc[start:end]

                # Lógica Crítica:
                # Se for o PRIMEIRO lote do PRIMEIRO chunk => replace (cria a tabela)
                # Qualquer outro lote => append
                mode = "replace" if (first_chunk and start == 0) else "append"

                with engine.begin() as conn:
                    batch.to_sql(
                        TABLE_OUT,
                        con=conn,
                        if_exists=mode,
                        index=False,
                        chunksize=WRITE_SIZE, # Garante que o pandas respeite o tamanho
                        method="multi",       # Mantém performance, mas com lote menor
                        dtype=dtype,
                    )
                
                # Se rodou o primeiro lote com sucesso, nunca mais usamos replace
                if mode == "replace":
                    first_chunk = False 

                total_linhas += len(batch)
                print(f"[chunk {i}] Gravado lote {start}-{end}. Total: {total_linhas}")
                
                time.sleep(RATE_LIMIT)

        print(f"✅ Processo concluído. Linhas totais gravadas: {total_linhas}")

    except SQLAlchemyError as e:
        print("❌ Erro ao gravar no MySQL via to_sql:")
        print(e)
        traceback.print_exc()
        raise
    finally:
        engine.dispose()
        print("🔒 Conexão encerrada.")


# =========================
# Airflow DAG
# =========================
SP_TZ = pendulum.timezone("America/Sao_Paulo")

with DAG(
    dag_id="OVH-tb_reservas_por_data",
    start_date=pendulum.datetime(2025, 9, 23, 8, 0, tz=SP_TZ),
    schedule="0 5 * * *",  # executa todo dia às 5h
    catchup=False,
    tags=["Tabelas - OVH"],
) as dag:

    @task()
    def build_tb_reservas_por_data():
        main()

    build_tb_reservas_por_data()
