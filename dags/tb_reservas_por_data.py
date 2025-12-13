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
        # Timeout menor aqui é seguro porque as queries serão rápidas
        connect_args={"init_command": "SET SESSION innodb_lock_wait_timeout=60"} 
    )

    BATCH_SIZE = 1000   # Processamos 1000 reservas por vez (seguro para memória)
    total_gravado = 0
    first_run = True

    try:
        print("[main] Etapa 1: Buscando lista de IDs para processar...")
        
        # 1. Busca APENAS os IDs (Leve e Rápido)
        # Isso carrega apenas uma lista de inteiros na memória
        ids_query = "SELECT id FROM tb_reservas WHERE status_id IN (4,5,6,7)"
        df_ids = pd.read_sql(ids_query, engine)
        
        all_ids = df_ids["id"].tolist()
        total_ids = len(all_ids)
        print(f"[main] Total de reservas a processar: {total_ids}")

        if total_ids == 0:
            print("Nenhuma reserva encontrada. Encerrando.")
            return

        # 2. Itera sobre a lista de IDs em blocos (fatiamento de lista)
        # range(start, stop, step)
        for i in range(0, total_ids, BATCH_SIZE):
            # Pega o lote atual de IDs (ex: 1000 IDs)
            batch_ids = all_ids[i : i + BATCH_SIZE]
            
            # Transforma a lista [1, 2, 3] em string "1, 2, 3" para o SQL
            # (Seguro aqui pois são inteiros vindos do próprio banco)
            ids_string = ",".join(map(str, batch_ids))

            # 3. Query FOCADA apenas nesses IDs
            # O MySQL resolve isso instantaneamente pois usa o Índice Primário
            sql_query = f"""
                SELECT 
                    r.*, 
                    p.* FROM tb_reservas r
                LEFT JOIN tb_reservas_price_day p ON r.id = p.reservation_id
                WHERE r.id IN ({ids_string})
            """

            # Lê os dados detalhados deste pequeno lote
            df_batch = pd.read_sql(sql_query, engine)
            
            # Limpeza de colunas duplicadas pelo JOIN
            df_batch = df_batch.loc[:, ~df_batch.columns.duplicated()]

            # Processamento de Negócio
            df_batch = process_chunk(df_batch)
            dtype = build_dtype_map(df_batch)

            # Define modo de escrita
            # Se for o primeiro lote de todos -> replace (limpa a tabela)
            # Todos os subsequentes -> append
            mode = "replace" if first_run else "append"

            # Gravação
            with engine.begin() as conn:
                df_batch.to_sql(
                    TABLE_OUT,
                    con=conn,
                    if_exists=mode,
                    index=False,
                    method="multi", 
                    chunksize=BATCH_SIZE, # Escreve o lote inteiro de uma vez
                    dtype=dtype,
                )

            total_gravado += len(df_batch)
            first_run = False
            
            # Log de progresso claro
            percent = round((i + BATCH_SIZE) / total_ids * 100, 2)
            if percent > 100: percent = 100
            print(f"[Progresso] Lote {i // BATCH_SIZE + 1} processado. Total linhas gravadas: {total_gravado} ({percent}%)")

            # Pausa leve para não saturar CPU
            time.sleep(0.2)

        print(f"✅ Processo concluído com sucesso! Total final: {total_gravado}")

    except SQLAlchemyError as e:
        print("❌ Erro de Banco de Dados:")
        print(e)
        traceback.print_exc()
        raise
    except Exception as e:
        print("❌ Erro Geral:")
        traceback.print_exc()
        raise
    finally:
        engine.dispose()


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
