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

# =========================
# SQL com colunas únicas (corrigido)
# =========================
SQL_FINAL = """
SELECT 
    -- Campos da tb_reservas
    r.id AS r_id,
    r.status_id AS r_status_id,
    r.user_id AS r_user_id,
    r.unit_id AS r_unit_id,
    r.start_date AS r_start_date,
    r.end_date AS r_end_date,
    r.created_at AS r_created_at,
    r.updated_at AS r_updated_at,
    r.total_price AS r_total_price,
    r.currency AS r_currency,
    r.channel AS r_channel,
    r.notes AS r_notes,

    -- Campos da tb_reservas_price_day
    p.id AS p_id,
    p.reservation_id AS p_reservation_id,
    p.date AS p_date,
    p.price AS p_price,
    p.currency AS p_currency,
    p.created_at AS p_created_at,
    p.updated_at AS p_updated_at

FROM tb_reservas AS r
LEFT JOIN tb_reservas_price_day AS p
  ON r.id = p.reservation_id
WHERE r.status_id IN (4,5,6,7);
"""


# =========================
# Funções utilitárias
# =========================
def sanitize_non_scalars(df: pd.DataFrame) -> pd.DataFrame:
    def is_non_scalar(v):
        return isinstance(v, (dict, list, set, tuple))

    out = df.copy()
    conv = []
    for col in out.columns:
        if out[col].dtype == "object":
            if out[col].map(is_non_scalar).any():
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
    out = df.copy()
    for col in out.columns:
        if out[col].dtype == "object":
            try:
                out[col] = pd.to_datetime(out[col], format="%Y-%m-%d %H:%M:%S", errors="coerce")
            except Exception:
                try:
                    out[col] = pd.to_datetime(out[col], format="%d/%m/%Y %H:%M:%S", errors="coerce")
                except Exception:
                    out[col] = pd.to_datetime(out[col], errors="coerce")

    for col in out.select_dtypes(include=["datetimetz"]).columns:
        out[col] = out[col].dt.tz_convert("UTC").dt.tz_localize(None)
    return out


def force_base_nulls(df: pd.DataFrame) -> pd.DataFrame:
    return df.where(pd.notnull(df), None)


def build_dtype_map(df: pd.DataFrame):
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


# =========================
# Função principal
# =========================
def main():
    engine = create_engine(
        f"mysql+pymysql://{DB_USER}:{quote_plus(DB_PASS)}@{DB_HOST}:{DB_PORT}/{DB_NAME}?charset=utf8mb4",
        pool_pre_ping=True,
    )

    table_name = "tb_reservas_por_data"
    CHUNK_SIZE = 50000

    try:
        print("[main] Iniciando leitura em chunks e merge SQL...")
        chunks = pd.read_sql(SQL_FINAL, con=engine, chunksize=CHUNK_SIZE)

        total_linhas = 0
        for i, chunk in enumerate(chunks):
            print(f"[chunk {i}] Linhas recebidas: {len(chunk)}")

            # --- Normalizações e sanitização ---
            chunk = normalize_datetimes(chunk)
            chunk = sanitize_non_scalars(chunk)
            chunk = force_base_nulls(chunk)

            # --- Substitui casas espelhos ---
            casas_espelhos = {
                567515: 451446, 747169: 345511, 588400: 747861, 588406: 747861, 441403: 747861,
                614452: 341242, 747735: 341242, 747171: 379860, 379240: 747177, 747215: 334111,
                747220: 380233, 614526: 488453, 747747: 488453, 614527: 488453, 575254: 511116,
                747224: 445230, 747230: 334112, 563466: 590935, 747235: 443999, 614955: 495437,
                747754: 495437, 747249: 598346, 747092: 379248, 747254: 425652, 746111: 514404,
                735680: 498139
            }
            if "r_unit_id" in chunk.columns:
                chunk["r_unit_id"] = pd.to_numeric(chunk["r_unit_id"], errors="coerce").astype("Int64")
                chunk["r_unit_id"] = chunk["r_unit_id"].replace(casas_espelhos)

            dtype = build_dtype_map(chunk)

            # --- Grava incrementalmente ---
            with engine.begin() as conn:
                chunk.to_sql(
                    table_name,
                    con=conn,
                    if_exists="replace" if i == 0 else "append",
                    index=False,
                    chunksize=1000,
                    method="multi",
                    dtype=dtype
                )

            total_linhas += len(chunk)
            print(f"[chunk {i}] Gravado com sucesso. Total acumulado: {total_linhas}")

        print(f"[main] Processo concluído. Linhas totais gravadas: {total_linhas}")

    except SQLAlchemyError as e:
        print("Erro ao gravar no MySQL via to_sql (root cause abaixo):")
        print(e)
        traceback.print_exc()
        raise
    finally:
        engine.dispose()


# =================
# Airflow DAG
# =================
SP_TZ = pendulum.timezone("America/Sao_Paulo")

with DAG(
    dag_id="OVH-tb_reservas_por_data",
    start_date=pendulum.datetime(2025, 9, 23, 8, 0, tz=SP_TZ),
    schedule="0 5 * * *",
    catchup=False,
    tags=["Tabelas - OVH"],
) as dag:

    @task()
    def build_tb_reservas_por_data():
        main()

    build_tb_reservas_por_data()
