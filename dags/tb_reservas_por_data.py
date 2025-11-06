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

SQL_RESERVAS = """
SELECT *
FROM tb_reservas
WHERE status_id IN (4,5,6,7);
"""

SQL_PRECOS = """
SELECT *
FROM tb_reservas_price_day;
"""

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
            # tenta converter assumindo o padrão ISO ou BR, e depois fallback
            try:
                out[col] = pd.to_datetime(out[col], format="%Y-%m-%d %H:%M:%S", errors="coerce")
            except Exception:
                try:
                    out[col] = pd.to_datetime(out[col], format="%d/%m/%Y %H:%M:%S", errors="coerce")
                except Exception:
                    out[col] = pd.to_datetime(out[col], errors="coerce")

    # remove timezone se houver
    for col in out.select_dtypes(include=["datetimetz"]).columns:
        out[col] = out[col].dt.tz_convert("UTC").dt.tz_localize(None)

    return out

def force_base_nulls(df: pd.DataFrame) -> pd.DataFrame:
    # Converte NaN/NaT para None para o driver
    return df.where(pd.notnull(df), None)

def build_dtype_map(df: pd.DataFrame):
    """
    Define tipos para MySQL. Tudo que for 'object' vai como TEXT para evitar
    problemas de serialização; datetime64 como DATETIME; ints e floats mapeados.
    """
    dtype = {}
    for col, dt in df.dtypes.items():
        if str(dt).startswith("datetime64"):
            dtype[col] = DateTime()
        elif str(dt).startswith("int"):
            # Tentar BigInteger quando parecer grande
            dtype[col] = BigInteger() if "64" in str(dt) else Integer()
        elif str(dt).startswith("float"):
            dtype[col] = Float()
        elif str(dt) == "bool":
            dtype[col] = Boolean()
        elif str(dt) == "object":
            dtype[col] = Text()
        # demais tipos (category etc.) deixamos o pandas decidir
    return dtype

def main():
    engine = create_engine(
        f"mysql+pymysql://{DB_USER}:{quote_plus(DB_PASS)}@{DB_HOST}:{DB_PORT}/{DB_NAME}?charset=utf8mb4",
        pool_pre_ping=True,
    )
    try:
        reservas = pd.read_sql(SQL_RESERVAS, con=engine)
        precos   = pd.read_sql(SQL_PRECOS,   con=engine)

        if reservas.empty:
            print("Aviso: tb_reservas não retornou linhas com status_id em (4,5,6,7). Encerrando.")
            return

        resultado = pd.merge(
            reservas,
            precos,
            left_on="id",
            right_on="reservation_id",
            how="left",
            suffixes=("", "_price"),
            copy=False
        )

        # casas espelhos
        casas_espelhos = {
            567515: 451446, 747169: 345511, 588400: 747861, 588406: 747861, 441403: 747861,
            614452: 341242, 747735: 341242, 747171: 379860, 379240: 747177, 747215: 334111,
            747220: 380233, 614526: 488453, 747747: 488453, 614527: 488453, 575254: 511116,
            747224: 445230, 747230: 334112, 563466: 590935, 747235: 443999, 614955: 495437,
            747754: 495437, 747249: 598346, 747092: 379248, 747254: 425652, 746111: 514404,
            735680: 498139
        }
        if "unit_id" in resultado.columns:
            resultado["unit_id"] = pd.to_numeric(resultado["unit_id"], errors="coerce").astype("Int64")
            resultado["unit_id"] = resultado["unit_id"].replace(casas_espelhos)

        # normalizações
        resultado = normalize_datetimes(resultado)
        resultado = sanitize_non_scalars(resultado)
        resultado = force_base_nulls(resultado)

        # mapeia dtype explícito
        dtype = build_dtype_map(resultado)

        table_name = "tb_reservas_por_data"
        try:
            # Dica: use method=None (sem multi) para capturar o erro raiz de forma mais clara.
            with engine.begin() as conn:
                resultado.to_sql(
                    table_name,
                    con=conn,
                    if_exists="replace",
                    index=False,
                    chunksize=None,   # um único insert por linha (diagnóstico)
                    method=None,      # <— troque para "multi" quando estabilizar
                    dtype=dtype
                )
            print(f"Dados Gravados Com Sucesso em {table_name}. Linhas: {len(resultado)}")
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
