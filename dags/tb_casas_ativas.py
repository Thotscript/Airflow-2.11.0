import json
from urllib.parse import quote_plus

import requests
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from airflow.models import Variable
from airflow import DAG
from airflow.decorators import task
import pendulum


# =========================
# Configs de Banco (MySQL)
# =========================
DB_USER = "root"
DB_PASS = "Tfl1234@"         # quote_plus cuida de caracteres especiais
DB_HOST = "host.docker.internal"
DB_PORT = 3306
DB_NAME = "ovh_silver"

# =========================
# Configs da API Streamline
# =========================
STREAMLINE_URL = "https://web.streamlinevrs.com/api/json"
TOKEN_KEY = Variable.get("STREAMLINE_TOKEN_KEY")
TOKEN_SECRET = Variable.get("STREAMLINE_TOKEN_SECRET")
HEADERS = {"Content-Type": "application/json"}

# =========================
# Tabela de destino (MySQL)
# =========================
DEST_TABLE = "tb_active_houses"


def fetch_property_list_wordpress() -> pd.DataFrame:
    payload = {
        "methodName": "GetPropertyListWordPress",
        "params": {
            "token_key": TOKEN_KEY,
            "token_secret": TOKEN_SECRET
        }
    }
    resp = requests.post(STREAMLINE_URL, data=json.dumps(payload), headers=HEADERS, timeout=60)
    resp.raise_for_status()
    data_dict = resp.json()

    if "data" not in data_dict or "property" not in data_dict["data"]:
        raise ValueError("Resposta da API não contém 'data.property'.")

    df = pd.json_normalize(data_dict["data"]["property"])

    # Caso queira enxugar colunas/linhas:
    # selected_columns = ["id", "name", "bedrooms_number", "bathrooms_number", "max_adults", "renting_type"]
    # df = df.loc[:, selected_columns]
    # df = df[df['renting_type'] == 'RENTING']

    return df


def sanitize_for_sql(df: pd.DataFrame) -> pd.DataFrame:
    """
    Converte valores não-escalares (dict, list, set, tuple) para JSON string.
    Mantém os demais tipos intactos.
    """
    def is_non_scalar(v):
        return isinstance(v, (dict, list, set, tuple))

    out = df.copy()
    converted_cols = []
    for col in out.columns:
        if out[col].dtype == "object":
            if out[col].map(is_non_scalar).any():
                out[col] = out[col].apply(
                    lambda v: json.dumps(list(v), ensure_ascii=False)
                    if isinstance(v, set)
                    else (json.dumps(v, ensure_ascii=False) if is_non_scalar(v) else v)
                )
                converted_cols.append(col)
    if converted_cols:
        print(f"[sanitize_for_sql] Colunas convertidas para JSON string: {converted_cols}")
    return out


def main():
    # ---- Engine MySQL ----
    engine = create_engine(
        f"mysql+pymysql://{DB_USER}:{quote_plus(DB_PASS)}@{DB_HOST}:{DB_PORT}/{DB_NAME}?charset=utf8mb4",
        pool_pre_ping=True,
    )
    try:
        # ---- Chama API e normaliza ----
        df = fetch_property_list_wordpress()
        if df.empty:
            print("Aviso: API retornou zero propriedades. Encerrando.")
            return

        # (Opcional) tentar converter colunas que parecem datas
        for col in df.columns:
            if df[col].dtype == "object":
                # tenta conversão "best effort"; se não der, mantém
                try:
                    converted = pd.to_datetime(df[col], errors="ignore")
                    df[col] = converted
                except Exception:
                    pass

        # ---- Sanitização para evitar "dict can not be used as parameter" ----
        df = sanitize_for_sql(df)

        # ---- Grava no MySQL (transacional) ----
        try:
            with engine.begin() as conn:
                df.to_sql(
                    DEST_TABLE,
                    con=conn,
                    if_exists="replace",   # use "append" se quiser incremental
                    index=False,
                    chunksize=50_000,
                    method="multi"
                )
            print(f"Dados gravados com sucesso em {DEST_TABLE}. Linhas: {len(df)}")
        except SQLAlchemyError as e:
            # Mostra o erro real (se alguma coluna/valor ainda estiver problemático)
            print("Erro ao gravar no MySQL via to_sql:", e)
            raise
    finally:
        engine.dispose()


# =================
# Airflow DAG
# =================
SP_TZ = pendulum.timezone("America/Sao_Paulo")

with DAG(
    dag_id="OVH-tb_active_houses",
    start_date=pendulum.datetime(2025, 9, 23, 8, 0, tz=SP_TZ),
    schedule="0 5 * * *",   # diariamente às 05:00
    catchup=False,
    tags=["Tabelas - OVH"],
) as dag:

    @task()
    def build_active_houses():
        main()

    build_active_houses()
