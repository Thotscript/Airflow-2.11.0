import json
from urllib.parse import quote_plus

import requests
import pandas as pd
from sqlalchemy import create_engine

from airflow import DAG
from airflow.decorators import task
import pendulum


# =========================
# Configs de Banco (MySQL)
# =========================
DB_USER = "root"
DB_PASS = "Tfl1234@"         # se tiver caracteres especiais, o quote_plus abaixo cuida
DB_HOST = "host.docker.internal"
DB_PORT = 3306
DB_NAME = "ovh_silver"       # ajuste se necessário

# =========================
# Configs da API Streamline
# =========================
STREAMLINE_URL = "https://web.streamlinevrs.com/api/json"
TOKEN_KEY     = "a43cb1b5ed27cce283ab2bb4df540037"
TOKEN_SECRET  = "72c7b8d5ba4b0ef14fe97a7e4179bdfa92cfc6ea"

HEADERS = {"Content-Type": "application/json"}

# =========================
# Tabela de destino (MySQL)
# =========================
DEST_TABLE = "tb_active_houses"


def fetch_property_list_wordpress() -> pd.DataFrame:
    """
    Chama o endpoint GetPropertyListWordPress e retorna um DataFrame normalizado.
    """
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

    # validações básicas
    if "data" not in data_dict or "property" not in data_dict["data"]:
        raise ValueError("Resposta da API não contém 'data.property'.")

    df = pd.json_normalize(data_dict["data"]["property"])

    # --- Ative se quiser restringir colunas / linhas, como no seu código original ---
    # selected_columns = ["id", "name", "bedrooms_number", "bathrooms_number", "max_adults", "renting_type"]
    # df = df.loc[:, selected_columns]
    # df = df[df['renting_type'] == 'RENTING']

    return df


def main():
    # ---- Cria engine MySQL ----
    engine = create_engine(
        f"mysql+pymysql://{DB_USER}:{quote_plus(DB_PASS)}@{DB_HOST}:{DB_PORT}/{DB_NAME}?charset=utf8mb4"
    )

    try:
        # ---- Chama API e normaliza ----
        property_list_wordpress = fetch_property_list_wordpress()

        if property_list_wordpress.empty:
            print("Aviso: API retornou zero propriedades. Encerrando.")
            return

        # ---- Grava no MySQL ----
        property_list_wordpress.to_sql(
            DEST_TABLE,
            con=engine,
            if_exists="replace",   # use "append" se preferir inserir incrementalmente
            index=False,
            chunksize=50_000,
            method="multi"
        )

        print(f"Dados gravados com sucesso em {DEST_TABLE}. Linhas: {len(property_list_wordpress)}")

    finally:
        engine.dispose()


# =================
# Airflow DAG
# =================
SP_TZ = pendulum.timezone("America/Sao_Paulo")

with DAG(
    dag_id="OVH-tb_active_houses",
    start_date=pendulum.datetime(2025, 9, 23, 8, 0, tz=SP_TZ),
    schedule="0 5 * * *",   # diariamente às 05:00 (ajuste se quiser)
    catchup=False,
    tags=["Tabelas - OVH"],
) as dag:

    @task()
    def build_active_houses():
        main()

    build_active_houses()
