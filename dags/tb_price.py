import json
from datetime import datetime, timedelta

import pandas as pd
import requests
from sqlalchemy import create_engine
from urllib.parse import quote_plus
from airflow import DAG
from airflow.decorators import task
import pendulum

def main():
    # --- CONFIG BÁSICA ---
    DB_USER = "root"
    DB_PASS = "Tfl1234@"              
    DB_HOST = "host.docker.internal"
    DB_PORT = 3306
    DB_NAME = "ovh_silver"

    API_URL = "https://web.streamlinevrs.com/api/json"
    TOKEN_KEY = "a43cb1b5ed27cce283ab2bb4df540037"
    TOKEN_SECRET = "72c7b8d5ba4b0ef14fe97a7e4179bdfa92cfc6ea"

    # --- DATAS ---
    today = datetime.now().strftime('%Y/%m/%d')
    now_str = datetime.now().strftime('%Y/%m/%d %H:%M:%S')
    end_date = (datetime.now() + timedelta(days=547)).strftime('%Y/%m/%d')

    # --- ENGINE (já com o database definido) ---
    engine = create_engine(
        f"mysql+pymysql://{DB_USER}:{quote_plus(DB_PASS)}@{DB_HOST}:{DB_PORT}/{DB_NAME}?charset=utf8mb4"
    )

    # --- LER UNIDADES ---
    df_property = pd.read_sql(
        "SELECT id, Administradora FROM tb_property_list_wordpress WHERE id <> 346521",
        con=engine
    )

    # --- LOOP MAIS SIMPLES POSSÍVEL (sequencial) ---
    frames = []
    headers = {"Content-Type": "application/json"}

    for row in df_property.itertuples(index=False):
        payload = {
            "methodName": "GetPropertyRates",
            "params": {
                "token_key": TOKEN_KEY,
                "token_secret": TOKEN_SECRET,
                "unit_id": int(row.id),
                "startdate": today,
                "enddate": end_date
            }
        }

        resp = requests.post(API_URL, data=json.dumps(payload), headers=headers, timeout=60)
        data = resp.json()

        if isinstance(data, dict) and data.get("data"):
            df = pd.json_normalize(data["data"])
            df["id_unit"] = int(row.id)
            df["data_price"] = now_str
            df["Administradora"] = row.Administradora
            frames.append(df)

    # --- CONCATENAR E GRAVAR ---
    if frames:
        tb_price_total = pd.concat(frames, ignore_index=True)
        tb_price_total.to_sql("tb_price_history", con=engine, if_exists="append", index=False)
        tb_price_total.to_sql("tb_price",        con=engine, if_exists="replace", index=False)

    engine.dispose()
    print("Tabela salva no banco!")


if __name__ == "__main__":
    main()

SP_TZ = pendulum.timezone("America/Sao_Paulo")

with DAG(
    dag_id="OVH-tb_price",
    start_date=pendulum.datetime(2025, 9, 23, 8, 0, tz=SP_TZ),
    schedule="0 5 * * *",
    catchup=False,
    tags=["Tabelas - OVH"],
) as dag:

    @task()
    def tb_price():
        main()
        
    tb_price()
