import json
import time
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
from collections import deque

import pandas as pd
import requests
from sqlalchemy import create_engine, text, inspect
from urllib.parse import quote_plus
import pendulum
from airflow import DAG
from airflow.decorators import task

# ---------------------------------------------------------
# CONFIG STREAMLINE (inline)
# ---------------------------------------------------------
API_URL = "https://web.streamlinevrs.com/api/json"
TOKEN_KEY = "3ef223d3bbf7086cfb86df7e98d6e5d2"
TOKEN_SECRET = "a88d05b895affb815cc8a4d96670698ee486ea30"

# ---------------------------------------------------------
# CONFIG BANCO MYSQL
# ---------------------------------------------------------
DB_USER = "root"
DB_PASS = "Tfl1234@"   # será escapada na URL
DB_HOST = "host.docker.internal"
DB_PORT = 3306
DB_NAME = "ovh_silver"

# ---------------------------------------------------------
# CONTROLE DE THROTTLE SIMPLES (SEM LIB EXTERNA)
# ---------------------------------------------------------
WINDOW_SEC = 60
MAX_CALLS = 95
_call_times = deque()


def throttle():
    """
    Controle de taxa: máximo ~MAX_CALLS requisições em WINDOW_SEC segundos.
    Sem libs externas.
    """
    now = time.time()
    _call_times.append(now)

    # remove chamadas mais antigas que WINDOW_SEC
    while _call_times and (now - _call_times[0]) > WINDOW_SEC:
        _call_times.popleft()

    if len(_call_times) >= MAX_CALLS:
        sleep_for = WINDOW_SEC - (now - _call_times[0])
        if sleep_for > 0:
            print(f"[THROTTLE] Atingiu limite, dormindo {sleep_for:.2f}s")
            time.sleep(sleep_for)


# ---------------------------------------------------------
# FUNÇÃO PARA BUSCAR DADOS NO STREAMLINE
# ---------------------------------------------------------
headers = {
    "Content-Type": "application/json"
}


def get_unit_data(id_unit, admin, today, end_date, now_str):
    """
    Chama a API do Streamline para obter as rates de uma unidade.
    Usa throttle manual para respeitar limite de requisições.
    """
    data = {
        "methodName": "GetPropertyRates",
        "params": {
            "token_key": TOKEN_KEY,
            "token_secret": TOKEN_SECRET,
            "unit_id": str(id_unit),
            "rate_types": {
                "id": 8730
            },
            "startdate": today,
            "enddate": end_date
        }
    }

    # throttle manual
    throttle()

    response = requests.post(API_URL, data=json.dumps(data), headers=headers)
    response.raise_for_status()

    data_dict = response.json()

    tb_referrer = pd.json_normalize(data_dict["data"])
    tb_referrer["id_unit"] = id_unit
    tb_referrer["Administradora"] = admin
    tb_referrer["data_price"] = now_str

    return tb_referrer


# ---------------------------------------------------------
# FUNÇÃO PRINCIPAL (CHAMADA PELA DAG)
# ---------------------------------------------------------
def main():
    start_time = time.time()

    # monta string de conexão do MySQL
    db_pass_enc = quote_plus(DB_PASS)
    conn_str = (
        f"mysql+pymysql://{DB_USER}:{db_pass_enc}"
        f"@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    )

    engine = create_engine(conn_str)
    inspector = inspect(engine)

    # verifica existência das tabelas antes de deletar
    metas_exists = inspector.has_table("tb_metas")
    metas_unit_exists = inspector.has_table("tb_metas_unit_id")

    with engine.begin() as connection:
        if metas_exists:
            connection.execute(text("DELETE FROM tb_metas"))
        if metas_unit_exists:
            connection.execute(text("DELETE FROM tb_metas_unit_id"))

    # 2) Lê os dados do Google Sheets (em vez do Excel local)
    sheet_url = (
        "https://docs.google.com/spreadsheets/"
        "d/16pqFr2eFEGGjzsAFMU8pKjlQOVgEEkzo8-78QZ1sXyM/export?format=xlsx"
    )
    df_upload_old = pd.read_excel(sheet_url)

    # 3) Grava em tb_metas
    df_upload_old.to_sql("tb_metas", engine, if_exists="append", index=False)

    # cria df com apenas id_unit para tb_metas_unit_id
    unique_column_df = df_upload_old.drop_duplicates(subset=["id_unit"])
    unique_column_df = unique_column_df[["id_unit"]]
    unique_column_df.to_sql("tb_metas_unit_id", engine, if_exists="append", index=False)

    # depois de gravar, atualiza info de existência (no caso da primeira vez)
    inspector = inspect(engine)
    metas_unit_exists = inspector.has_table("tb_metas_unit_id")

    # -----------------------------------------------------
    # LOOP PRINCIPAL
    # -----------------------------------------------------
    for i in range(15000):
        print(f"Iteração {i}")

        # 4) Monta query dependendo da existência de tb_metas_unit_id
        if metas_unit_exists:
            # usa filtro de já carregados
            query = """
                SELECT A.id, A.Administradora
                FROM tb_property_list_wordpress AS A
                LEFT JOIN (
                    SELECT DISTINCT id_unit, 'SIM' AS CARREGADO
                    FROM tb_metas_unit_id
                ) AS B ON A.id = B.id_unit
                WHERE B.CARREGADO IS NULL
                  AND A.renting_type = 'RENTING'
            """
        else:
            # PRIMEIRA VEZ: ignora o filtro pois a tabela ainda não existe
            query = """
                SELECT A.id, A.Administradora
                FROM tb_property_list_wordpress AS A
                WHERE A.renting_type = 'RENTING'
            """

        df = pd.read_sql_query(query, engine)

        # se não há mais unidades, encerra
        if df.empty:
            print("Nenhuma unidade pendente. Encerrando loop.")
            break

        # processa apenas 1 por iteração (igual ao original)
        df = df.head(1)

        # datas para a API
        today_date = datetime(2022, 1, 1)
        today = today_date.strftime("%m/%d/%Y")
        now_str = datetime.now().strftime("%Y/%m/%d %H:%M:%S")
        end_date = (datetime.now() + timedelta(days=547)).strftime("%Y/%m/%d")

        params_list = [(row.id, row.Administradora) for row in df.itertuples(index=False)]

        # 5) Executa chamadas em paralelo na API
        with ThreadPoolExecutor(max_workers=4) as executor:
            result_list = executor.map(
                lambda x: get_unit_data(x[0], x[1], today, end_date, now_str),
                params_list
            )
            # pequena pausa extra para aliviar pressão
            time.sleep(0.6)

        # 6) Concatena resultados
        tb_referrer = pd.concat(result_list, ignore_index=True)
        print(tb_referrer.head(10))

        # 7) Grava em tb_metas
        tb_referrer.to_sql("tb_metas", engine, if_exists="append", index=False)

        # 8) Atualiza tb_metas_unit_id (vai criando/atualizando conforme roda)
        unique_column_df = tb_referrer.drop_duplicates(subset=["id_unit"])
        unique_column_df = unique_column_df[["id_unit"]]
        unique_column_df.to_sql("tb_metas_unit_id", engine, if_exists="append", index=False)

    engine.dispose()

    print("Dados Gravados Com Sucesso")
    end_time = time.time()
    execution_time = end_time - start_time
    print(f"Tempo de execução: {execution_time:.2f} segundos")


# ---------------------------------------------------------
# DEFINIÇÃO DA DAG
# ---------------------------------------------------------
SP_TZ = pendulum.timezone("America/Sao_Paulo")

with DAG(
    dag_id="OVH-tb_metas",
    start_date=pendulum.datetime(2025, 9, 23, 8, 0, tz=SP_TZ),
    schedule="15 5 * * *",
    catchup=False,
    tags=["Tabelas - OVH"],
) as dag:

    @task()
    def tb_metas():
        main()

    tb_metas()
