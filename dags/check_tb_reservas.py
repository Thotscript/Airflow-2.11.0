import pendulum
import pandas as pd
from sqlalchemy import create_engine
from urllib.parse import quote_plus
from airflow import DAG
from airflow.decorators import task

SP_TZ = pendulum.timezone("America/Sao_Paulo")

def get_engine(database: str):
    DB_USER = "root"
    DB_PASS = "Tfl1234@"
    DB_HOST = "host.docker.internal"
    DB_PORT = 3306
    url = f"mysql+pymysql://{DB_USER}:{quote_plus(DB_PASS)}@{DB_HOST}:{DB_PORT}/{database}?charset=utf8mb4"
    return create_engine(url, pool_pre_ping=True, future=True)

def connection_df(database: str, table_name: str):
    engine = get_engine(database)
    with engine.begin() as conn:
        return pd.read_sql(f"SELECT * FROM {table_name};", conn)

def replace_silver_table(engine, df_bronze, table_name):
    df_bronze.to_sql(name=table_name, con=engine, if_exists='replace', index=False)

def comp_linhas(df1: pd.DataFrame, df2: pd.DataFrame) -> bool:
    i_df_silver = len(df2)
    i_df_bronze = len(df1)
    if i_df_bronze < i_df_silver:
        return False
    if i_df_silver == 0:
        return i_df_bronze == 0
    diferenca_percentual = (i_df_bronze - i_df_silver) / i_df_silver
    return diferenca_percentual <= 0.05

with DAG(
    dag_id="OVH-Check-tb_reservas",
    start_date=pendulum.datetime(2025, 9, 23, 8, 0, tz=SP_TZ),
    schedule=None,          # será acionado pelo TriggerDagRunOperator
    catchup=False,
    tags=["Check - tb_reservas"],
) as dag:

    @task
    def check_data_quality() -> bool:
        df_silver = connection_df("ovh_silver", "tb_reservas")
        df_bronze = connection_df("ovh_bronze", "tb_reservas")

        # colunas
        if set(df_bronze.columns) != set(df_silver.columns):
            print(f"[COLUMNS] Divergentes: silver={len(df_silver.columns)} bronze={len(df_bronze.columns)}")
            return False
        print("[COLUMNS] OK")

        # linhas
        if not comp_linhas(df_bronze, df_silver):
            print("[LINHAS] Bronze->Silver acima de 5%")
            return False
        print("[LINHAS] OK")

        # preço total (tolerância 10%)
        total_silver = df_silver.get('price_nightly', pd.Series(dtype=float)).sum()
        total_bronze = df_bronze.get('price_nightly', pd.Series(dtype=float)).sum()

        if total_silver == 0:
            price_ok = (total_bronze == 0)
        else:
            diff = (total_bronze - total_silver) / total_silver
            price_ok = abs(diff) <= 0.10

        if not price_ok:
            print("[PRICE] Diferença > 10%")
            return False
        print("[PRICE] OK")

        return True

    @task
    def update_silver_table(check_result: bool):
        if not check_result:
            print("Checagens falharam. Não atualizando.")
            return
        print("Checagens OK. Substituindo tabela...")
        engine = get_engine("ovh_silver")
        df_bronze = connection_df("ovh_bronze", "tb_reservas")
        replace_silver_table(engine, df_bronze, "tb_reservas")
        print("ovh_silver.tb_reservas substituída com sucesso.")

    update_silver_table(check_data_quality())
