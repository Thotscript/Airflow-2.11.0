import pendulum
import pandas as pd
from sqlalchemy import create_engine, text
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

def log_check_consistencia(status_ok: bool,
                           table_name: str = "tb_reservas",
                           dag_origem: str = "OVH-tb_reservas"):
    """
    Garante criação da tabela e grava um log da checagem.
    """
    engine = get_engine("ovh_silver")

    create_table_sql = text("""
        CREATE TABLE IF NOT EXISTS tb_check_consistencia (
            id INT AUTO_INCREMENT PRIMARY KEY,
            table_name VARCHAR(100) NOT NULL,
            dag_origem VARCHAR(100) NOT NULL,
            dt_run DATETIME NOT NULL,
            status TINYINT(1) NOT NULL
        );
    """)

    with engine.begin() as conn:
        conn.execute(create_table_sql)

    dt_run = pendulum.now(SP_TZ).naive()

    df_log = pd.DataFrame([{
        "table_name": table_name,
        "dag_origem": dag_origem,
        "dt_run": dt_run,
        "status": 1 if status_ok else 0
    }])

    df_log.to_sql(
        name="tb_check_consistencia",
        con=engine,
        if_exists="append",
        index=False
    )

with DAG(
    dag_id="OVH-Check-tb_reservas",
    start_date=pendulum.datetime(2025, 9, 23, 8, 0, tz=SP_TZ),
    schedule=None,
    catchup=False,
    tags=["Check - tb_reservas"],
) as dag:

    @task
    def check_data_quality() -> bool:
        df_silver = connection_df("ovh_silver", "tb_reservas")
        df_bronze = connection_df("ovh_bronze", "tb_reservas")

        if set(df_bronze.columns) != set(df_silver.columns):
            print("[COLUMNS] Divergentes")
            return False
        print("[COLUMNS] OK")

        if not comp_linhas(df_bronze, df_silver):
            print("[LINHAS] Divergência > 5%")
            return False
        print("[LINHAS] OK")

        total_silver = df_silver.get('price_nightly', pd.Series(dtype=float)).sum()
        total_bronze = df_bronze.get('price_nightly', pd.Series(dtype=float)).sum()

        if total_silver == 0:
            price_ok = (total_bronze == 0)
        else:
            diff = (total_bronze - total_silver) / total_silver
            price_ok = abs(diff) <= 0.10

        if not price_ok:
            print("[PRICE] Divergência > 10%")
            return False
        print("[PRICE] OK")

        return True

    @task
    def update_silver_table(check_result: bool) -> bool:
        if not check_result:
            print("Não atualizando tabela silver.")
            return False

        print("Atualizando tabela silver...")
        engine = get_engine("ovh_silver")
        df_bronze = connection_df("ovh_bronze", "tb_reservas")
        replace_silver_table(engine, df_bronze, "tb_reservas")
        print("Tabela silver atualizada.")
        return True

    @task
    def registrar_log(status_final: bool):
        log_check_consistencia(status_ok=status_final,
                               table_name="tb_reservas",
                               dag_origem="OVH-tb_reservas")
        print(f"[LOG] Consistência registrada (status={status_final}).")

    status = check_data_quality()
    status_after_update = update_silver_table(status)
    registrar_log(status_after_update)
