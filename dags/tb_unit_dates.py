import time
from datetime import datetime, timedelta

import pandas as pd
from sqlalchemy import create_engine
from urllib.parse import quote_plus

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
# Query Ajustada (MySQL)
# =========================
SQL_UNITS = """
SELECT DISTINCT id, Administradora
FROM (
    SELECT
        CAST(id AS CHAR)                                   AS id,
        CAST(Administradora AS CHAR) COLLATE utf8mb4_unicode_ci AS Administradora
    FROM tb_property_list_wordpress
    WHERE renting_type = 'RENTING'

    UNION
    SELECT
        CAST(unit_id AS CHAR)                              AS id,
        CAST(Administradora AS CHAR) COLLATE utf8mb4_unicode_ci AS Administradora
    FROM tb_reservas
) AS CombinedData;
"""


def main():
    # 1) Datas (2018-01-01 até 31/12 do ano atual + 3 anos)
    start_date = datetime(2018, 1, 1)
    end_date = datetime.now().replace(month=12, day=31) + timedelta(days=365 * 3)

    # 2) Engine MySQL (com DB definido na URL)
    engine = create_engine(
        f"mysql+pymysql://{DB_USER}:{quote_plus(DB_PASS)}@{DB_HOST}:{DB_PORT}/{DB_NAME}?charset=utf8mb4"
    )

    # 3) Lê unidades (id, Administradora)
    df_units = pd.read_sql(SQL_UNITS, con=engine)

    if df_units.empty:
        print("Nenhuma unidade retornada pela consulta. Encerrando.")
        engine.dispose()
        return

    # 4) Gera intervalo diário e formata em MM/DD/YYYY
    date_range = pd.date_range(start=start_date, end=end_date, freq="D")
    date_df = pd.DataFrame({"Date": date_range.strftime("%m/%d/%Y")})

    # 5) Cross join (todas as unidades x todas as datas)
    #    Requer pandas >= 1.2 para how='cross'
    df_final = df_units.merge(date_df, how="cross")

    # 6) Grava em tb_unit_dates (replace para substituir a tabela inteira)
    #    Usa chunksize e method='multi' para performance
    table_name = "tb_unit_dates"
    df_final.to_sql(
        table_name,
        con=engine,
        if_exists="replace",
        index=False,
        chunksize=100_000,
        method="multi",
    )

    engine.dispose()
    print(
        f"Dados gravados com sucesso em {table_name}. "
        f"Unidades: {len(df_units)}  Datas: {len(date_df)}  Linhas: {len(df_final)}"
    )


if __name__ == "__main__":
    main()


# ================
# Airflow DAG
# ================
SP_TZ = pendulum.timezone("America/Sao_Paulo")

with DAG(
    dag_id="OVH-tb_unit_dates",
    start_date=pendulum.datetime(2025, 9, 23, 8, 0, tz=SP_TZ),
    schedule="0 5 * * *",   # diariamente às 05:00
    catchup=False,
    tags=["Tabelas - OVH"],
) as dag:

    @task()
    def build_tb_unit_dates():
        main()

    build_tb_unit_dates()
