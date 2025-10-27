import time
from datetime import datetime
from urllib.parse import quote_plus

import pandas as pd
from sqlalchemy import create_engine

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
# Consultas (MySQL)
# =========================
SQL_RESERVAS = """
SELECT *
FROM tb_reserva
WHERE status_id IN (4,5,6,7);
"""

SQL_PRECOS = """
SELECT *
FROM tb_reservas_price_day;
"""


def main():
    # ---- Engine MySQL ----
    engine = create_engine(
        f"mysql+pymysql://{DB_USER}:{quote_plus(DB_PASS)}@{DB_HOST}:{DB_PORT}/{DB_NAME}?charset=utf8mb4"
    )

    # ---- Leitura das tabelas ----
    reservas = pd.read_sql(SQL_RESERVAS, con=engine)
    precos   = pd.read_sql(SQL_PRECOS,   con=engine)

    if reservas.empty:
        print("Aviso: tb_reserva não retornou linhas com status_id em (4,5,6,7). Encerrando.")
        engine.dispose()
        return

    # ---- Merge (LEFT) reservas x preços ----
    # left_on: reservas.id ; right_on: reservas_price_day.reservation_id
    resultado = pd.merge(
        reservas,
        precos,
        left_on="id",
        right_on="reservation_id",
        how="left",
        suffixes=("", "_price")
    )

    # ---- Substituições de 'casas espelhos' em unit_id ----
    casas_espelhos = {
        567515: 451446, 747169: 345511, 588400: 747861, 588406: 747861, 441403: 747861,
        614452: 341242, 747735: 341242, 747171: 379860, 379240: 747177, 747215: 334111,
        747220: 380233, 614526: 488453, 747747: 488453, 614527: 488453, 575254: 511116,
        747224: 445230, 747230: 334112, 563466: 590935, 747235: 443999, 614955: 495437,
        747754: 495437, 747249: 598346, 747092: 379248, 747254: 425652, 746111: 514404,
        735680: 498139
    }

    # garante que unit_id é inteiro (se vier float/str)
    if "unit_id" in resultado.columns:
        # Tenta converter preservando nulos
        resultado["unit_id"] = pd.to_numeric(resultado["unit_id"], errors="coerce").astype("Int64")
        # replace funciona com Int64; onde NaN, não altera
        resultado["unit_id"] = resultado["unit_id"].replace(casas_espelhos)

    # ---- Atualização de datas de criação por ID ----
    novas_datas = {
        33956627:'10/21/2023 00:00:00', 33967753:'07/11/2023 00:00:00',
        33967744:'06/28/2023 00:00:00', 33967723:'05/16/2023 00:00:00',
        33967713:'05/05/2023 00:00:00', 33967706:'04/27/2023 00:00:00',
        33967510:'08/15/2023 00:00:00', 33967500:'08/09/2023 00:00:00',
        33967475:'07/24/2023 00:00:00', 33967419:'07/10/2023 00:00:00',
        33967394:'06/28/2023 00:00:00', 33967360:'06/23/2023 00:00:00',
        33967333:'06/16/2023 00:00:00', 33967306:'06/09/2023 00:00:00',
        33967263:'06/01/2023 00:00:00', 33967237:'05/25/2023 00:00:00',
        33967196:'05/19/2023 00:00:00', 33967169:'05/12/2023 00:00:00',
        33967099:'05/03/2023 00:00:00', 33967056:'04/30/2023 00:00:00',
        33966324:'09/30/2023 00:00:00', 33966266:'06/16/2023 00:00:00',
        33966241:'05/21/2023 00:00:00', 33966206:'05/06/2023 00:00:00',
        33966183:'04/27/2023 00:00:00', 33966128:'03/10/2023 00:00:00',
        33965469:'07/09/2023 00:00:00', 33965399:'05/24/2023 00:00:00',
        33965322:'03/10/2023 00:00:00', 33964156:'06/27/2023 00:00:00',
        33964092:'05/20/2023 00:00:00', 33964053:'05/09/2023 00:00:00',
        33964037:'04/22/2023 00:00:00', 33954119:'08/14/2023 00:00:00',
        33954087:'07/22/2023 00:00:00', 33954062:'07/13/2023 00:00:00',
        33954044:'07/06/2023 00:00:00', 33954019:'06/28/2023 00:00:00',
        33954005:'06/21/2023 00:00:00', 33953973:'06/16/2023 00:00:00',
        33953949:'06/09/2023 00:00:00', 33953921:'05/07/2023 00:00:00',
        33953894:'04/27/2023 00:00:00', 33953880:'04/21/2023 00:00:00',
        33952017:'07/22/2023 00:00:00', 33951998:'07/14/2023 00:00:00',
        33951977:'06/20/2023 00:00:00', 33951957:'05/05/2023 00:00:00',
        33951931:'04/24/2023 00:00:00', 33951070:'08/10/2023 00:00:00',
        33951028:'07/20/2023 00:00:00', 33951012:'07/13/2023 00:00:00',
        33950981:'07/08/2023 00:00:00', 33950870:'06/28/2023 00:00:00',
        33950789:'06/14/2023 00:00:00', 33950758:'06/03/2023 00:00:00',
        33950709:'05/07/2023 00:00:00', 33950688:'04/28/2023 00:00:00',
        33950638:'04/21/2023 00:00:00', 33966030:'05/21/2023 00:00:00',
        33965965:'04/16/2023 00:00:00', 33953002:'06/09/2023 00:00:00',
        33952959:'03/10/2023 00:00:00', 33952549:'04/21/2023 00:00:00',
        33966168:'03/30/2023 00:00:00', 33966140:'03/22/2023 00:00:00',
        33965373:'04/14/2023 00:00:00', 33953869:'04/15/2023 00:00:00',
        33953850:'03/30/2023 00:00:00', 33953826:'03/24/2023 00:00:00',
        33953812:'03/17/2023 00:00:00', 33952977:'03/31/2023 00:00:00',
        33951917:'04/09/2023 00:00:00', 33951900:'03/30/2023 00:00:00',
        33951877:'03/17/2023 00:00:00', 33950938:'07/04/2023 00:00:00',
        33950605:'04/04/2023 00:00:00'
    }

    # Atualiza a coluna creation_date conforme IDs
    if "creation_date" in resultado.columns:
        for _id, nova_data in novas_datas.items():
            mask = resultado["id"] == _id
            if mask.any():
                resultado.loc[mask, "creation_date"] = nova_data

    # ---- Gravação no MySQL ----
    table_name = "tb_reservas_por_data"
    resultado.to_sql(
        table_name,
        con=engine,
        if_exists="replace",
        index=False,
        chunksize=100_000,
        method="multi"
    )

    engine.dispose()
    print(f"Dados Gravados Com Sucesso em {table_name}. Linhas: {len(resultado)}")


# =================
# Airflow DAG
# =================
SP_TZ = pendulum.timezone("America/Sao_Paulo")

with DAG(
    dag_id="OVH-tb_reservas_por_data",
    start_date=pendulum.datetime(2025, 9, 23, 8, 0, tz=SP_TZ),
    schedule="30 5 * * *",  # diariamente 05:30
    catchup=False,
    tags=["Tabelas - OVH"],
) as dag:

    @task()
    def build_tb_reservas_por_data():
        main()

    build_tb_reservas_por_data()
