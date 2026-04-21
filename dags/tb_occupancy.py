import pandas as pd
import mysql.connector
import pendulum

from datetime import datetime, timedelta, date
from airflow import DAG
from airflow.decorators import task

DB_CFG = dict(
    host="host.docker.internal",
    user="root",
    password="Tfl1234@",
    database="ovh_bronze",  # mantido igual ao seu exemplo
)

SOURCE_SCHEMA = "ovh_silver"
DEST_SCHEMA = "ovh_silver"

TB_RESERVAS = "tb_reservas"
TB_ACTIVE_HOUSES = "tb_active_houses"
TB_NAME = "tb_occupancy_reservation_day"

TARGET_YEAR = 2026


def create_occupancy_table():
    conn = mysql.connector.connect(**DB_CFG)
    try:
        cur = conn.cursor()
        create_sql = f"""
        CREATE TABLE IF NOT EXISTS `{DEST_SCHEMA}`.`{TB_NAME}` (
            `unit_id` BIGINT NOT NULL,
            `confirmation_id` VARCHAR(50) NOT NULL,
            `occupied_date` DATE NOT NULL,
            `days` TINYINT NOT NULL DEFAULT 1,
            `year` INT NOT NULL,
            `month` INT NOT NULL,
            `day` INT NOT NULL,
            `month_str` VARCHAR(7) NOT NULL,
            `startdate` DATE NOT NULL,
            `enddate` DATE NOT NULL,
            `extraction_date` DATETIME NOT NULL,
            PRIMARY KEY (`unit_id`, `confirmation_id`, `occupied_date`),
            KEY `idx_confirmation_id` (`confirmation_id`),
            KEY `idx_occupied_date` (`occupied_date`),
            KEY `idx_unit_date` (`unit_id`, `occupied_date`),
            KEY `idx_month_str` (`month_str`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """
        cur.execute(create_sql)
        conn.commit()
    finally:
        conn.close()


def fetch_reservations():
    """
    Busca as reservas diretamente da tb_reservas
    e relaciona com tb_active_houses:
      tb_reservas.unit_id = tb_active_houses.id
    """
    year_start = f"{TARGET_YEAR}-01-01"
    year_end = f"{TARGET_YEAR}-12-31"

    query = f"""
        SELECT DISTINCT
            r.unit_id,
            CAST(r.confirmation_id AS CHAR) AS confirmation_id,
            DATE(r.startdate) AS startdate,
            DATE(r.enddate) AS enddate
        FROM `{SOURCE_SCHEMA}`.`{TB_RESERVAS}` r
        INNER JOIN `{SOURCE_SCHEMA}`.`{TB_ACTIVE_HOUSES}` ah
            ON ah.id = r.unit_id
        WHERE ah.renting_type = 'RENTING'
          AND ah.id IS NOT NULL
          AND r.unit_id IS NOT NULL
          AND r.confirmation_id IS NOT NULL
          AND r.startdate IS NOT NULL
          AND r.enddate IS NOT NULL
          AND DATE(r.enddate) >= %s
          AND DATE(r.startdate) <= %s
    """

    conn = mysql.connector.connect(**DB_CFG)
    try:
        df = pd.read_sql(query, conn, params=(year_start, year_end))
        return df
    finally:
        conn.close()


def build_daily_occupancy_rows(df_reservas: pd.DataFrame) -> pd.DataFrame:
    """
    Gera 1 linha por dia reservado.

    Exemplo:
      confirmation_id | days | occupied_date | unit_id
             233      |  1   | 2026-04-10    | 24
             233      |  1   | 2026-04-11    | 24
             233      |  1   | 2026-04-12    | 24
    """
    if df_reservas.empty:
        return pd.DataFrame(columns=[
            "unit_id",
            "confirmation_id",
            "occupied_date",
            "days",
            "year",
            "month",
            "day",
            "month_str",
            "startdate",
            "enddate",
            "extraction_date",
        ])

    year_start = date(TARGET_YEAR, 1, 1)
    year_end = date(TARGET_YEAR, 12, 31)
    extraction_date = datetime.now()

    rows = []

    for row in df_reservas.itertuples(index=False):
        unit_id = int(row.unit_id)
        confirmation_id = str(row.confirmation_id)

        startdate = pd.to_datetime(row.startdate).date()
        enddate = pd.to_datetime(row.enddate).date()

        # recorta a reserva para o ano alvo
        effective_start = max(startdate, year_start)
        effective_end = min(enddate, year_end)

        if effective_end < effective_start:
            continue

        for occupied_date in pd.date_range(start=effective_start, end=effective_end, freq="D"):
            occupied_date = occupied_date.date()

            rows.append({
                "unit_id": unit_id,
                "confirmation_id": confirmation_id,
                "occupied_date": occupied_date,
                "days": 1,
                "year": occupied_date.year,
                "month": occupied_date.month,
                "day": occupied_date.day,
                "month_str": occupied_date.strftime("%Y-%m"),
                "startdate": startdate,
                "enddate": enddate,
                "extraction_date": extraction_date,
            })

    df_days = pd.DataFrame(rows)

    if not df_days.empty:
        df_days = df_days.drop_duplicates(
            subset=["unit_id", "confirmation_id", "occupied_date"],
            keep="last"
        )

    return df_days


def clear_target_year():
    conn = mysql.connector.connect(**DB_CFG)
    try:
        cur = conn.cursor()
        delete_sql = f"DELETE FROM `{DEST_SCHEMA}`.`{TB_NAME}` WHERE `year` = %s"
        cur.execute(delete_sql, (TARGET_YEAR,))
        conn.commit()
        print(f"[INFO] Dados antigos de {TARGET_YEAR} removidos de {DEST_SCHEMA}.{TB_NAME}")
    finally:
        conn.close()


def save_occupancy_data(df: pd.DataFrame):
    if df.empty:
        print("[INFO] Nenhum dado para salvar.")
        return

    conn = mysql.connector.connect(**DB_CFG)
    try:
        cur = conn.cursor()

        insert_sql = f"""
        INSERT INTO `{DEST_SCHEMA}`.`{TB_NAME}`
        (
            unit_id,
            confirmation_id,
            occupied_date,
            days,
            year,
            month,
            day,
            month_str,
            startdate,
            enddate,
            extraction_date
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        data = [tuple(r) for r in df.itertuples(index=False, name=None)]
        cur.executemany(insert_sql, data)
        conn.commit()

        print(f"[INFO] {len(data)} linhas inseridas em {DEST_SCHEMA}.{TB_NAME}")
    finally:
        conn.close()


def main():
    print("[INFO] Iniciando geração da tabela diária de ocupação por reserva...")
    create_occupancy_table()

    df_reservas = fetch_reservations()
    print(f"[INFO] Reservas encontradas: {len(df_reservas)}")

    if df_reservas.empty:
        print("[INFO] Nenhuma reserva encontrada para o período.")
        return

    df_daily = build_daily_occupancy_rows(df_reservas)
    print(f"[INFO] Linhas diárias geradas: {len(df_daily)}")

    clear_target_year()
    save_occupancy_data(df_daily)

    print("[INFO] Processo concluído com sucesso.")


SP_TZ = pendulum.timezone("America/Sao_Paulo")

with DAG(
    dag_id="OVH-tb_occupancy_reservation_day",
    start_date=pendulum.datetime(2025, 12, 19, 8, 0, tz=SP_TZ),
    schedule="0 2 * * *",
    catchup=False,
    tags=["Tabelas - OVH", "Ocupacao", "Reservas", "Diaria"],
) as dag:

    @task(retries=4, retry_exponential_backoff=True)
    def calculate_occupancy_reservation_day():
        main()

    calculate_occupancy_reservation_day()