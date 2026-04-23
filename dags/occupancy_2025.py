import pandas as pd
import mysql.connector
import pendulum

from datetime import datetime, date
from airflow import DAG
from airflow.decorators import task

DB_CFG = dict(
    host="host.docker.internal",
    user="root",
    password="Tfl1234@",
    database="ovh_bronze",
)

SOURCE_SCHEMA = "ovh_silver"
DEST_SCHEMA   = "ovh_silver"

TB_RESERVAS = "tb_reservas"
TB_NAME     = "tb_occupancy_reservation_day"

TARGET_YEAR = 2025


def create_occupancy_table():
    conn = mysql.connector.connect(**DB_CFG)
    try:
        cur = conn.cursor()

        create_sql = f"""
        CREATE TABLE IF NOT EXISTS `{DEST_SCHEMA}`.`{TB_NAME}` (
            `unit_id`         BIGINT        NOT NULL,
            `confirmation_id` VARCHAR(50)   NOT NULL,
            `occupied_date`   DATE          NOT NULL,
            `days`            TINYINT       NOT NULL DEFAULT 1,
            `year`            INT           NOT NULL,
            `month`           INT           NOT NULL,
            `day`             INT           NOT NULL,
            `month_str`       VARCHAR(7)    NOT NULL,
            `startdate`       DATE          NOT NULL,
            `enddate`         DATE          NOT NULL,
            `rate`            DECIMAL(12,2) NULL COMMENT 'price_nightly / days_number',
            `extraction_date` DATETIME      NOT NULL,
            KEY `idx_confirmation_id` (`confirmation_id`),
            KEY `idx_occupied_date`   (`occupied_date`),
            KEY `idx_unit_date`       (`unit_id`, `occupied_date`),
            KEY `idx_month_str`       (`month_str`),
            KEY `idx_extraction_date` (`extraction_date`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """
        cur.execute(create_sql)

        cur.execute(f"SHOW COLUMNS FROM `{DEST_SCHEMA}`.`{TB_NAME}`")
        existing_cols = {row[0] for row in cur.fetchall()}

        if "rate" not in existing_cols:
            cur.execute(f"""
                ALTER TABLE `{DEST_SCHEMA}`.`{TB_NAME}`
                ADD COLUMN `rate` DECIMAL(12,2) NULL
                COMMENT 'price_nightly / days_number'
                AFTER `enddate`
            """)
            print("[MIGRATE] Coluna `rate` adicionada.")

        conn.commit()
    finally:
        conn.close()


def fetch_reservations(target_year: int):
    year_start = f"{target_year}-01-01"
    year_end   = f"{target_year}-12-31"

    query = f"""
        SELECT DISTINCT
            r.unit_id,
            CAST(r.confirmation_id AS CHAR) AS confirmation_id,
            DATE(r.startdate)               AS startdate,
            DATE(r.enddate)                 AS enddate,
            CASE
                WHEN COALESCE(r.days_number, 0) > 0
                THEN ROUND(r.price_nightly / r.days_number, 2)
                ELSE NULL
            END AS rate
        FROM `{SOURCE_SCHEMA}`.`{TB_RESERVAS}` r
        WHERE r.unit_id         IS NOT NULL
          AND r.confirmation_id IS NOT NULL
          AND r.startdate       IS NOT NULL
          AND r.enddate         IS NOT NULL
          AND DATE(r.enddate)   >= %s
          AND DATE(r.startdate) <= %s
          AND COALESCE(r.price_nightly, 0) > 0
          AND COALESCE(r.days_number,   0) > 0
    """
    conn = mysql.connector.connect(**DB_CFG)
    try:
        return pd.read_sql(query, conn, params=(year_start, year_end))
    finally:
        conn.close()


def build_daily_occupancy_rows(df_reservas: pd.DataFrame, target_year: int) -> pd.DataFrame:
    if df_reservas.empty:
        return pd.DataFrame(columns=[
            "unit_id", "confirmation_id", "occupied_date", "days",
            "year", "month", "day", "month_str",
            "startdate", "enddate", "rate", "extraction_date",
        ])

    extraction_date = datetime.now()
    rows = []

    year_start = date(target_year, 1, 1)
    year_end   = date(target_year, 12, 31)

    for row in df_reservas.itertuples(index=False):
        unit_id         = int(row.unit_id)
        confirmation_id = str(row.confirmation_id)
        startdate       = pd.to_datetime(row.startdate).date()
        enddate         = pd.to_datetime(row.enddate).date()
        rate            = float(row.rate) if row.rate is not None else None

        if enddate < startdate:
            continue

        occ_start = max(startdate, year_start)
        occ_end   = min(enddate, year_end)

        if occ_end < occ_start:
            continue

        for occupied_date in pd.date_range(start=occ_start, end=occ_end, freq="D"):
            occupied_date = occupied_date.date()
            rows.append({
                "unit_id":         unit_id,
                "confirmation_id": confirmation_id,
                "occupied_date":   occupied_date,
                "days":            1,
                "year":            occupied_date.year,
                "month":           occupied_date.month,
                "day":             occupied_date.day,
                "month_str":       occupied_date.strftime("%Y-%m"),
                "startdate":       startdate,
                "enddate":         enddate,
                "rate":            rate,
                "extraction_date": extraction_date,
            })

    df_daily = pd.DataFrame(rows)

    if not df_daily.empty:
        df_daily = df_daily.drop_duplicates(
            subset=["unit_id", "confirmation_id", "occupied_date"]
        )

    return df_daily


def replace_only_target_year(df: pd.DataFrame, target_year: int):
    conn = mysql.connector.connect(**DB_CFG)
    try:
        cur = conn.cursor()

        print(f"[INFO] Removendo dados existentes do ano {target_year}...")
        cur.execute(
            f"DELETE FROM `{DEST_SCHEMA}`.`{TB_NAME}` WHERE `year` = %s",
            (target_year,)
        )

        if df.empty:
            conn.commit()
            print(f"[INFO] Nenhum dado para inserir para o ano {target_year}.")
            return

        insert_sql = f"""
        INSERT INTO `{DEST_SCHEMA}`.`{TB_NAME}`
        (
            unit_id, confirmation_id, occupied_date, days,
            year, month, day, month_str,
            startdate, enddate, rate, extraction_date
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        data = [tuple(r) for r in df.itertuples(index=False, name=None)]
        cur.executemany(insert_sql, data)
        conn.commit()

        print(f"[INFO] {len(data)} linhas inseridas para o ano {target_year} em {DEST_SCHEMA}.{TB_NAME}")
    finally:
        conn.close()


def main():
    print(f"[INFO] Iniciando backfill da OCC para o ano {TARGET_YEAR}...")
    create_occupancy_table()

    df_reservas = fetch_reservations(TARGET_YEAR)
    print(f"[INFO] Reservas encontradas: {len(df_reservas)}")

    if df_reservas.empty:
        print("[INFO] Nenhuma reserva encontrada.")
        replace_only_target_year(pd.DataFrame(), TARGET_YEAR)
        return

    df_daily = build_daily_occupancy_rows(df_reservas, TARGET_YEAR)
    print(f"[INFO] Linhas diárias geradas: {len(df_daily)}")

    replace_only_target_year(df_daily, TARGET_YEAR)
    print("[INFO] Processo concluído com sucesso.")


SP_TZ = pendulum.timezone("America/Sao_Paulo")

with DAG(
    dag_id="OVH-tb_occupancy_reservation_day_backfill_2025",
    start_date=pendulum.datetime(2025, 12, 19, 8, 0, tz=SP_TZ),
    schedule=None,
    catchup=False,
    tags=["Tabelas - OVH", "Ocupacao", "Reservas", "Backfill"],
) as dag:

    @task(retries=1)
    def calculate_occupancy_reservation_day():
        main()

    calculate_occupancy_reservation_day()