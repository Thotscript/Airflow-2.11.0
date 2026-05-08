import time
import pandas as pd
import requests
import mysql.connector
import pendulum

from collections import deque
from datetime import datetime, timedelta, date
from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable

# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURAÇÕES
# ─────────────────────────────────────────────────────────────────────────────
API_URL      = "https://web.streamlinevrs.com/api/json"
TOKEN_KEY    = Variable.get("STREAMLINE_TOKEN_KEY")
TOKEN_SECRET = Variable.get("STREAMLINE_TOKEN_SECRET")

DB_CFG = dict(
    host="host.docker.internal",
    user="root",
    password="Tfl1234@",
    database="ovh_bronze",
)

SOURCE_SCHEMA = "ovh_silver"
DEST_SCHEMA   = "ovh_silver"

TB_RESERVAS                  = "tb_reservas"
TB_ACTIVE_HOUSES             = "tb_active_houses"
TB_OCCUPANCY_HOUSES          = "tb_occupancy_houses"
TB_OCCUPANCY_RESERVATION_DAY = "tb_occupancy_reservation_day"

# Anos processados na ocupação por API (calendário de bloqueios)
TARGET_YEARS_HOUSES = [2025, 2026]
# Ano processado na ocupação por reserva (diária)
TARGET_YEAR_RESERVATION = 2026

# ─────────────────────────────────────────────────────────────────────────────
# THROTTLE / RETRY (API Streamline)
# ─────────────────────────────────────────────────────────────────────────────
WINDOW_SEC = 60
MAX_CALLS  = 90
_call_times = deque()


def throttle():
    now = time.time()
    _call_times.append(now)
    while _call_times and (now - _call_times[0]) > WINDOW_SEC:
        _call_times.popleft()
    if len(_call_times) >= MAX_CALLS:
        sleep_for = WINDOW_SEC - (now - _call_times[0]) + 0.1
        if sleep_for > 0:
            time.sleep(sleep_for)


def post_with_retry(session, payload, retries=6, base_delay=1, timeout=60):
    delay = base_delay
    headers = {"Content-Type": "application/json"}
    resp = None
    for attempt in range(retries):
        throttle()
        try:
            resp = session.post(API_URL, json=payload, headers=headers, timeout=timeout)
            if resp.status_code == 200:
                data = resp.json()
                if isinstance(data, dict) and data.get("data") is None and data.get("Response") is None:
                    print(f"[RETRY] Resposta nula (possível rate limit). Tentativa {attempt+1}/{retries}")
                    time.sleep(delay)
                    delay *= 2
                    continue
                return resp
            if resp.status_code in (429, 500, 502, 503, 504):
                retry_after = resp.headers.get("Retry-After")
                if retry_after:
                    try:
                        time.sleep(float(retry_after))
                    except Exception:
                        time.sleep(delay)
                else:
                    time.sleep(delay)
                delay = min(delay * 2, 30)
                continue
            return resp
        except Exception as e:
            print(f"[RETRY] Exceção na chamada: {e}. Tentativa {attempt+1}")
            time.sleep(delay)
            delay *= 2
    return resp


# ─────────────────────────────────────────────────────────────────────────────
# HELPERS COMPARTILHADOS
# ─────────────────────────────────────────────────────────────────────────────
def get_active_houses():
    conn = mysql.connector.connect(**DB_CFG)
    try:
        query = f"SELECT id FROM `{SOURCE_SCHEMA}`.`{TB_ACTIVE_HOUSES}` WHERE renting_type = 'RENTING'"
        df = pd.read_sql(query, conn)
        return df['id'].tolist()
    finally:
        conn.close()


def append_snapshot(table_full_name: str, insert_sql: str, df: pd.DataFrame):
    """
    Estratégia de APPEND DIÁRIO:
      1. Mantém intactos todos os snapshots de dias anteriores (HISTÓRICO).
      2. Apaga APENAS as linhas cuja DATE(extraction_date) = hoje, se houver.
         Isso permite reexecutar a DAG no mesmo dia e atualizar o snapshot
         do dia corrente sem perder histórico.
      3. Insere as novas linhas com extraction_date = agora.
    """
    if df.empty:
        print(f"[INFO] {table_full_name}: nenhum dado para salvar.")
        return

    extraction_day = df["extraction_date"].iloc[0].date()

    conn = mysql.connector.connect(**DB_CFG)
    try:
        cur = conn.cursor()

        # 1) Apaga somente o snapshot do dia atual (se existir)
        cur.execute(
            f"DELETE FROM {table_full_name} WHERE DATE(extraction_date) = %s",
            (extraction_day,),
        )
        deleted = cur.rowcount
        if deleted > 0:
            print(f"[INFO] {table_full_name}: {deleted} linhas do dia {extraction_day} removidas para reprocesso.")

        # 2) Insere o snapshot atualizado do dia
        data = [tuple(r) for r in df.itertuples(index=False, name=None)]
        cur.executemany(insert_sql, data)
        conn.commit()
        print(f"[INFO] {table_full_name}: {cur.rowcount} linhas inseridas (snapshot {extraction_day}).")
    finally:
        conn.close()


# ═════════════════════════════════════════════════════════════════════════════
# 1) OCUPAÇÃO MENSAL POR CASA (via API Streamline) — tb_occupancy_houses
# ═════════════════════════════════════════════════════════════════════════════
def make_payload_calendar(unit_id: int, startdate: str, enddate: str):
    return {
        "methodName": "GetPropertyAvailabilityCalendarRawData",
        "params": {
            "token_key":    TOKEN_KEY,
            "token_secret": TOKEN_SECRET,
            "unit_id":      unit_id,
            "startdate":    startdate,
            "enddate":      enddate,
        },
    }


def extract_blocked_period(data):
    if isinstance(data, list):
        return data
    if not isinstance(data, dict):
        return []
    if "Response" in data:
        response_data = data.get("Response")
        if isinstance(response_data, dict):
            inner_data = response_data.get("data")
            if isinstance(inner_data, dict):
                blocked = inner_data.get("blocked_period", [])
                if blocked:
                    return blocked
            elif isinstance(inner_data, list):
                return inner_data
    data_field = data.get("data")
    if isinstance(data_field, list):
        return data_field
    if isinstance(data_field, dict):
        blocked = data_field.get("blocked_period", [])
        if blocked:
            return blocked
    if "status" in data and not data_field:
        return []
    return []


def fetch_calendar(session: requests.Session, unit_id: int, startdate: str, enddate: str) -> list:
    payload = make_payload_calendar(unit_id, startdate, enddate)
    try:
        resp = post_with_retry(session, payload)
        resp.raise_for_status()
        data = resp.json()
        if isinstance(data, dict) and data.get("error"):
            print(f"[WARN] API erro para unit_id={unit_id}: {data.get('error')}")
            return []
        blocked = extract_blocked_period(data)
        return blocked if blocked else []
    except Exception as e:
        print(f"[ERROR] Erro inesperado para unit_id={unit_id}: {e}")
        return []


def calculate_occupancy_extended(blocked: list, startdate: str, today: date) -> dict:
    mes_obj   = datetime.strptime(startdate, "%m/%d/%Y")
    first_day = mes_obj.date()
    if mes_obj.month == 12:
        last_day = date(mes_obj.year + 1, 1, 1) - timedelta(days=1)
    else:
        last_day = date(mes_obj.year, mes_obj.month + 1, 1) - timedelta(days=1)

    dias_no_mes = last_day.day

    is_complete_month = 1 if last_day < today else 0
    is_future_month   = 1 if first_day > today else 0

    if is_future_month:
        days_elapsed = 0
    elif is_complete_month:
        days_elapsed = dias_no_mes
    else:
        days_elapsed = (today - first_day).days + 1

    if not blocked:
        return {
            "occupancy_rate":       0.0,
            "days_occupied":        0,
            "days_in_month":        dias_no_mes,
            "days_elapsed":         days_elapsed,
            "days_occupied_past":   0,
            "days_occupied_future": 0,
            "is_complete_month":    is_complete_month,
            "is_future_month":      is_future_month,
        }

    dias_ocupados_set        = set()
    dias_ocupados_past_set   = set()
    dias_ocupados_future_set = set()

    for b in blocked:
        try:
            start = datetime.strptime(b["startdate"], "%m/%d/%Y").date()
            end   = datetime.strptime(b["enddate"],   "%m/%d/%Y").date()

            if end < first_day or start > last_day:
                continue

            overlap_start = max(start, first_day)
            overlap_end   = min(end,   last_day)

            current = overlap_start
            while current <= overlap_end:
                dias_ocupados_set.add(current)
                if current <= today:
                    dias_ocupados_past_set.add(current)
                else:
                    dias_ocupados_future_set.add(current)
                current += timedelta(days=1)
        except Exception:
            continue

    dias_ocupados        = min(len(dias_ocupados_set),        dias_no_mes)
    days_occupied_past   = min(len(dias_ocupados_past_set),   dias_no_mes)
    days_occupied_future = min(len(dias_ocupados_future_set), dias_no_mes)
    ocupacao             = dias_ocupados / dias_no_mes

    return {
        "occupancy_rate":       round(ocupacao, 4),
        "days_occupied":        dias_ocupados,
        "days_in_month":        dias_no_mes,
        "days_elapsed":         days_elapsed,
        "days_occupied_past":   days_occupied_past,
        "days_occupied_future": days_occupied_future,
        "is_complete_month":    is_complete_month,
        "is_future_month":      is_future_month,
    }


def create_occupancy_houses_table():
    conn = mysql.connector.connect(**DB_CFG)
    try:
        cur = conn.cursor()
        cur.execute(f"""
        CREATE TABLE IF NOT EXISTS `{DEST_SCHEMA}`.`{TB_OCCUPANCY_HOUSES}` (
            `unit_id`              BIGINT       NOT NULL,
            `year`                 INT          NOT NULL,
            `month`                INT          NOT NULL,
            `month_str`            VARCHAR(7)   NOT NULL,
            `occupancy_rate`       DECIMAL(5,4) NULL,
            `days_occupied`        INT          NULL,
            `days_in_month`        INT          NULL,
            `days_elapsed`         INT          NULL COMMENT 'Dias do mes ja passados ate hoje (inclusive). Use para denominador do YTD.',
            `days_occupied_past`   INT          NULL COMMENT 'Dias ocupados que ja ocorreram (<= hoje). Numerador do YTD.',
            `days_occupied_future` INT          NULL COMMENT 'Dias ocupados ainda no futuro (> hoje).',
            `is_complete_month`    TINYINT(1)   NULL COMMENT '1 = mes inteiro ja passou.',
            `is_future_month`      TINYINT(1)   NULL COMMENT '1 = mes ainda nao comecou.',
            `extraction_date`      DATETIME     NOT NULL,
            PRIMARY KEY (`unit_id`, `year`, `month`, `extraction_date`),
            KEY `idx_extraction_date` (`extraction_date`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """)
        conn.commit()
    finally:
        conn.close()


def build_months_for_years(years):
    months = []
    for year in years:
        for month in range(1, 13):
            startdate = f"{month:02d}/01/{year}"
            if month == 12:
                enddate = f"01/10/{year + 1}"
            else:
                last_day_num = (datetime(year, month + 1, 1) - timedelta(days=1)).day
                extended_end = datetime(year, month, last_day_num) + timedelta(days=10)
                enddate = extended_end.strftime("%m/%d/%Y")
            months.append({
                "startdate": startdate,
                "enddate":   enddate,
                "year":      year,
                "month":     month,
                "month_str": f"{year}-{month:02d}",
            })
    return months


def main_occupancy_houses():
    print("[INFO] Iniciando ocupação mensal por casa (API Streamline)...")
    create_occupancy_houses_table()

    unit_ids = get_active_houses()
    if not unit_ids:
        print("[INFO] Nenhuma casa ativa encontrada.")
        return

    today           = date.today()
    extraction_date = datetime.now()
    months          = build_months_for_years(TARGET_YEARS_HOUSES)

    session = requests.Session()
    results = []

    for unit_id in unit_ids:
        print(f"\n=== Processando unit_id: {unit_id} ===")
        for month_info in months:
            blocked = fetch_calendar(session, unit_id, month_info["startdate"], month_info["enddate"])
            occ     = calculate_occupancy_extended(blocked, month_info["startdate"], today)

            results.append({
                "unit_id":              unit_id,
                "year":                 month_info["year"],
                "month":                month_info["month"],
                "month_str":            month_info["month_str"],
                "occupancy_rate":       occ["occupancy_rate"],
                "days_occupied":        occ["days_occupied"],
                "days_in_month":        occ["days_in_month"],
                "days_elapsed":         occ["days_elapsed"],
                "days_occupied_past":   occ["days_occupied_past"],
                "days_occupied_future": occ["days_occupied_future"],
                "is_complete_month":    occ["is_complete_month"],
                "is_future_month":      occ["is_future_month"],
                "extraction_date":      extraction_date,
            })
            print(
                f"  Casa {unit_id} | {month_info['month_str']} | "
                f"Ocupação: {occ['occupancy_rate']:.2%} ({occ['days_occupied']}/{occ['days_in_month']} dias) | "
                f"Passado: {occ['days_occupied_past']} | Futuro: {occ['days_occupied_future']} | "
                f"Elapsed: {occ['days_elapsed']}"
            )

    df = pd.DataFrame(results)

    insert_sql = f"""
    INSERT INTO `{DEST_SCHEMA}`.`{TB_OCCUPANCY_HOUSES}`
        (unit_id, year, month, month_str,
         occupancy_rate, days_occupied, days_in_month,
         days_elapsed, days_occupied_past, days_occupied_future,
         is_complete_month, is_future_month,
         extraction_date)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    append_snapshot(f"`{DEST_SCHEMA}`.`{TB_OCCUPANCY_HOUSES}`", insert_sql, df)
    print(f"\n[INFO] Concluído ocupação por casa. {len(results)} registros processados.")


# ═════════════════════════════════════════════════════════════════════════════
# 2) OCUPAÇÃO DIÁRIA POR RESERVA — tb_occupancy_reservation_day
# ═════════════════════════════════════════════════════════════════════════════
def create_occupancy_reservation_table():
    conn = mysql.connector.connect(**DB_CFG)
    try:
        cur = conn.cursor()
        cur.execute(f"""
        CREATE TABLE IF NOT EXISTS `{DEST_SCHEMA}`.`{TB_OCCUPANCY_RESERVATION_DAY}` (
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
            `rate`            DECIMAL(12,2) NULL     COMMENT 'price_nightly / days_number',
            `extraction_date` DATETIME      NOT NULL,
            KEY `idx_confirmation_id` (`confirmation_id`),
            KEY `idx_occupied_date`   (`occupied_date`),
            KEY `idx_unit_date`       (`unit_id`, `occupied_date`),
            KEY `idx_month_str`       (`month_str`),
            KEY `idx_extraction_date` (`extraction_date`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """)

        # Migração: garante coluna `rate`
        cur.execute(f"SHOW COLUMNS FROM `{DEST_SCHEMA}`.`{TB_OCCUPANCY_RESERVATION_DAY}`")
        existing_cols = {row[0] for row in cur.fetchall()}
        if "rate" not in existing_cols:
            cur.execute(f"""
                ALTER TABLE `{DEST_SCHEMA}`.`{TB_OCCUPANCY_RESERVATION_DAY}`
                ADD COLUMN `rate` DECIMAL(12,2) NULL
                COMMENT 'price_nightly / days_number'
                AFTER `enddate`
            """)
            print("[MIGRATE] Coluna `rate` adicionada.")

        conn.commit()
    finally:
        conn.close()


def fetch_reservations():
    year_start = f"{TARGET_YEAR_RESERVATION}-01-01"
    year_end   = f"{TARGET_YEAR_RESERVATION}-12-31"

    query = f"""
        SELECT DISTINCT
            r.unit_id,
            CAST(r.confirmation_id AS CHAR)                  AS confirmation_id,
            DATE(r.startdate)                                AS startdate,
            DATE(r.enddate)                                  AS enddate,
            CASE
                WHEN COALESCE(r.days_number, 0) > 0
                THEN ROUND(r.price_nightly / r.days_number, 2)
                ELSE NULL
            END                                              AS rate
        FROM `{SOURCE_SCHEMA}`.`{TB_RESERVAS}` r
        INNER JOIN `{SOURCE_SCHEMA}`.`{TB_ACTIVE_HOUSES}` ah
            ON ah.id = r.unit_id
        WHERE ah.renting_type   = 'RENTING'
          AND ah.id             IS NOT NULL
          AND r.unit_id         IS NOT NULL
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
        df = pd.read_sql(query, conn, params=(year_start, year_end))
        return df
    finally:
        conn.close()


def build_daily_occupancy_rows(df_reservas: pd.DataFrame) -> pd.DataFrame:
    if df_reservas.empty:
        return pd.DataFrame(columns=[
            "unit_id", "confirmation_id", "occupied_date", "days",
            "year", "month", "day", "month_str",
            "startdate", "enddate", "rate", "extraction_date",
        ])

    extraction_date = datetime.now()
    rows = []

    for row in df_reservas.itertuples(index=False):
        unit_id         = int(row.unit_id)
        confirmation_id = str(row.confirmation_id)
        startdate       = pd.to_datetime(row.startdate).date()
        enddate         = pd.to_datetime(row.enddate).date()
        rate            = float(row.rate) if row.rate is not None else None

        if enddate < startdate:
            continue

        for occupied_date in pd.date_range(start=startdate, end=enddate, freq="D"):
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

    return pd.DataFrame(rows)


def main_occupancy_reservation_day():
    print("[INFO] Iniciando ocupação diária por reserva...")
    create_occupancy_reservation_table()

    df_reservas = fetch_reservations()
    print(f"[INFO] Reservas encontradas: {len(df_reservas)}")
    if df_reservas.empty:
        print("[INFO] Nenhuma reserva encontrada para o período.")
        return

    df_daily = build_daily_occupancy_rows(df_reservas)
    print(f"[INFO] Linhas diárias geradas: {len(df_daily)}")

    insert_sql = f"""
    INSERT INTO `{DEST_SCHEMA}`.`{TB_OCCUPANCY_RESERVATION_DAY}`
    (
        unit_id, confirmation_id, occupied_date, days,
        year, month, day, month_str,
        startdate, enddate, rate, extraction_date
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    append_snapshot(f"`{DEST_SCHEMA}`.`{TB_OCCUPANCY_RESERVATION_DAY}`", insert_sql, df_daily)
    print("[INFO] Concluído ocupação diária por reserva.")


# ═════════════════════════════════════════════════════════════════════════════
# DAG UNIFICADA
# ═════════════════════════════════════════════════════════════════════════════
SP_TZ = pendulum.timezone("America/Sao_Paulo")

with DAG(
    dag_id="OVH-tb_occupancy_daily_append",
    start_date=pendulum.datetime(2025, 12, 19, 8, 0, tz=SP_TZ),
    schedule="0 2 * * *",
    catchup=False,
    tags=["Tabelas - OVH", "Ocupacao", "Diaria", "Append"],
) as dag:

    @task(task_id="occupancy_houses_api", retries=4, retry_exponential_backoff=True)
    def task_occupancy_houses():
        main_occupancy_houses()

    @task(task_id="occupancy_reservation_day", retries=1, retry_exponential_backoff=True)
    def task_occupancy_reservation_day():
        main_occupancy_reservation_day()

    # Rodam em paralelo - são independentes (banco diferente de campos)
    task_occupancy_houses()
    task_occupancy_reservation_day()