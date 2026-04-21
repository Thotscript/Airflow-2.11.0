import os
import json
import re
import time
import hashlib
import numpy as np
import pandas as pd
import requests
from requests.adapters import HTTPAdapter, Retry
from collections import deque
import mysql.connector
import pendulum
from airflow.models import Variable
from datetime import datetime, timedelta, date
from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowException
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

API_URL = "https://web.streamlinevrs.com/api/json"
TOKEN_KEY = Variable.get("STREAMLINE_TOKEN_KEY")
TOKEN_SECRET = Variable.get("STREAMLINE_TOKEN_SECRET")

DB_CFG = dict(
    host="host.docker.internal",
    user="root",
    password="Tfl1234@",
    database="ovh_bronze",
)

# Tabela atual consolidada por casa/mês
TB_NAME = "tb_occupancy_houses"
# Nova tabela detalhada por reserva (com confirmation_id)
TB_DETAIL_NAME = "tb_occupancy_houses_detail"

# ---- THROTTLE (Controle de Vazão) ----
WINDOW_SEC = 60
MAX_CALLS = 90
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



def make_payload_calendar(unit_id: int, startdate: str, enddate: str):
    return {
        "methodName": "GetPropertyAvailabilityCalendarRawData",
        "params": {
            "token_key": TOKEN_KEY,
            "token_secret": TOKEN_SECRET,
            "unit_id": unit_id,
            "startdate": startdate,
            "enddate": enddate,
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

        if isinstance(data, list):
            print(f"[DEBUG] unit_id={unit_id}: API retornou lista com {len(data)} itens")
            return data

        if isinstance(data, dict):
            data_field = data.get("data")
            print(f"[DEBUG] unit_id={unit_id}: dict com 'data' = {type(data_field)}")
            if isinstance(data_field, list):
                print(f"[DEBUG] unit_id={unit_id}: data é lista com {len(data_field)} reservas")
            elif isinstance(data_field, dict):
                bp = data_field.get("blocked_period", [])
                print(f"[DEBUG] unit_id={unit_id}: data.blocked_period = {len(bp) if bp else 0} reservas")

        if isinstance(data, dict) and data.get("error"):
            print(f"[WARN] API erro para unit_id={unit_id}: {data.get('error')}")
            return []

        blocked = extract_blocked_period(data)
        print(f"[DEBUG] unit_id={unit_id}: extract_blocked_period retornou {len(blocked)} reservas")
        return blocked if blocked else []
    except Exception as e:
        print(f"[ERROR] Erro inesperado para unit_id={unit_id}: {e}")
        return []



def extract_confirmation_id(reason: str):
    """
    Extrai o confirmation_id do campo reason.
    Ex.: 'Reservation #23622' -> '23622'
    """
    if not reason:
        return None

    text = str(reason).strip()
    patterns = [
        r"#\s*(\d+)\b",
        r"\breservation\s*#?\s*(\d+)\b",
        r"\bconfirmation\s*#?\s*(\d+)\b",
    ]

    for pattern in patterns:
        match = re.search(pattern, text, flags=re.IGNORECASE)
        if match:
            return match.group(1)
    return None



def build_reservation_key(unit_id, month_str, reservation_startdate, reservation_enddate, reason, confirmation_id):
    raw = f"{unit_id}|{month_str}|{reservation_startdate}|{reservation_enddate}|{reason}|{confirmation_id or ''}"
    return hashlib.md5(raw.encode("utf-8")).hexdigest()



def calculate_occupancy_extended(blocked: list, startdate: str, today: date) -> dict:
    """
    Mantém a visão consolidada mensal por casa.
    """
    mes_obj = datetime.strptime(startdate, "%m/%d/%Y")
    first_day = mes_obj.date()
    if mes_obj.month == 12:
        last_day = date(mes_obj.year + 1, 1, 1) - timedelta(days=1)
    else:
        last_day = date(mes_obj.year, mes_obj.month + 1, 1) - timedelta(days=1)

    dias_no_mes = last_day.day

    is_complete_month = 1 if last_day < today else 0
    is_future_month = 1 if first_day > today else 0

    if is_future_month:
        days_elapsed = 0
    elif is_complete_month:
        days_elapsed = dias_no_mes
    else:
        days_elapsed = (today - first_day).days + 1

    if not blocked:
        return {
            "occupancy_rate": 0.0,
            "days_occupied": 0,
            "days_in_month": dias_no_mes,
            "days_elapsed": days_elapsed,
            "days_occupied_past": 0,
            "days_occupied_future": 0,
            "is_complete_month": is_complete_month,
            "is_future_month": is_future_month,
        }

    dias_ocupados_set = set()
    dias_ocupados_past_set = set()
    dias_ocupados_future_set = set()

    for b in blocked:
        try:
            start = datetime.strptime(b["startdate"], "%m/%d/%Y").date()
            end = datetime.strptime(b["enddate"], "%m/%d/%Y").date()

            if end < first_day or start > last_day:
                continue

            overlap_start = max(start, first_day)
            overlap_end = min(end, last_day)

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

    dias_ocupados = min(len(dias_ocupados_set), dias_no_mes)
    days_occupied_past = min(len(dias_ocupados_past_set), dias_no_mes)
    days_occupied_future = min(len(dias_ocupados_future_set), dias_no_mes)
    ocupacao = dias_ocupados / dias_no_mes

    return {
        "occupancy_rate": round(ocupacao, 4),
        "days_occupied": dias_ocupados,
        "days_in_month": dias_no_mes,
        "days_elapsed": days_elapsed,
        "days_occupied_past": days_occupied_past,
        "days_occupied_future": days_occupied_future,
        "is_complete_month": is_complete_month,
        "is_future_month": is_future_month,
    }



def calculate_occupancy_detail(blocked: list, unit_id: int, year: int, month: int, month_str: str, startdate: str, today: date) -> list:
    """
    Nova visão detalhada por reserva dentro do mês.
    Gera 1 linha por reservation_key / confirmation_id / mês.
    Isso permite cruzar com um de-para de categorias e depois re-agregar.
    """
    mes_obj = datetime.strptime(startdate, "%m/%d/%Y")
    first_day = mes_obj.date()
    if mes_obj.month == 12:
        last_day = date(mes_obj.year + 1, 1, 1) - timedelta(days=1)
    else:
        last_day = date(mes_obj.year, mes_obj.month + 1, 1) - timedelta(days=1)

    dias_no_mes = last_day.day
    is_complete_month = 1 if last_day < today else 0
    is_future_month = 1 if first_day > today else 0

    if is_future_month:
        days_elapsed = 0
    elif is_complete_month:
        days_elapsed = dias_no_mes
    else:
        days_elapsed = (today - first_day).days + 1

    if not blocked:
        return []

    grouped = {}

    for b in blocked:
        try:
            reservation_start = datetime.strptime(b["startdate"], "%m/%d/%Y").date()
            reservation_end = datetime.strptime(b["enddate"], "%m/%d/%Y").date()
            reason = str(b.get("reason") or "").strip()
            confirmation_id = extract_confirmation_id(reason)

            if reservation_end < first_day or reservation_start > last_day:
                continue

            overlap_start = max(reservation_start, first_day)
            overlap_end = min(reservation_end, last_day)
            reservation_key = build_reservation_key(
                unit_id=unit_id,
                month_str=month_str,
                reservation_startdate=reservation_start.isoformat(),
                reservation_enddate=reservation_end.isoformat(),
                reason=reason,
                confirmation_id=confirmation_id,
            )

            if reservation_key not in grouped:
                grouped[reservation_key] = {
                    "unit_id": unit_id,
                    "year": year,
                    "month": month,
                    "month_str": month_str,
                    "reservation_key": reservation_key,
                    "confirmation_id": confirmation_id,
                    "reason": reason,
                    "reservation_startdate": reservation_start,
                    "reservation_enddate": reservation_end,
                    "days_in_month": dias_no_mes,
                    "days_elapsed": days_elapsed,
                    "is_complete_month": is_complete_month,
                    "is_future_month": is_future_month,
                    "_days": set(),
                    "_days_past": set(),
                    "_days_future": set(),
                }

            current = overlap_start
            while current <= overlap_end:
                grouped[reservation_key]["_days"].add(current)
                if current <= today:
                    grouped[reservation_key]["_days_past"].add(current)
                else:
                    grouped[reservation_key]["_days_future"].add(current)
                current += timedelta(days=1)

        except Exception as e:
            print(f"[WARN] Falha ao processar bloqueio unit_id={unit_id} mês={month_str}: {e} | payload={b}")
            continue

    rows = []
    for item in grouped.values():
        days_occupied = min(len(item["_days"]), dias_no_mes)
        days_occupied_past = min(len(item["_days_past"]), dias_no_mes)
        days_occupied_future = min(len(item["_days_future"]), dias_no_mes)
        occupancy_rate = round(days_occupied / dias_no_mes, 4) if dias_no_mes else 0.0

        rows.append({
            "unit_id": item["unit_id"],
            "year": item["year"],
            "month": item["month"],
            "month_str": item["month_str"],
            "reservation_key": item["reservation_key"],
            "confirmation_id": item["confirmation_id"],
            "reason": item["reason"],
            "reservation_startdate": item["reservation_startdate"],
            "reservation_enddate": item["reservation_enddate"],
            "occupancy_rate": occupancy_rate,
            "days_occupied": days_occupied,
            "days_in_month": item["days_in_month"],
            "days_elapsed": item["days_elapsed"],
            "days_occupied_past": days_occupied_past,
            "days_occupied_future": days_occupied_future,
            "is_complete_month": item["is_complete_month"],
            "is_future_month": item["is_future_month"],
        })

    return rows



def get_active_houses():
    conn = mysql.connector.connect(**DB_CFG)
    try:
        query = "SELECT id FROM ovh_silver.tb_active_houses WHERE renting_type = 'RENTING'"
        df = pd.read_sql(query, conn)
        return df["id"].tolist()
    finally:
        conn.close()



def create_occupancy_tables():
    conn = mysql.connector.connect(**DB_CFG)
    try:
        cur = conn.cursor()

        # Tabela consolidada atual
        create_sql_summary = f"""
        CREATE TABLE IF NOT EXISTS `{TB_NAME}` (
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
            PRIMARY KEY (`unit_id`, `year`, `month`, `extraction_date`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """

        # Nova tabela detalhada por reserva
        create_sql_detail = f"""
        CREATE TABLE IF NOT EXISTS `{TB_DETAIL_NAME}` (
            `unit_id`               BIGINT       NOT NULL,
            `year`                  INT          NOT NULL,
            `month`                 INT          NOT NULL,
            `month_str`             VARCHAR(7)   NOT NULL,
            `reservation_key`       VARCHAR(32)  NOT NULL,
            `confirmation_id`       VARCHAR(50)  NULL,
            `reason`                VARCHAR(255) NULL,
            `reservation_startdate` DATE         NULL,
            `reservation_enddate`   DATE         NULL,
            `occupancy_rate`        DECIMAL(5,4) NULL,
            `days_occupied`         INT          NULL,
            `days_in_month`         INT          NULL,
            `days_elapsed`          INT          NULL,
            `days_occupied_past`    INT          NULL,
            `days_occupied_future`  INT          NULL,
            `is_complete_month`     TINYINT(1)   NULL,
            `is_future_month`       TINYINT(1)   NULL,
            `extraction_date`       DATETIME     NOT NULL,
            PRIMARY KEY (`unit_id`, `year`, `month`, `reservation_key`, `extraction_date`),
            KEY `idx_confirmation_id` (`confirmation_id`),
            KEY `idx_reason` (`reason`),
            KEY `idx_month_str` (`month_str`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """

        cur.execute(create_sql_summary)
        cur.execute(create_sql_detail)
        conn.commit()
    finally:
        conn.close()



def save_occupancy_data(df: pd.DataFrame):
    if df.empty:
        return

    conn = mysql.connector.connect(**DB_CFG)
    try:
        cur = conn.cursor()
        insert_sql = f"""
        INSERT INTO `{TB_NAME}`
            (unit_id, year, month, month_str,
             occupancy_rate, days_occupied, days_in_month,
             days_elapsed, days_occupied_past, days_occupied_future,
             is_complete_month, is_future_month,
             extraction_date)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            occupancy_rate       = VALUES(occupancy_rate),
            days_occupied        = VALUES(days_occupied),
            days_in_month        = VALUES(days_in_month),
            days_elapsed         = VALUES(days_elapsed),
            days_occupied_past   = VALUES(days_occupied_past),
            days_occupied_future = VALUES(days_occupied_future),
            is_complete_month    = VALUES(is_complete_month),
            is_future_month      = VALUES(is_future_month),
            extraction_date      = VALUES(extraction_date)
        """
        cur.executemany(insert_sql, [tuple(r) for r in df.itertuples(index=False, name=None)])
        conn.commit()
    finally:
        conn.close()



def save_occupancy_detail_data(df: pd.DataFrame):
    if df.empty:
        return

    conn = mysql.connector.connect(**DB_CFG)
    try:
        cur = conn.cursor()
        insert_sql = f"""
        INSERT INTO `{TB_DETAIL_NAME}`
            (unit_id, year, month, month_str,
             reservation_key, confirmation_id, reason,
             reservation_startdate, reservation_enddate,
             occupancy_rate, days_occupied, days_in_month,
             days_elapsed, days_occupied_past, days_occupied_future,
             is_complete_month, is_future_month,
             extraction_date)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            confirmation_id      = VALUES(confirmation_id),
            reason               = VALUES(reason),
            reservation_startdate= VALUES(reservation_startdate),
            reservation_enddate  = VALUES(reservation_enddate),
            occupancy_rate       = VALUES(occupancy_rate),
            days_occupied        = VALUES(days_occupied),
            days_in_month        = VALUES(days_in_month),
            days_elapsed         = VALUES(days_elapsed),
            days_occupied_past   = VALUES(days_occupied_past),
            days_occupied_future = VALUES(days_occupied_future),
            is_complete_month    = VALUES(is_complete_month),
            is_future_month      = VALUES(is_future_month),
            extraction_date      = VALUES(extraction_date)
        """
        cur.executemany(insert_sql, [tuple(r) for r in df.itertuples(index=False, name=None)])
        conn.commit()
    finally:
        conn.close()



def main():
    create_occupancy_tables()
    unit_ids = get_active_houses()
    if not unit_ids:
        return

    today = date.today()

    months_2026 = []
    for month in range(1, 13):
        startdate = f"{month:02d}/01/2026"
        if month == 12:
            enddate = "01/10/2027"
        else:
            last_day = (datetime(2026, month + 1, 1) - timedelta(days=1)).day
            extended_end = datetime(2026, month, last_day) + timedelta(days=10)
            enddate = extended_end.strftime("%m/%d/%Y")

        months_2026.append({
            "startdate": startdate,
            "enddate": enddate,
            "year": 2026,
            "month": month,
            "month_str": f"2026-{month:02d}",
        })

    session = requests.Session()
    summary_results = []
    detail_results = []
    extraction_date = datetime.now()

    for unit_id in unit_ids:
        print(f"\n=== Processando unit_id: {unit_id} ===")
        for month_info in months_2026:
            blocked = fetch_calendar(session, unit_id, month_info["startdate"], month_info["enddate"])

            # 1) mantém a tabela consolidada atual
            occ = calculate_occupancy_extended(blocked, month_info["startdate"], today)
            summary_results.append({
                "unit_id": unit_id,
                "year": month_info["year"],
                "month": month_info["month"],
                "month_str": month_info["month_str"],
                "occupancy_rate": occ["occupancy_rate"],
                "days_occupied": occ["days_occupied"],
                "days_in_month": occ["days_in_month"],
                "days_elapsed": occ["days_elapsed"],
                "days_occupied_past": occ["days_occupied_past"],
                "days_occupied_future": occ["days_occupied_future"],
                "is_complete_month": occ["is_complete_month"],
                "is_future_month": occ["is_future_month"],
                "extraction_date": extraction_date,
            })

            # 2) nova tabela detalhada com confirmation_id
            detail_rows = calculate_occupancy_detail(
                blocked=blocked,
                unit_id=unit_id,
                year=month_info["year"],
                month=month_info["month"],
                month_str=month_info["month_str"],
                startdate=month_info["startdate"],
                today=today,
            )
            for row in detail_rows:
                row["extraction_date"] = extraction_date
            detail_results.extend(detail_rows)

            print(
                f"  Casa {unit_id} | {month_info['month_str']} | "
                f"Ocupação consolidada: {occ['occupancy_rate']:.2%} ({occ['days_occupied']}/{occ['days_in_month']} dias) | "
                f"Reservas detalhadas: {len(detail_rows)}"
            )

    save_occupancy_data(pd.DataFrame(summary_results))
    save_occupancy_detail_data(pd.DataFrame(detail_results))


SP_TZ = pendulum.timezone("America/Sao_Paulo")
with DAG(
    dag_id="OVH-tb_occupancy_houses",
    start_date=pendulum.datetime(2025, 12, 19, 8, 0, tz=SP_TZ),
    schedule="0 2 * * *",
    catchup=False,
    tags=["Tabelas - OVH", "Ocupacao"],
) as dag:

    @task(retries=4, retry_exponential_backoff=True)
    def calculate_occupancy_2026():
        main()

    calculate_occupancy_2026()
