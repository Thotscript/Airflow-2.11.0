import os
import json
import re
import time
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

TB_NAME = "tb_occupancy_reservation_month"

# ---- THROTTLE (Controle de Vazão) ----
WINDOW_SEC = 60
MAX_CALLS = 90
_call_times = deque()

# Extrai o confirmation_id do campo reason: "Reservation #23622"
RESERVATION_ID_REGEX = re.compile(r"Reservation\s*#\s*(\d+)", re.IGNORECASE)


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
                    print(f"[RETRY] Resposta nula (possível rate limit). Tentativa {attempt + 1}/{retries}")
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
            print(f"[RETRY] Exceção na chamada: {e}. Tentativa {attempt + 1}")
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
        if resp is None:
            print(f"[ERROR] Resposta vazia para unit_id={unit_id}")
            return []

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
        print(f"[DEBUG] unit_id={unit_id}: extract_blocked_period retornou {len(blocked)} bloqueios")
        return blocked if blocked else []

    except Exception as e:
        print(f"[ERROR] Erro inesperado para unit_id={unit_id}: {e}")
        return []


def extract_confirmation_id(block: dict):
    reason = str(block.get("reason", "")).strip()
    match = RESERVATION_ID_REGEX.search(reason)
    if match:
        return match.group(1)
    return None


def build_reservation_month_rows(blocked: list, unit_id: int, month_info: dict, extraction_date: datetime):
    """
    Gera 1 linha por reserva no mês:
      unit_id + confirmation_id + ano + mês

    Observação:
    - Usa o campo reason para extrair o confirmation_id:
        "Reservation #23622" -> "23622"
    - Ignora bloqueios sem esse padrão, porque não haverá JOIN confiável com tb_reservas.
    """
    mes_obj = datetime.strptime(month_info["startdate"], "%m/%d/%Y")
    first_day = mes_obj.date()

    if mes_obj.month == 12:
        last_day = date(mes_obj.year + 1, 1, 1) - timedelta(days=1)
    else:
        last_day = date(mes_obj.year, mes_obj.month + 1, 1) - timedelta(days=1)

    rows = []

    for b in blocked:
        try:
            start = datetime.strptime(b["startdate"], "%m/%d/%Y").date()
            end = datetime.strptime(b["enddate"], "%m/%d/%Y").date()

            # ignora reservas totalmente fora do mês
            if end < first_day or start > last_day:
                continue

            overlap_start = max(start, first_day)
            overlap_end = min(end, last_day)
            days_occupied = (overlap_end - overlap_start).days + 1

            reason = str(b.get("reason", "")).strip()
            confirmation_id = extract_confirmation_id(b)

            # só grava itens que realmente são reservas identificáveis
            if not confirmation_id:
                print(f"[INFO] unit_id={unit_id} | {month_info['month_str']} | bloqueio ignorado sem reservation id: {reason}")
                continue

            rows.append({
                "unit_id": unit_id,
                "confirmation_id": confirmation_id,
                "reason": reason,
                "year": month_info["year"],
                "month": month_info["month"],
                "month_str": month_info["month_str"],
                "startdate": start,
                "enddate": end,
                "overlap_start": overlap_start,
                "overlap_end": overlap_end,
                "days_occupied": days_occupied,
                "extraction_date": extraction_date,
            })

        except Exception as e:
            print(f"[WARN] Erro processando bloqueio unit_id={unit_id}: {e} | bloco={b}")
            continue

    return rows


def get_active_houses():
    conn = mysql.connector.connect(**DB_CFG)
    try:
        query = "SELECT id FROM ovh_silver.tb_active_houses WHERE renting_type = 'RENTING'"
        df = pd.read_sql(query, conn)
        return df["id"].tolist()
    finally:
        conn.close()


def create_occupancy_table():
    conn = mysql.connector.connect(**DB_CFG)
    try:
        cur = conn.cursor()
        create_sql = f"""
        CREATE TABLE IF NOT EXISTS `{TB_NAME}` (
            `unit_id` BIGINT NOT NULL,
            `confirmation_id` VARCHAR(50) NOT NULL,
            `reason` VARCHAR(255) NULL,
            `year` INT NOT NULL,
            `month` INT NOT NULL,
            `month_str` VARCHAR(7) NOT NULL,
            `startdate` DATE NOT NULL,
            `enddate` DATE NOT NULL,
            `overlap_start` DATE NOT NULL,
            `overlap_end` DATE NOT NULL,
            `days_occupied` INT NOT NULL,
            `extraction_date` DATETIME NOT NULL,
            PRIMARY KEY (`unit_id`, `confirmation_id`, `year`, `month`),
            KEY `idx_confirmation_id` (`confirmation_id`),
            KEY `idx_month_str` (`month_str`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """
        cur.execute(create_sql)
        conn.commit()
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
        INSERT INTO `{TB_NAME}`
        (
            unit_id, confirmation_id, reason,
            year, month, month_str,
            startdate, enddate, overlap_start, overlap_end,
            days_occupied, extraction_date
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            reason = VALUES(reason),
            startdate = VALUES(startdate),
            enddate = VALUES(enddate),
            overlap_start = VALUES(overlap_start),
            overlap_end = VALUES(overlap_end),
            days_occupied = VALUES(days_occupied),
            extraction_date = VALUES(extraction_date)
        """
        cur.executemany(insert_sql, [tuple(r) for r in df.itertuples(index=False, name=None)])
        conn.commit()
        print(f"[INFO] {cur.rowcount} linhas inseridas/atualizadas em {TB_NAME}")
    finally:
        conn.close()


def main():
    create_occupancy_table()
    unit_ids = get_active_houses()
    if not unit_ids:
        print("[INFO] Nenhuma casa ativa encontrada.")
        return

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
    results = []
    extraction_date = datetime.now()

    for unit_id in unit_ids:
        print(f"\n=== Processando unit_id: {unit_id} ===")
        for month_info in months_2026:
            blocked = fetch_calendar(session, unit_id, month_info["startdate"], month_info["enddate"])
            rows = build_reservation_month_rows(blocked, unit_id, month_info, extraction_date)
            results.extend(rows)

            print(
                f"  Casa {unit_id} | {month_info['month_str']} | "
                f"reservas identificadas: {len(rows)}"
            )

    df_results = pd.DataFrame(results)

    if not df_results.empty:
        # remove duplicidades defensivamente
        df_results = df_results.drop_duplicates(
            subset=["unit_id", "confirmation_id", "year", "month"],
            keep="last"
        )

    save_occupancy_data(df_results)


SP_TZ = pendulum.timezone("America/Sao_Paulo")

with DAG(
    dag_id="OVH-tb_occupancy_houses",
    start_date=pendulum.datetime(2025, 12, 19, 8, 0, tz=SP_TZ),
    schedule="0 2 * * *",
    catchup=False,
    tags=["Tabelas - OVH", "Ocupacao", "Reservas"],
) as dag:

    @task(retries=4, retry_exponential_backoff=True)
    def calculate_occupancy_2026():
        main()

    calculate_occupancy_2026()