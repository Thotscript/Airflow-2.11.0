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
from datetime import datetime, timedelta, date
from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowException

API_URL = "https://web.streamlinevrs.com/api/json"
TOKEN_KEY = "3ef223d3bbf7086cfb86df7e98d6e5d2"
TOKEN_SECRET = "a88d05b895affb815cc8a4d96670698ee486ea30"

DB_CFG = dict(
    host="host.docker.internal",
    user="root",
    password="Tfl1234@",
    database="ovh_bronze",
)

TB_NAME = "tb_occupancy_houses"

# ---- THROTTLE ----
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
                    try: time.sleep(float(retry_after))
                    except: time.sleep(delay)
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
    if isinstance(data, list): return data
    if not isinstance(data, dict): return []
    if "Response" in data:
        response_data = data.get("Response")
        if isinstance(response_data, dict):
            inner_data = response_data.get("data")
            if isinstance(inner_data, dict):
                blocked = inner_data.get("blocked_period", [])
                if blocked: return blocked
            elif isinstance(inner_data, list):
                return inner_data
    data_field = data.get("data")
    if isinstance(data_field, list): return data_field
    if isinstance(data_field, dict):
        blocked = data_field.get("blocked_period", [])
        if blocked: return blocked
    if "status" in data and not data_field: return []
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


def calculate_occupancy_extended(blocked: list, startdate: str, today: date) -> dict:
    """
    Calcula ocupação mensal com colunas extras para YTD e Full Year no Power BI.

    Colunas novas:
    - days_elapsed       : dias do mês já passados até hoje (inclusive)
                           mês passado  → days_in_month
                           mês atual    → dia de hoje dentro do mês
                           mês futuro   → 0
    - days_occupied_past : dias ocupados que já ocorreram (≤ hoje)
    - days_occupied_future: dias ocupados que ainda vão ocorrer (> hoje)
    - is_complete_month  : 1 se o mês inteiro já passou, 0 caso contrário
    - is_future_month    : 1 se o mês ainda não começou, 0 caso contrário

    Fórmulas Power BI:
      YTD Occupancy   = DIVIDE(SUM(days_occupied_past), SUM(days_elapsed))
      Full Year       = DIVIDE(SUM(days_occupied),      SUM(days_in_month))
    """
    mes_obj = datetime.strptime(startdate, "%m/%d/%Y")
    first_day = mes_obj.date()
    if mes_obj.month == 12:
        last_day = date(mes_obj.year + 1, 1, 1) - timedelta(days=1)
    else:
        last_day = date(mes_obj.year, mes_obj.month + 1, 1) - timedelta(days=1)

    dias_no_mes = last_day.day

    # ── Flags de status do mês ──────────────────────────────────────────────
    is_complete_month = 1 if last_day < today else 0
    is_future_month   = 1 if first_day > today else 0

    # ── days_elapsed: quantos dias do mês já "passaram" ─────────────────────
    if is_future_month:
        days_elapsed = 0
    elif is_complete_month:
        days_elapsed = dias_no_mes
    else:
        # Mês em curso: conta do dia 1 até hoje (inclusive)
        days_elapsed = (today - first_day).days + 1

    if not blocked:
        return {
            "occupancy_rate":      0.0,
            "days_occupied":       0,
            "days_in_month":       dias_no_mes,
            "days_elapsed":        days_elapsed,
            "days_occupied_past":  0,
            "days_occupied_future":0,
            "is_complete_month":   is_complete_month,
            "is_future_month":     is_future_month,
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
        "occupancy_rate":      round(ocupacao, 4),
        "days_occupied":       dias_ocupados,
        "days_in_month":       dias_no_mes,
        "days_elapsed":        days_elapsed,
        "days_occupied_past":  days_occupied_past,
        "days_occupied_future":days_occupied_future,
        "is_complete_month":   is_complete_month,
        "is_future_month":     is_future_month,
    }


def get_active_houses():
    conn = mysql.connector.connect(**DB_CFG)
    try:
        query = "SELECT id FROM ovh_silver.tb_active_houses WHERE renting_type = 'RENTING'"
        df = pd.read_sql(query, conn)
        return df['id'].tolist()
    finally:
        conn.close()

def create_occupancy_table():
    conn = mysql.connector.connect(**DB_CFG)
    try:
        cur = conn.cursor()
        create_sql = f"""
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
        cur.execute(create_sql)
        conn.commit()
    finally:
        conn.close()

def save_occupancy_data(df: pd.DataFrame):
    if df.empty: return
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


def main_2025():
    create_occupancy_table()
    unit_ids = get_active_houses()
    if not unit_ids:
        print("[INFO] Nenhuma casa ativa encontrada.")
        return

    # 2025 já passou inteiro → today é fixo no último dia do ano para o backfill
    # Mantemos date.today() real para que days_elapsed seja correto:
    # todos os meses de 2025 serão is_complete_month=1 e days_elapsed=days_in_month
    today = date.today()

    months_2025 = []
    for month in range(1, 13):
        startdate = f"{month:02d}/01/2025"
        if month == 12:
            enddate = "01/10/2026"
        else:
            last_day = (datetime(2025, month + 1, 1) - timedelta(days=1)).day
            extended_end = datetime(2025, month, last_day) + timedelta(days=10)
            enddate = extended_end.strftime("%m/%d/%Y")
        months_2025.append({
            'startdate': startdate,
            'enddate':   enddate,
            'year':      2025,
            'month':     month,
            'month_str': f"2025-{month:02d}",
        })

    session = requests.Session()
    results = []
    extraction_date = datetime.now()

    for unit_id in unit_ids:
        print(f"\n=== Processando unit_id: {unit_id} ===")
        for month_info in months_2025:
            blocked = fetch_calendar(session, unit_id, month_info['startdate'], month_info['enddate'])
            occ = calculate_occupancy_extended(blocked, month_info['startdate'], today)

            results.append({
                'unit_id':              unit_id,
                'year':                 month_info['year'],
                'month':                month_info['month'],
                'month_str':            month_info['month_str'],
                'occupancy_rate':       occ['occupancy_rate'],
                'days_occupied':        occ['days_occupied'],
                'days_in_month':        occ['days_in_month'],
                'days_elapsed':         occ['days_elapsed'],
                'days_occupied_past':   occ['days_occupied_past'],
                'days_occupied_future': occ['days_occupied_future'],
                'is_complete_month':    occ['is_complete_month'],
                'is_future_month':      occ['is_future_month'],
                'extraction_date':      extraction_date,
            })
            print(
                f"  Casa {unit_id} | {month_info['month_str']} | "
                f"Ocupação: {occ['occupancy_rate']:.2%} ({occ['days_occupied']}/{occ['days_in_month']} dias) | "
                f"Passado: {occ['days_occupied_past']} | Futuro: {occ['days_occupied_future']} | "
                f"Elapsed: {occ['days_elapsed']}"
            )

    save_occupancy_data(pd.DataFrame(results))
    print(f"\n[INFO] Concluído. {len(results)} registros salvos/atualizados.")


SP_TZ = pendulum.timezone("America/Sao_Paulo")

with DAG(
    dag_id="OVH-tb_occupancy_houses_2025_backfill",
    start_date=pendulum.datetime(2025, 12, 19, 8, 0, tz=SP_TZ),
    schedule="@once",
    catchup=False,
    tags=["Tabelas - OVH", "Ocupacao", "Backfill"],
) as dag:

    @task(retries=4, retry_exponential_backoff=True)
    def calculate_occupancy_2025():
        main_2025()

    calculate_occupancy_2025()