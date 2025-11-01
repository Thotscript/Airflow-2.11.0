import pandas as pd
import requests
import json
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
import time
from collections import deque
from threading import Lock

from sqlalchemy import create_engine, inspect
from sqlalchemy.dialects.mysql import insert as mysql_insert
from sqlalchemy import MetaData, Table
from urllib.parse import quote_plus

import pendulum
from airflow import DAG
from airflow.decorators import task

# ==============================
# CONFIG MYSQL
# ==============================
DB_USER = "root"
DB_PASS = "Tfl1234@"
DB_HOST = "host.docker.internal"
DB_PORT = 3306
DB_NAME = "ovh_silver"

# ==============================
# TOKENS FIXOS (tenant único)
# ==============================
TOKEN_KEY = "a43cb1b5ed27cce283ab2bb4df540037"
TOKEN_SECRET = "72c7b8d5ba4b0ef14fe97a7e4179bdfa92cfc6ea"

# ==============================
# API
# ==============================
URL = "https://web.streamlinevrs.com/api/json"
HEADERS = {"Content-Type": "application/json"}

# Datas auxiliares
TODAY_STR = datetime.now().strftime('%Y/%m/%d')
NOW_STR = datetime.now().strftime('%Y/%m/%d %H:%M:%S')
END_DATE_STR = (datetime.now() + timedelta(days=547)).strftime('%Y/%m/%d')

# ==============================
# RATE LIMIT MANUAL (thread-safe)
# ==============================
WINDOW_SEC = 60
MAX_CALLS = 95  # folga abaixo do limite oficial (100/min)
_call_times = deque()
_call_lock = Lock()

def throttle():
    """Garante no máx. MAX_CALLS por WINDOW_SEC. Bloqueia a thread se atingir o limite."""
    now = time.time()
    sleep_for = 0.0
    with _call_lock:
        # remove timestamps fora da janela
        while _call_times and (now - _call_times[0]) > WINDOW_SEC:
            _call_times.popleft()
        if len(_call_times) >= MAX_CALLS:
            sleep_for = WINDOW_SEC - (now - _call_times[0]) + 0.05
        # registra a chamada somente após possível espera
    if sleep_for > 0:
        time.sleep(sleep_for)
    with _call_lock:
        _call_times.append(time.time())

# ==============================
# HELPERS de retry/backoff
# ==============================
def _should_retry_http(status_code: int) -> bool:
    return status_code in (429, 502, 503, 504)

def _extract_status_code_from_body_json(text: str):
    try:
        obj = json.loads(text)
        if isinstance(obj, dict):
            st = obj.get("status") or {}
            return st.get("code")
    except Exception:
        pass
    return None

def post_with_retry(payload: dict, timeout=60, max_retries=6):
    """POST com throttle + retry/backoff exponencial e detecção de E0013 no body JSON."""
    base = 1.0
    for attempt in range(max_retries):
        throttle()
        try:
            resp = requests.post(URL, data=json.dumps(payload), headers=HEADERS, timeout=timeout)
        except requests.RequestException as e:
            # erro de rede -> backoff
            wait = base * (2 ** attempt)
            time.sleep(wait)
            continue

        # HTTP OK
        if resp.status_code == 200:
            # Pode vir E0013 dentro do JSON
            code_in_body = _extract_status_code_from_body_json(resp.text)
            if code_in_body == "E0013":
                # estourou RPM do provedor; espera virar a janela
                time.sleep(60)
                continue
            return resp

        # HTTP ruim mas recuperável
        if _should_retry_http(resp.status_code):
            wait = base * (2 ** attempt)
            time.sleep(wait)
            continue

        # erro definitivo
        return resp
    return None

# ==============================
# Função chamada em paralelo
# Parâmetros: (confirmation_id, admin)
# ==============================
def get_unit_data(confirmation_id, admin):
    payload = {
        "methodName": "GetReservationPrice",
        "params": {
            "token_key": TOKEN_KEY,
            "token_secret": TOKEN_SECRET,
            "confirmation_id": str(confirmation_id),
            "return_payments": 1
        }
    }

    resp = post_with_retry(payload, timeout=60, max_retries=6)
    if resp is None:
        print(f"[warn] Falha após retries - confirmation_id={confirmation_id} admin={admin}")
        return None

    if resp.status_code != 200:
        print(f"[warn] HTTP {resp.status_code} - confirmation_id={confirmation_id} admin={admin} body={resp.text[:200]}")
        return None

    try:
        data_dict = resp.json()
    except ValueError:
        print(f"[warn] JSON inválido - confirmation_id={confirmation_id} admin={admin} body={resp.text[:200]}")
        return None

    # Valida estrutura esperada
    if not isinstance(data_dict, dict) or not data_dict.get("data"):
        status = data_dict.get("status", {})
        print(f"[info] data vazio - confirmation_id={confirmation_id} admin={admin} status={status}")
        return None

    d = data_dict["data"]
    reservation_id = d.get("reservation_id")
    reservation_days = d.get("reservation_days")

    if not reservation_id or not isinstance(reservation_days, list) or not reservation_days:
        print(f"[info] reservation_days ausente - confirmation_id={confirmation_id} admin={admin}")
        return None

    df_reservation_days = pd.DataFrame(reservation_days)

    # Colunas auxiliares
    df_reservation_days["reservation_id"] = reservation_id
    df_reservation_days["data_price"] = NOW_STR
    df_reservation_days["Administradora"] = admin

    return df_reservation_days

# ==============================
# Persistência com schema-aware
# ==============================
def normalize_and_write(engine, df: pd.DataFrame, table_name: str):
    if df.empty:
        print("Nada para gravar (DataFrame vazio).")
        return

    # conversões
    # 'date' vem como 'DD/MM/YYYY' -> DATE
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], dayfirst=True, errors="coerce").dt.date

    if "data_price" in df.columns:
        df["data_price"] = pd.to_datetime(df["data_price"], errors="coerce")

    for nc in ["price", "extra", "discount", "original_cost", "reservation_id"]:
        if nc in df.columns:
            df[nc] = pd.to_numeric(df[nc], errors="coerce")

    # garanta as colunas alvo, mesmo que venham ausentes (preenche com NaN/None)
    target_cols = [
        "date", "season", "price", "extra", "discount", "original_cost",
        "reservation_id", "data_price", "Administradora"
    ]
    for c in target_cols:
        if c not in df.columns:
            df[c] = None

    # reordena e grava
    df = df[target_cols]

    df.to_sql(
        table_name,
        engine,
        if_exists="append",
        index=False,
        chunksize=10000,
        method="multi",
    )
    print(f"Gravou {len(df)} linhas em '{table_name}'.")


# ==============================
# Lógica principal
# ==============================
def main():
    start_time = time.time()

    engine = create_engine(
        f"mysql+pymysql://{DB_USER}:{quote_plus(DB_PASS)}@{DB_HOST}:{DB_PORT}/{DB_NAME}?charset=utf8mb4"
    )

    # Carrega confirmation_id e Administradora
    df = pd.read_sql(
        """
        SELECT 
            a.confirmation_id,
            a.Administradora
        FROM tb_reservas a 
        WHERE 1=1
        AND days_number > 1
        AND status_id IN (4,5,6,7)
        """,
        con=engine
    )
    # df = df.head(20000)  # opcional

    params_list = [(row.confirmation_id, row.Administradora) for row in df.itertuples(index=False)]

    # Paralelismo controlado
    with ThreadPoolExecutor(max_workers=4) as executor:
        results = list(executor.map(lambda x: get_unit_data(*x), params_list))

    # Filtra Nones
    results = [r for r in results if r is not None]

    if not results:
        print("Nenhum resultado retornado (sem linhas para gravar).")
        tb_price_reservas = pd.DataFrame()
    else:
        tb_price_reservas = pd.concat(results, ignore_index=True)

    # Pré-visualização
    print(tb_price_reservas.head(10))

    # Persistência no MySQL (append) com normalização de schema
    if not tb_price_reservas.empty:
        normalize_and_write(engine, tb_price_reservas, table_name="tb_reservas_price_day")
    else:
        print("Nada para gravar (DataFrame vazio).")

    engine.dispose()

    end_time = time.time()
    execution_time = end_time - start_time
    print(f"Tempo de execução: {execution_time:.2f} segundos")

# === Airflow ===
SP_TZ = pendulum.timezone("America/Sao_Paulo")

with DAG(
    dag_id="OVH-tb_reservas_price_day",
    start_date=pendulum.datetime(2025, 9, 23, 8, 0, tz=SP_TZ),
    schedule="15 0 * * *",  # todos os dias 05:00 BRT
    catchup=False,
    tags=["Tabelas - OVH"],
    max_active_runs=1,    # evita concorrência do próprio DAG
) as dag:

    @task(
        task_id="run_tb_reservas_price_day",
        retries=4,
        retry_exponential_backoff=True,
    )
    def run_tb_reservas_price_day():
        main()

    run_tb_reservas_price_day()

