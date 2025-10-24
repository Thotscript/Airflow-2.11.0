import pandas as pd
import requests
import json
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
import time
from collections import deque
from threading import Lock

from sqlalchemy import create_engine
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
        while _call_times and (now - _call_times[0]) > WINDOW_SEC:
            _call_times.popleft()
        if len(_call_times) >= MAX_CALLS:
            sleep_for = WINDOW_SEC - (now - _call_times[0]) + 0.05
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
        except requests.RequestException:
            wait = base * (2 ** attempt)
            time.sleep(wait)
            continue

        if resp.status_code == 200:
            code_in_body = _extract_status_code_from_body_json(resp.text)
            if code_in_body == "E0013":
                time.sleep(60)  # rate limit do provedor
                continue
            return resp

        if _should_retry_http(resp.status_code):
            wait = base * (2 ** attempt)
            time.sleep(wait)
            continue

        return resp
    return None

# ==============================
# Função chamada em paralelo
# Parâmetros: (confirmation_id, admin)
# Coleta: id, confirmation_id, referrer_url
# ==============================
def get_unit_data(confirmation_id, admin):
    payload = {
        "methodName": "GetReservationInfo",
        "params": {
            "token_key": TOKEN_KEY,
            "token_secret": TOKEN_SECRET,
            "confirmation_id": str(confirmation_id),
            "show_agents_referrer_information": 1
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

    if not isinstance(data_dict, dict) or not data_dict.get("data"):
        status = data_dict.get("status", {})
        print(f"[info] data vazio - confirmation_id={confirmation_id} admin={admin} status={status}")
        return None

    res = data_dict["data"].get("reservation")
    if not isinstance(res, dict):
        print(f"[info] campo 'reservation' ausente - confirmation_id={confirmation_id} admin={admin}")
        return None

    # Extrai somente o necessário
    row = {
        "id": res.get("id"),
        "confirmation_id": res.get("confirmation_id"),
        "referrer_url": res.get("referrer_url"),
        "data_coleta": NOW_STR,
        "Administradora": admin
    }

    df_ref = pd.DataFrame([row])
    return df_ref

# ==============================
# Persistência com normalização
# Tabela alvo: tb_referrer
# ==============================
def normalize_and_write(engine, df: pd.DataFrame, table_name: str = "tb_referrer"):
    if df.empty:
        print("Nada para gravar (DataFrame vazio).")
        return

    # Tipos
    for c in ["id", "confirmation_id"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    if "data_coleta" in df.columns:
        df["data_coleta"] = pd.to_datetime(df["data_coleta"], errors="coerce")

    # Garante colunas alvo
    target_cols = ["id", "confirmation_id", "referrer_url", "data_coleta", "Administradora"]
    for c in target_cols:
        if c not in df.columns:
            df[c] = None
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

    # === MANTIDA A MESMA CONSULTA (corrigido 'DISTINC' -> 'DISTINCT') ===
    df = pd.read_sql(
        """
        SELECT 
            a.confirmation_id,
            a.Administradora
        FROM tb_reservas a
        LEFT JOIN (SELECT DISTINCT reservation_id, 'SIM' AS CARREGADO FROM tb_reservas_price_day) b
            ON a.id = b.reservation_id 
        WHERE 1=1
        AND days_number > 1
        AND CARREGADO IS NULL
        AND status_id IN (4,5,6,7)
        """,
        con=engine
    )
    # df = df.head(20000)  # opcional para limitar

    params_list = [(row.confirmation_id, row.Administradora) for row in df.itertuples(index=False)]

    with ThreadPoolExecutor(max_workers=4) as executor:
        results = list(executor.map(lambda x: get_unit_data(*x), params_list))

    results = [r for r in results if r is not None]

    if not results:
        print("Nenhum resultado retornado (sem linhas para gravar).")
        tb_referrer = pd.DataFrame()
    else:
        tb_referrer = pd.concat(results, ignore_index=True)

    print(tb_referrer.head(10))

    if not tb_referrer.empty:
        normalize_and_write(engine, tb_referrer, table_name="tb_referrer")
    else:
        print("Nada para gravar (DataFrame vazio).")

    engine.dispose()

    end_time = time.time()
    execution_time = end_time - start_time
    print(f"Tempo de execução: {execution_time:.2f} segundos")

# === Airflow ===
SP_TZ = pendulum.timezone("America/Sao_Paulo")

with DAG(
    dag_id="OVH-tb_referrer",
    start_date=pendulum.datetime(2025, 9, 23, 8, 0, tz=SP_TZ),
    schedule="0 5 * * *",  # todos os dias 05:00 BRT
    catchup=False,
    tags=["Tabelas - OVH"],
    max_active_runs=1,    # evita concorrência do próprio DAG
) as dag:

    @task(
        task_id="run_tb_referrer",
        retries=4,
        retry_exponential_backoff=True,
    )
    def run_tb_referrer():
        main()

    run_tb_referrer()
