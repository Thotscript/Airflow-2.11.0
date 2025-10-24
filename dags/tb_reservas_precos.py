import json
import time
from datetime import datetime
from collections import deque

import pandas as pd
import requests
from sqlalchemy import create_engine
from urllib.parse import quote_plus
import pendulum
from airflow import DAG
from airflow.decorators import task

# ==============================
# CONFIG API (inline)
# ==============================
API_URL = "https://web.streamlinevrs.com/api/json"
TOKEN_KEY = "a43cb1b5ed27cce283ab2bb4df540037"
TOKEN_SECRET = "72c7b8d5ba4b0ef14fe97a7e4179bdfa92cfc6ea"

# Lista inline de confirmações (exemplos; ajuste conforme necessário)
# Você pode adicionar/remover itens livremente.
CONFIRMATIONS = [
    {"confirmation_id": "479193", "Administradora": "ONE VACATION HOME"},
]

# ==============================
# THROTTLE (sem libs externas)
# ==============================
WINDOW_SEC = 60
MAX_CALLS = 95  # folga abaixo do limite oficial (100/min)
_call_times = deque()

def throttle():
    now = time.time()
    _call_times.append(now)
    # remove chamadas fora da janela
    while _call_times and (now - _call_times[0]) > WINDOW_SEC:
        _call_times.popleft()
    # se estourou a janela, aguarda liberar
    if len(_call_times) >= MAX_CALLS:
        sleep_for = WINDOW_SEC - (now - _call_times[0]) + 0.05
        if sleep_for > 0:
            time.sleep(sleep_for)

# ==============================
# POST c/ retry + backoff
# ==============================
def post_with_retry(session, payload, retries=6, base_delay=1, timeout=60):
    delay = base_delay
    last_resp = None
    for _ in range(retries):
        throttle()
        try:
            resp = session.post(
                API_URL,
                data=json.dumps(payload),
                headers={"Content-Type": "application/json"},
                timeout=timeout
            )
            last_resp = resp

            if resp.status_code == 200:
                return resp

            # respeita Retry-After p/ 429/5xx
            if resp.status_code in (429, 500, 502, 503, 504):
                retry_after = resp.headers.get("Retry-After")
                if retry_after:
                    try:
                        time.sleep(float(retry_after))
                    except ValueError:
                        time.sleep(delay)
                else:
                    time.sleep(delay)
                delay = min(delay * 2, 30)
                continue

            # erros não-retryáveis
            return resp
        except requests.RequestException:
            time.sleep(delay)
            delay = min(delay * 2, 30)
            continue
    return last_resp

# ==============================
# LÓGICA PRINCIPAL
# ==============================
def main():
    start_time = time.time()
    now_str = datetime.now().strftime('%Y/%m/%d %H:%M:%S')

    # ---- ENGINE MySQL (igual ao exemplo) ----
    DB_USER = "root"
    DB_PASS = "Tfl1234@"
    DB_HOST = "host.docker.internal"
    DB_PORT = 3306
    DB_NAME = "ovh_silver"

    engine = create_engine(
        f"mysql+pymysql://{DB_USER}:{quote_plus(DB_PASS)}@{DB_HOST}:{DB_PORT}/{DB_NAME}?charset=utf8mb4"
    )

    frames = []
    fails = []
    ok = 0

    # ---- LOOP DE CHAMADAS (inline) ----
    with requests.Session() as session:
        for item in CONFIRMATIONS:
            confirmation_id = str(item["confirmation_id"])
            admin = item.get("Administradora") or ""

            payload = {
                "methodName": "GetReservationPrice",
                "params": {
                    "token_key": TOKEN_KEY,
                    "token_secret": TOKEN_SECRET,
                    "confirmation_id": confirmation_id,
                    "return_payments": 1
                }
            }

            resp = post_with_retry(session, payload, retries=6, base_delay=1, timeout=60)
            if resp is None:
                fails.append((confirmation_id, admin, "no_response", "None"))
                continue

            status = resp.status_code
            if status != 200:
                fails.append((confirmation_id, admin, status, resp.text[:200]))
                continue

            # tenta decodificar JSON
            try:
                data = resp.json()
            except ValueError as e:
                fails.append((confirmation_id, admin, status, f"json_error: {e}"))
                continue

            # checagens básicas
            if not isinstance(data, dict) or "data" not in data or not data["data"]:
                fails.append((confirmation_id, admin, status, "empty data"))
                continue

            d = data["data"]
            reservation_id = d.get("reservation_id")
            reservation_days = d.get("reservation_days")

            if not reservation_id or not isinstance(reservation_days, list) or not reservation_days:
                fails.append((confirmation_id, admin, status, "missing reservation_days"))
                continue

            try:
                df_days = pd.DataFrame(reservation_days)
                # anexa colunas auxiliares
                df_days["reservation_id"] = reservation_id
                df_days["data_price"] = now_str
                df_days["Administradora"] = admin
                frames.append(df_days)
                ok += 1
            except Exception as e:
                fails.append((confirmation_id, admin, "build_df_error", str(e)))
                continue

    # ---- PERSISTIR TABELA NO MySQL ----
    if frames:
        tb_price_reservas = pd.concat(frames, ignore_index=True)
        # histórico acumulado
        tb_price_reservas.to_sql("tb_reservas_price_day", con=engine, if_exists="append", index=False)

    engine.dispose()

    end_time = time.time()
    execution_time = end_time - start_time

    print(f"Tabela salva no banco! Sucesso: {ok}  Falhas: {len(fails)}")
    print(f"Tempo de execução: {execution_time:.2f} segundos")
    if fails:
        print("Falhas (amostra de 10):")
        for f in fails[:10]:
            print(f)

# ==============================
# DAG (Airflow)
# ==============================
SP_TZ = pendulum.timezone("America/Sao_Paulo")

with DAG(
    dag_id="OVH-tb_reservas_price_day",
    start_date=pendulum.datetime(2025, 9, 23, 8, 0, tz=SP_TZ),
    schedule="0 5 * * *",  # todos os dias 05:00 BRT
    catchup=False,
    tags=["Tabelas - OVH"],
) as dag:

    @task()
    def run_reservas_price_day():
        main()

    run_reservas_price_day()
