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
# Tokens "globais" (fallback). Use-os apenas se a reserva realmente pertencer a este tenant.
TOKEN_KEY = "a43cb1b5ed27cce283ab2bb4df540037"
TOKEN_SECRET = "72c7b8d5ba4b0ef14fe97a7e4179bdfa92cfc6ea"

# ===== FIX: confirmation_id único =====
# Preencha token_key/token_secret abaixo se a reserva for de outra administradora.
CONFIRMATIONS = [
    {
        "confirmation_id": 479193,                 # <- FIXADO
        "Administradora": "ONE VACATION HOME",
        # "token_key":   "<<<TOKEN_KEY_DA_ONE>>>",  # (opcional) recomendável preencher se for de outro tenant
        # "token_secret":"<<<TOKEN_SECRET_DA_ONE>>>"
    },
]

# Habilita log quando a API responder 200 mas vier data vazia (para inspeção).
DEBUG_LOG_EMPTY_JSON = True

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
            confirmation_id = str(item["confirmation_id"])  # força string
            admin = item.get("Administradora") or ""

            # usa token do item se fornecido; senão, cai no global
            tkey = item.get("token_key", TOKEN_KEY)
            tsecret = item.get("token_secret", TOKEN_SECRET)

            def build_payload(field_name, field_value):
                return {
                    "methodName": "GetReservationPrice",
                    "params": {
                        "token_key": tkey,
                        "token_secret": tsecret,
                        field_name: field_value,
                        "return_payments": 1
                    }
                }

            # 1ª tentativa com confirmation_id
            resp = post_with_retry(session, build_payload("confirmation_id", confirmation_id),
                                   retries=6, base_delay=1, timeout=60)

            data = None
            try_alt = False
            if resp is not None and resp.status_code == 200:
                try:
                    data = resp.json()
                except ValueError:
                    data = None

                # se vier vazio, tenta fallback com reservation_id
                if not (isinstance(data, dict) and data.get("data")):
                    try_alt = True
                    if DEBUG_LOG_EMPTY_JSON:
                        print(f"[debug] empty data (confirmation_id={confirmation_id}, admin={admin}) -> {resp.text[:500]}")

            if try_alt:
                resp = post_with_retry(session, build_payload("reservation_id", confirmation_id),
                                       retries=3, base_delay=1, timeout=60)
                data = None
                if resp is not None and resp.status_code == 200:
                    try:
                        data = resp.json()
                    except ValueError:
                        data = None

            # Tratamento final
            if resp is None:
                fails.append((confirmation_id, admin, "no_response", "None"))
                continue

            status = resp.status_code
            if status != 200:
                fails.append((confirmation_id, admin, status, resp.text[:200]))
                continue

            if not (isinstance(data, dict) and data.get("data")):
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
    else:
        # (opcional) cria a tabela vazia com um schema mínimo para não falhar na 1ª execução
        # Ajuste as colunas conforme sua resposta real de reservation_days
        schema_cols = ["reservation_date", "price", "reservation_id", "data_price", "Administradora"]
        pd.DataFrame(columns=schema_cols).head(0).to_sql("tb_reservas_price_day", con=engine, if_exists="append", index=False)

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
