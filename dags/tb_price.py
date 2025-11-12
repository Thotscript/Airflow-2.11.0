import json
from datetime import datetime, timedelta
from collections import deque
import time

import pandas as pd
import requests
from sqlalchemy import create_engine
from urllib.parse import quote_plus
from airflow import DAG
from airflow.decorators import task
import pendulum


API_URL = "https://web.streamlinevrs.com/api/json"
TOKEN_KEY = "d9cec367a327955392db9424e0462a79"
TOKEN_SECRET = "6ec0ccc91eabaf773a9cd6ad5ba0acb377a6c958"

# ---- THROTTLE (sem libs externas) ----
WINDOW_SEC = 60
MAX_CALLS = 95  # deixe folga abaixo do limite oficial
_call_times = deque()

def throttle():
    now = time.time()
    _call_times.append(now)
    # remove chamadas mais antigas que a janela
    while _call_times and (now - _call_times[0]) > WINDOW_SEC:
        _call_times.popleft()
    # se estourou a janela, dorme até liberar
    if len(_call_times) >= MAX_CALLS:
        sleep_for = WINDOW_SEC - (now - _call_times[0]) + 0.05
        if sleep_for > 0:
            time.sleep(sleep_for)

# ---- POST com retry/backoff (sem libs externas) ----
def post_with_retry(session, payload, retries=5, base_delay=1, timeout=60):
    delay = base_delay
    for attempt in range(retries):
        throttle()
        resp = session.post(API_URL, data=json.dumps(payload),
                            headers={"Content-Type": "application/json"},
                            timeout=timeout)
        # sucesso
        if resp.status_code == 200:
            return resp

        # respeita Retry-After se presente
        if resp.status_code in (429, 500, 502, 503, 504):
            retry_after = resp.headers.get("Retry-After")
            if retry_after:
                try:
                    time.sleep(float(retry_after))
                except ValueError:
                    time.sleep(delay)
            else:
                time.sleep(delay)
            delay = min(delay * 2, 30)  # cap no backoff
            continue

        # erros não-retryáveis -> retorna como está
        return resp

    return resp  # última resposta


def main():
    # --- CONFIG BÁSICA ---
    DB_USER = "root"
    DB_PASS = "Tfl1234@"
    DB_HOST = "host.docker.internal"
    DB_PORT = 3306
    DB_NAME = "ovh_silver"

    # --- DATAS ---
    today = datetime.now().strftime('%Y/%m/%d')
    now_str = datetime.now().strftime('%Y/%m/%d %H:%M:%S')
    end_date = (datetime.now() + timedelta(days=547)).strftime('%Y/%m/%d')

    # --- ENGINE (já com o database definido) ---
    engine = create_engine(
        f"mysql+pymysql://{DB_USER}:{quote_plus(DB_PASS)}@{DB_HOST}:{DB_PORT}/{DB_NAME}?charset=utf8mb4"
    )

    # --- LER UNIDADES ---
    df_property = pd.read_sql(
        "SELECT id, Administradora FROM tb_property_list_wordpress WHERE id <> 346521",
        con=engine
    )

    frames = []
    fails = []
    ok = 0

    with requests.Session() as session:
        for row in df_property.itertuples(index=False):
            payload = {
                "methodName": "GetPropertyRates",
                "params": {
                    "token_key": TOKEN_KEY,
                    "token_secret": TOKEN_SECRET,
                    "unit_id": int(row.id),
                    "startdate": today,
                    "enddate": end_date
                }
            }

            try:
                resp = post_with_retry(session, payload, retries=6, base_delay=1, timeout=60)
                status = resp.status_code
                data = resp.json() if status == 200 else {}

                if status != 200:
                    fails.append((row.id, row.Administradora, status, resp.text[:200]))
                    continue

                if not (isinstance(data, dict) and data.get("data")):
                    # resposta válida porém vazia
                    fails.append((row.id, row.Administradora, status, "empty data"))
                    continue

                df = pd.json_normalize(data["data"])
                df["id_unit"] = int(row.id)
                df["data_price"] = now_str
                df["Administradora"] = row.Administradora
                frames.append(df)
                ok += 1

            except requests.RequestException as e:
                fails.append((row.id, row.Administradora, "exception", str(e)))

    # --- CONCATENAR E GRAVAR ---
    if frames:
        tb_price_total = pd.concat(frames, ignore_index=True)
        tb_price_total.to_sql("tb_price_history", con=engine, if_exists="append", index=False)
        tb_price_total.to_sql("tb_price",        con=engine, if_exists="replace", index=False)

    engine.dispose()

    print(f"Tabela salva no banco! Sucesso: {ok}  Falhas: {len(fails)}")
    if fails:
        # imprime só amostra para log enxuto
        print("Falhas (amostra de 10):")
        for f in fails[:10]:
            print(f)


if __name__ == "__main__":
    main()


SP_TZ = pendulum.timezone("America/Sao_Paulo")

with DAG(
    dag_id="OVH-tb_price",
    start_date=pendulum.datetime(2025, 9, 23, 8, 0, tz=SP_TZ),
    schedule="15 5 * * *",
    catchup=False,
    tags=["Tabelas - OVH"],
) as dag:

    @task()
    def tb_price():
        main()

    tb_price()
