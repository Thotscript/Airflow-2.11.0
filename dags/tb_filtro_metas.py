import json
from datetime import datetime, timedelta
from collections import deque
import time
from airflow.models import Variable
import pandas as pd
import requests
from sqlalchemy import create_engine
from urllib.parse import quote_plus
from airflow import DAG
from airflow.decorators import task
import pendulum

# ---------------------------------------------------------
# CONFIG API — TOKENS INLINE
# ---------------------------------------------------------
API_URL = "https://web.streamlinevrs.com/api/json"
TOKEN_KEY = Variable.get("STREAMLINE_TOKEN_KEY")
TOKEN_SECRET = Variable.get("STREAMLINE_TOKEN_SECRET")

# ---------------------------------------------------------
# THROTTLE — sem libs externas (95 calls / 60s)
# ---------------------------------------------------------
WINDOW_SEC = 60
MAX_CALLS = 95
_call_times = deque()

def throttle():
    now = time.time()
    _call_times.append(now)

    # Remove chamadas fora da janela de 60s
    while _call_times and (now - _call_times[0]) > WINDOW_SEC:
        _call_times.popleft()

    # Se já batemos o limite, dorme até liberar
    if len(_call_times) >= MAX_CALLS:
        sleep_for = WINDOW_SEC - (now - _call_times[0]) + 0.05
        if sleep_for > 0:
            time.sleep(sleep_for)

# ---------------------------------------------------------
# POST COM RETRY/BACKOFF
# ---------------------------------------------------------
def post_with_retry(session, payload, retries=5, base_delay=1, timeout=60):
    delay = base_delay

    for attempt in range(retries):
        throttle()

        resp = session.post(
            API_URL,
            data=json.dumps(payload),
            headers={"Content-Type": "application/json"},
            timeout=timeout
        )

        # OK
        if resp.status_code == 200:
            return resp

        # Erros temporários -> tenta de novo
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

        # Erro definitivo (400, 401, etc.) -> devolve assim mesmo
        return resp

    # Se saiu do loop, devolve última resposta
    return resp

# ---------------------------------------------------------
# MAIN
# ---------------------------------------------------------
def main():
    # --- CONFIG BANCO (MySQL) ---
    DB_USER = "root"
    DB_PASS = "Tfl1234@"
    DB_HOST = "host.docker.internal"
    DB_PORT = 3306
    DB_NAME = "ovh_silver"

    engine = create_engine(
        f"mysql+pymysql://{DB_USER}:{quote_plus(DB_PASS)}@{DB_HOST}:{DB_PORT}/{DB_NAME}?charset=utf8mb4"
    )

    now_str = datetime.now().strftime('%Y/%m/%d %H:%M:%S')

    # ---------------------------------------------------------
    # SELECT — tokens NÃO vêm daqui. Somente ID + Administradora
    # ---------------------------------------------------------
    df_property = pd.read_sql(
        """
        SELECT id, Administradora 
        FROM tb_property_list_wordpress
        WHERE renting_type = 'RENTING'
        """,
        con=engine
    )

    frames = []
    fails = []
    ok = 0

    with requests.Session() as session:
        for row in df_property.itertuples(index=False):

            payload = {
                "methodName": "GetPropertyCustomFields",
                "params": {
                    # TOKENS INLINE
                    "token_key": TOKEN_KEY,
                    "token_secret": TOKEN_SECRET,
                    "unit_id": int(row.id)
                }
            }

            try:
                resp = post_with_retry(session, payload, retries=6, base_delay=1, timeout=60)
            except requests.RequestException as e:
                fails.append((row.id, row.Administradora, "request_exception", str(e)))
                continue

            status = resp.status_code

            if status != 200:
                fails.append((row.id, row.Administradora, status, resp.text[:200]))
                continue

            # Tenta converter para JSON
            try:
                data = resp.json()
            except ValueError as e:
                fails.append((row.id, row.Administradora, status, f"invalid_json: {e}"))
                continue

            # Garante estrutura esperada
            fields = (
                isinstance(data, dict)
                and data.get("data")
                and data["data"].get("field")
                and data["data"]["field"]
            )

            if not fields:
                fails.append((row.id, row.Administradora, status, "empty fields"))
                continue

            try:
                tb = pd.json_normalize(data["data"]["field"])
            except Exception as e:
                fails.append((row.id, row.Administradora, status, f"normalize_error: {e}"))
                continue

            # Colunas extras
            tb["id_unit"] = int(row.id)
            tb["Administradora"] = row.Administradora
            tb["data_price"] = now_str

            # Mesmo filtro do script original
            tb = tb.loc[tb["field_id"] == 19668]

            if not tb.empty:
                frames.append(tb)
                ok += 1

    # ---------------------------------------------------------
    # GRAVA NO BANCO (MySQL) — tabela original: tb_filtro_metas
    # ---------------------------------------------------------
    if frames:
        tb_filtro_metas = pd.concat(frames, ignore_index=True)

        # Aqui você escolhe se quer snapshot (replace) ou histórico (append).
        # Pelo que você comentou, a tabela original é única, então vou de REPLACE.
        tb_filtro_metas.to_sql("tb_filtro_metas", con=engine, if_exists="replace", index=False)

        print("Tabela tb_filtro_metas atualizada!")
    else:
        print("Nenhum dado válido retornado")

    engine.dispose()

    print(f"Sucesso: {ok} | Falhas: {len(fails)}")
    if fails:
        print("\nFalhas (primeiros 10):")
        for f in fails[:10]:
            print(f)

# ---------------------------------------------------------
# AIRFLOW DAG
# ---------------------------------------------------------
SP_TZ = pendulum.timezone("America/Sao_Paulo")

with DAG(
    dag_id="OVH-tb_filtro_metas",
    start_date=pendulum.datetime(2025, 9, 23, 8, 0, tz=SP_TZ),
    schedule="30 5 * * *",  # todos os dias 05:30
    catchup=False,
    tags=["Tabelas - OVH"],
):
    @task()
    def tb_custom_fields():
        main()

    tb_custom_fields()
