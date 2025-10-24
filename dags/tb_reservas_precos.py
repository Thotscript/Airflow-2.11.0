import pandas as pd
import requests
import json
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
from ratelimit import limits, sleep_and_retry
import time

from sqlalchemy import create_engine
from urllib.parse import quote_plus

# ==============================
# CONFIG MYSQL
# ==============================
DB_USER = "root"
DB_PASS = "Tfl1234@"
DB_HOST = "host.docker.internal"
DB_PORT = 3306
DB_NAME = "ovh_silver"

engine = create_engine(
    f"mysql+pymysql://{DB_USER}:{quote_plus(DB_PASS)}@{DB_HOST}:{DB_PORT}/{DB_NAME}?charset=utf8mb4"
)

# ==============================
# TOKENS FIXOS (tenant único)
# ==============================
TOKEN_KEY = "a43cb1b5ed27cce283ab2bb4df540037"
TOKEN_SECRET = "72c7b8d5ba4b0ef14fe97a7e4179bdfa92cfc6ea"

# ==============================
# CARREGA DF DE ORIGEM (APENAS confirmation_id, Administradora)
# da tabela tb_reservas
# ==============================
df = pd.read_sql(
    """
    SELECT
        confirmation_id,
        Administradora
    FROM tb_reservas
    WHERE confirmation_id IS NOT NULL
    """,
    con=engine
)
# Opcional: limitar volume
# df = df.head(20000)

# ==============================
# API
# ==============================
url = "https://web.streamlinevrs.com/api/json"
headers = {"Content-Type": "application/json"}

# Datas auxiliares (iguais ao seu script)
today = datetime.now().strftime('%Y/%m/%d')
now = datetime.now().strftime('%Y/%m/%d %H:%M:%S')
end_date = (datetime.now() + timedelta(days=547)).strftime('%Y/%m/%d')

# ==============================
# Função chamada em paralelo
# Parâmetros agora: (confirmation_id, admin)
# ==============================
@sleep_and_retry
@limits(calls=100, period=60)  # 100 req/min
def get_unit_data(confirmation_id, admin):
    # payload LOCAL (evita data races)
    payload = {
        "methodName": "GetReservationPrice",
        "params": {
            "token_key": TOKEN_KEY,             # tokens fixos
            "token_secret": TOKEN_SECRET,
            "confirmation_id": str(confirmation_id),
            "return_payments": 1
        }
    }

    response = requests.post(url, data=json.dumps(payload), headers=headers, timeout=60)
    if response.status_code != 200:
        print(f"[warn] HTTP {response.status_code} - confirmation_id={confirmation_id} admin={admin} body={response.text[:200]}")
        return None

    try:
        data_dict = response.json()
    except ValueError:
        print(f"[warn] JSON inválido - confirmation_id={confirmation_id} admin={admin} body={response.text[:200]}")
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

    # transforma a lista de dicionários em um DataFrame
    df_reservation_days = pd.DataFrame(reservation_days)

    # adiciona colunas auxiliares (MESMOS nomes do seu código)
    df_reservation_days["reservation_id"] = reservation_id
    df_reservation_days["data_price"] = now
    df_reservation_days["Administradora"] = admin

    return df_reservation_days

# ==============================
# Monta params_list (agora só com confirmation_id e admin)
# ==============================
params_list = [
    (row.confirmation_id, row.Administradora)
    for row in df.itertuples(index=False)
]

start_time = time.time()

# ==============================
# Execução em paralelo
# ==============================
with ThreadPoolExecutor(max_workers=4) as executor:
    result_iter = executor.map(lambda x: get_unit_data(*x), params_list)
    time.sleep(0.6)

# Materializa resultados e filtra Nones
results = [r for r in result_iter if r is not None]

if not results:
    print("Nenhum resultado retornado (sem linhas para gravar).")
    tb_price_reservas = pd.DataFrame()
else:
    tb_price_reservas = pd.concat(results, ignore_index=True)

# Pré-visualização
print(tb_price_reservas.head(10))

# ==============================
# Persistência no MySQL (append)
# ==============================
if not tb_price_reservas.empty:
    table_name = "tb_reservas_price_day"
    tb_price_reservas.to_sql(table_name, engine, if_exists="append", index=False)
    print("Dados Gravados Com Sucesso")
else:
    print("Nada para gravar (DataFrame vazio).")

engine.dispose()

end_time = time.time()
execution_time = end_time - start_time
print(f"Tempo de execução: {execution_time:.2f} segundos")
