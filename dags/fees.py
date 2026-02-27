import time
import json
from collections import deque

import numpy as np
import pandas as pd
import requests
from requests.adapters import HTTPAdapter, Retry
import mysql.connector
import pendulum
from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable

# =========================
# Config
# =========================
API_URL = "https://web.streamlinevrs.com/api/json"
TOKEN_KEY    = Variable.get("STREAMLINE_TOKEN_KEY")
TOKEN_SECRET = Variable.get("STREAMLINE_TOKEN_SECRET")

DB_CFG = dict(
    host="host.docker.internal",
    user="root",
    password="Tfl1234@",
    database="ovh_bronze",
)

TB_SOURCE = "tb_reservas"
TB_TARGET = "tb_tax_and_fees"

# =========================
# Rate limit (igual ao original)
# =========================
REQUESTS_PER_MINUTE_BUDGET = 50
MIN_SLEEP_BETWEEN_REQUESTS = 0.30
_window = deque()

def _respect_window():
    now = time.time()
    while _window and (now - _window[0]) >= 60:
        _window.popleft()
    if len(_window) >= REQUESTS_PER_MINUTE_BUDGET:
        sleep_for = 60 - (now - _window[0]) + 0.05
        if sleep_for > 0:
            time.sleep(sleep_for)
    now = time.time()
    while _window and (now - _window[0]) >= 60:
        _window.popleft()

def rate_limited_post(*args, **kwargs):
    _respect_window()
    if _window:
        elapsed_since_last = time.time() - _window[-1]
        if elapsed_since_last < MIN_SLEEP_BETWEEN_REQUESTS:
            time.sleep(MIN_SLEEP_BETWEEN_REQUESTS - elapsed_since_last)
    resp = requests.post(*args, **kwargs, timeout=(10, 180))
    _window.append(time.time())
    return resp

def call_with_backoff(confirmation_id: int) -> dict:
    payload = {
        "methodName": "GetReservationInfo",
        "params": {
            "token_key": TOKEN_KEY,
            "token_secret": TOKEN_SECRET,
            "return_full": True,
            "show_taxes_and_fees": 1,
            "show_commission_information": 1,
            "confirmation_id": confirmation_id,
        },
    }
    headers = {
        "Content-Type": "application/json",
        "User-Agent": "streamline-client/1.1",
    }
    while True:
        response = rate_limited_post(API_URL, data=json.dumps(payload), headers=headers)
        try:
            data_dict = response.json()
        except Exception:
            time.sleep(2)
            continue

        status = data_dict.get("status", {})
        code   = status.get("code")

        if code in (None, "0", 0, "SUCCESS"):
            return data_dict

        if str(code) == "E0013" or "allowed requests per minute" in str(status).lower():
            print(f"  [RATE LIMIT] confirmation_id={confirmation_id} — aguardando 65s...")
            time.sleep(65)
            continue

        print(f"  [WARN] confirmation_id={confirmation_id} — code={code} status={status}")
        time.sleep(2)
        return data_dict

# =========================
# Fetch confirmation_ids do banco
# =========================
def fetch_confirmation_ids() -> list[int]:
    conn = mysql.connector.connect(**DB_CFG)
    try:
        cur = conn.cursor()
        cur.execute(f"SELECT DISTINCT confirmation_id FROM `{TB_SOURCE}` WHERE confirmation_id IS NOT NULL")
        ids = [row[0] for row in cur.fetchall()]
        print(f"{len(ids)} confirmation_ids distintos encontrados em {TB_SOURCE}.")
        return ids
    finally:
        cur.close()
        conn.close()

# =========================
# Parser do JSON de retorno
# =========================
def parse_response(confirmation_id: int, data_dict: dict) -> list[dict]:
    rows = []
    data = data_dict.get("data", {})

    commission = data.get("commission_information", {})
    comm_fields = {
        "management_commission_amount":  commission.get("management_commission_amount"),
        "management_commission_percent": commission.get("management_commission_percent"),
        "owner_commission_amount":       commission.get("owner_commission_amount"),
        "owner_commission_percent":      commission.get("owner_commission_percent"),
    }

    taxes_and_fees = data.get("taxes_and_fees", {})
    for tf in taxes_and_fees.get("tax_fee", []):
        rows.append({
            "confirmation_id": confirmation_id,
            "row_type":        "tax_fee",
            "fee_id":          tf.get("id"),
            "name":            tf.get("name"),
            "value":           tf.get("value"),
            "percent":         tf.get("percent"),
            "include_as_tax":  tf.get("include_as_tax"),
            "taxable":         tf.get("taxable"),
            **comm_fields,
        })

    if commission:
        rows.append({
            "confirmation_id": confirmation_id,
            "row_type":        "commission",
            "fee_id":          None,
            "name":            "Commission Summary",
            "value":           None,
            "percent":         None,
            "include_as_tax":  None,
            "taxable":         None,
            **comm_fields,
        })

    return rows

# =========================
# Persistência com swap (padrão do projeto)
# =========================
CREATE_SQL = """
CREATE TABLE `{table}` (
  `confirmation_id`                 INT            NULL,
  `row_type`                        VARCHAR(20)    NULL,
  `fee_id`                          BIGINT         NULL,
  `name`                            VARCHAR(255)   NULL,
  `value`                           DECIMAL(18,2)  NULL,
  `percent`                         DECIMAL(10,4)  NULL,
  `include_as_tax`                  TINYINT(1)     NULL,
  `taxable`                         TINYINT(1)     NULL,
  `management_commission_amount`    DECIMAL(18,2)  NULL,
  `management_commission_percent`   DECIMAL(10,4)  NULL,
  `owner_commission_amount`         DECIMAL(18,2)  NULL,
  `owner_commission_percent`        DECIMAL(10,4)  NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
"""

def create_and_swap(df: pd.DataFrame, final_table: str):
    if df.empty:
        print("DataFrame vazio — tabela final não alterada.")
        return

    tmp_table    = f"{final_table}__tmp"
    backup_table = f"{final_table}__old"

    for col in ["value", "percent", "management_commission_amount",
                "management_commission_percent", "owner_commission_amount",
                "owner_commission_percent"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    for col in ["include_as_tax", "taxable"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int8")

    df = df.replace({np.nan: None, float("nan"): None})
    df = df.where(pd.notna(df), None)

    conn = mysql.connector.connect(**DB_CFG)
    try:
        cur = conn.cursor()

        cur.execute(f"DROP TABLE IF EXISTS `{tmp_table}`")
        cur.execute(CREATE_SQL.format(table=tmp_table))
        conn.commit()

        cols_str     = ", ".join([f"`{c}`" for c in df.columns])
        placeholders = ", ".join(["%s"] * len(df.columns))
        insert_sql   = f"INSERT INTO `{tmp_table}` ({cols_str}) VALUES ({placeholders})"

        batch_size     = 1000
        total          = len(df)
        total_inserted = 0
        for i in range(0, total, batch_size):
            batch = df.iloc[i:i+batch_size]
            cur.executemany(insert_sql, [
                tuple(
                    None if (v is None or (isinstance(v, float) and np.isnan(v))) else v
                    for v in r
                )
                for r in batch.itertuples(index=False, name=None)
            ])
            conn.commit()
            total_inserted += len(batch)
            print(f"  [DB] {total_inserted}/{total} linhas inseridas em {tmp_table}")

        cur.execute(f"DROP TABLE IF EXISTS `{backup_table}`")
        cur.execute(
            "SELECT COUNT(*) FROM information_schema.tables "
            "WHERE table_schema = DATABASE() AND table_name = %s",
            (final_table,),
        )
        exists = cur.fetchone()[0]

        if exists:
            cur.execute(
                f"RENAME TABLE `{final_table}` TO `{backup_table}`, `{tmp_table}` TO `{final_table}`"
            )
            conn.commit()
            cur.execute(f"DROP TABLE IF EXISTS `{backup_table}`")
        else:
            cur.execute(f"RENAME TABLE `{tmp_table}` TO `{final_table}`")
        conn.commit()

        print(f"Tabela `{final_table}` atualizada com {total} linhas ({df['confirmation_id'].nunique()} reservas).")
    finally:
        try:
            cur.close()
        except Exception:
            pass
        conn.close()

# =========================
# Pipeline principal
# =========================
def main():
    confirmation_ids = fetch_confirmation_ids()

    all_rows = []
    total    = len(confirmation_ids)

    for i, cid in enumerate(confirmation_ids, 1):
        print(f"[{i}/{total}] Buscando confirmation_id={cid}...")
        try:
            data_dict = call_with_backoff(cid)
            rows = parse_response(cid, data_dict)
            if rows:
                all_rows.extend(rows)
            else:
                print(f"  [SKIP] Sem dados para confirmation_id={cid}")
        except Exception as e:
            print(f"  [ERROR] confirmation_id={cid} — {e}")
            continue

    if not all_rows:
        print("Nenhum dado coletado.")
        return

    df = pd.DataFrame(all_rows)
    print(f"\nTotal de linhas coletadas: {len(df)} | Reservas únicas: {df['confirmation_id'].nunique()}")
    create_and_swap(df, TB_TARGET)

# =========================
# Airflow DAG
# =========================
SP_TZ = pendulum.timezone("America/Sao_Paulo")

with DAG(
    dag_id="OVH-tb_tax_and_fees",
    start_date=pendulum.datetime(2025, 9, 23, 8, 0, tz=SP_TZ),
    schedule="0 1 * * *",       # roda 1h após tb_reservas (00:00) para garantir dados atualizados
    catchup=False,
    tags=["Tabelas - OVH"],
) as dag:

    @task(retries=4, retry_exponential_backoff=True)
    def tb_tax_and_fees():
        main()

    tb_tax_and_fees()