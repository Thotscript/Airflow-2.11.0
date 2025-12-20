import os
import json
import re
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import requests
from requests.adapters import HTTPAdapter, Retry
import mysql.connector

# =========================
# Configurações
# =========================
API_URL = "https://web.streamlinevrs.com/api/json"
TOKEN_KEY = "3ef223d3bbf7086cfb86df7e98d6e5d2"
TOKEN_SECRET = "a88d05b895affb815cc8a4d96670698ee486ea30"

DB_CFG = dict(
    host="host.docker.internal",
    user="root",
    password="Tfl1234@",
    database="ovh_bronze",
)

TB_NAME = "tb_ocupacao"

# =========================
# HTTP resiliente
# =========================
def build_session():
    s = requests.Session()
    retries = Retry(
        total=6,
        connect=6,
        read=6,
        backoff_factor=0.8,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("POST",),
        respect_retry_after_header=True,
        raise_on_status=False,
    )
    s.mount("https://", HTTPAdapter(max_retries=retries))
    return s


def make_api_call(session: requests.Session, method_name: str, params: dict) -> dict:
    payload = {
        "methodName": method_name,
        "params": params
    }
    headers = {"Content-Type": "application/json"}

    resp = session.post(API_URL, json=payload, headers=headers, timeout=(10, 40))
    print(f"[{method_name}] Status: {resp.status_code}")

    resp.raise_for_status()

    try:
        data = resp.json()
    except ValueError:
        raise RuntimeError(f"Resposta não-JSON: {resp.text[:500]}")

    if isinstance(data, dict) and data.get("error"):
        raise RuntimeError(f"API retornou erro: {data.get('error')}")

    return data


# =========================
# API Streamline
# =========================
def get_property_list(session: requests.Session) -> list:
    params = {
        "token_key": TOKEN_KEY,
        "token_secret": TOKEN_SECRET,
        "status_id": 1,
        "return_owner_id": True
    }

    print("Buscando lista de propriedades...")
    response = make_api_call(session, "GetPropertyList", params)

    return response.get("data", {}).get("properties", [])


def check_availability(session, unit_id, start_date, end_date):
    params = {
        "token_key": TOKEN_KEY,
        "token_secret": TOKEN_SECRET,
        "unit_id": unit_id,
        "startdate": start_date,
        "enddate": end_date,
        "occupants": 1,
        "disable_hk": 1
    }

    try:
        response = make_api_call(session, "GetPropertyAvailability", params)
        props = response.get("data", {}).get("properties", [])
        is_available = len(props) > 0

        return is_available, not is_available

    except Exception as e:
        print(f"Erro unit_id={unit_id}: {e}")
        return None, None


def generate_date_ranges(days=30):
    today = datetime.now()
    return [(today + timedelta(days=i)).strftime("%m/%d/%Y") for i in range(days)]


# =========================
# Limpeza de dados
# =========================
def sanitize_columns(df):
    df = df.loc[:, df.columns.notna()]
    df.columns = [str(c) for c in df.columns]

    df.columns = (
        pd.Series(df.columns)
        .str.strip()
        .str.replace(" ", "_", regex=False)
        .str.replace(".", "_", regex=False)
        .str.replace("-", "_", regex=False)
        .str.replace("(", "", regex=False)
        .str.replace(")", "", regex=False)
        .str.replace("[", "", regex=False)
        .str.replace("]", "", regex=False)
    )

    df.columns = df.columns.str[:64]
    return df


def clean_values(df):
    return df.replace({np.nan: None, "nan": None, "None": None, "": None})


# =========================
# MySQL helpers
# =========================
def infer_mysql_type(series):
    if pd.api.types.is_integer_dtype(series):
        return "BIGINT"
    if pd.api.types.is_float_dtype(series):
        return "DOUBLE"
    if pd.api.types.is_bool_dtype(series):
        return "TINYINT(1)"

    max_len = series.astype(str).map(len).max()
    return "VARCHAR(255)" if max_len <= 255 else "TEXT"


def build_create_table_sql(table, df):
    cols = [
        f"`{c}` {infer_mysql_type(df[c])} NULL"
        for c in df.columns
    ]
    return f"""
    CREATE TABLE `{table}` (
        {", ".join(cols)}
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """


def create_and_swap_typed(df, table):
    if df.empty:
        print("Nenhum dado para inserir.")
        return

    tmp = f"{table}__tmp"
    old = f"{table}__old"

    conn = mysql.connector.connect(**DB_CFG)
    cur = conn.cursor()

    try:
        cur.execute(f"DROP TABLE IF EXISTS `{tmp}`")
        cur.execute(build_create_table_sql(tmp, df))

        cols = ", ".join(f"`{c}`" for c in df.columns)
        placeholders = ", ".join(["%s"] * len(df.columns))

        sql = f"INSERT INTO `{tmp}` ({cols}) VALUES ({placeholders})"

        cur.executemany(sql, df.itertuples(index=False, name=None))
        conn.commit()

        cur.execute(f"DROP TABLE IF EXISTS `{old}`")
        cur.execute(f"RENAME TABLE `{table}` TO `{old}`, `{tmp}` TO `{table}`")
        cur.execute(f"DROP TABLE IF EXISTS `{old}`")
        conn.commit()

        print(f"Tabela `{table}` atualizada ({len(df)} registros).")

    finally:
        cur.close()
        conn.close()


# =========================
# Pipeline principal
# =========================
def main():
    session = build_session()
    properties = get_property_list(session)

    dates = generate_date_ranges(30)
    rows = []

    for prop in properties:
        unit_id = prop.get("id")
        name = prop.get("name", "")

        for date in dates:
            is_available, is_occupied = check_availability(
                session, unit_id, date, date
            )

            rows.append({
                "unit_id": unit_id,
                "property_name": name,
                "check_date": date,
                "is_available": is_available,
                "is_occupied": is_occupied,
                "checked_at": datetime.now()
            })

    df = pd.DataFrame(rows)
    df["check_date"] = pd.to_datetime(df["check_date"], format="%m/%d/%Y").dt.date

    df = sanitize_columns(df)
    df = clean_values(df)

    print(f"Registros: {len(df)}")
    create_and_swap_typed(df, TB_NAME)


# =========================
# Execução
# =========================
if __name__ == "__main__":
    main()
