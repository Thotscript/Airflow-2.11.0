import os
import json
import numpy as np
import pandas as pd
import requests
from requests.adapters import HTTPAdapter, Retry
import mysql.connector
import pendulum
from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowException

API_URL = "https://web.streamlinevrs.com/api/json"
TOKEN_KEY = "a43cb1b5ed27cce283ab2bb4df540037"
TOKEN_SECRET = "72c7b8d5ba4b0ef14fe97a7e4179bdfa92cfc6ea"

DB_CFG = dict(
    host="host.docker.internal",
    user="root",
    password="Tfl1234@",
    database="ovh_silver",
)

TB_NAME = "tb_reservas"
PAGE_SIZE = 5000

def build_session():
    s = requests.Session()
    retries = Retry(
        total=4,
        backoff_factor=0.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("POST",),
        raise_on_status=False,
    )
    s.mount("https://", HTTPAdapter(max_retries=retries))
    return s

def make_payload(page_number: int):
    return {
        "methodName": "GetReservations",
        "params": {
            "token_key": TOKEN_KEY,
            "token_secret": TOKEN_SECRET,
            "return_full": True,
            # "status_ids": "4,5,7",
            "page_results_number": PAGE_SIZE,
            "page_number": page_number,
        },
    }

def fetch_page(session: requests.Session, page_number: int) -> dict:
    payload = make_payload(page_number)
    headers = {"Content-Type": "application/json"}
    resp = session.post(API_URL, json=payload, headers=headers, timeout=60)
    # Log uma amostra da resposta
    print("Resposta:", resp.status_code, resp.text[:500])

    try:
        resp.raise_for_status()
    except requests.HTTPError as e:
        raise AirflowException(f"HTTP error: {e} | body={resp.text[:500]}")

    try:
        data = resp.json()
    except ValueError:
        raise AirflowException(f"Resposta não-JSON: {resp.text[:500]}")

    # Se a API encapsular erro em 200
    if isinstance(data, dict) and data.get("error"):
        raise AirflowException(f"API retornou erro: {data.get('error')}")

    return data

def extract_reservations(payload: dict):
    if not isinstance(payload, dict):
        return []
    data = payload.get("data")
    if isinstance(data, dict) and "reservations" in data:
        return data.get("reservations") or []
    # fallback (caso venha direto na raiz)
    if "reservations" in payload:
        return payload.get("reservations") or []
    return []

def sanitize_columns(df: pd.DataFrame) -> pd.DataFrame:
    # remove colunas com nome inválido
    df = df.loc[:, df.columns.notna()]
    df.columns = [str(c) for c in df.columns]
    bad = {'nan', 'None', '', 'NaN', 'NAN'}
    keep = [c for c in df.columns if c not in bad]
    df = df[keep]

    # normaliza nomes
    cols = (pd.Series(df.columns)
            .str.strip()
            .str.replace(' ', '_', regex=False)
            .str.replace('.', '_', regex=False)
            .str.replace('-', '_', regex=False)
            .str.replace('(', '', regex=False)
            .str.replace(')', '', regex=False)
            .str.replace('[', '', regex=False)
            .str.replace(']', '', regex=False))

    # limita a 64 chars (limite padrão do MySQL) mantendo unicidade
    def truncate_unique(names):
        seen = {}
        out = []
        for name in names:
            base = name[:64]
            if base not in seen:
                seen[base] = 0
                out.append(base)
            else:
                seen[base] += 1
                suffix = f"_{seen[base]}"
                trimmed = base[:64-len(suffix)] + suffix
                out.append(trimmed)
        return out

    df.columns = truncate_unique(cols)
    return df

def clean_values(df: pd.DataFrame) -> pd.DataFrame:
    # Padroniza valores nulos
    df = df.replace({np.nan: None, 'nan': None, 'NaN': None, 'None': None, '': None})
    df = df.where(pd.notna(df), None)
    return df

def upsert_full_table(df_full: pd.DataFrame):
    if df_full.empty:
        print("Nenhuma reserva retornada. Tabela não foi recriada (evitando apagar dados anteriores).")
        return

    conn = mysql.connector.connect(**DB_CFG)
    try:
        cur = conn.cursor()

        # recria sempre (se esse é o comportamento desejado)
        cur.execute(f"DROP TABLE IF EXISTS `{TB_NAME}`")

        cols_def = ", ".join([f"`{c}` TEXT" for c in df_full.columns])
        cur.execute(f"CREATE TABLE `{TB_NAME}` ({cols_def})")

        cols_str = ", ".join([f"`{c}`" for c in df_full.columns])
        placeholders = ", ".join(["%s"] * len(df_full.columns))
        insert_sql = f"INSERT INTO `{TB_NAME}` ({cols_str}) VALUES ({placeholders})"

        batch_size = 1000
        total = len(df_full)
        total_inserted = 0
        for i in range(0, total, batch_size):
            batch = df_full.iloc[i:i+batch_size]
            cur.executemany(insert_sql, batch.values.tolist())
            conn.commit()
            total_inserted += len(batch)
            print(f"Inseridas {total_inserted}/{total} linhas")

        print(f"Tabela `{TB_NAME}` recriada e {total} linhas inseridas com sucesso!")
    finally:
        try:
            cur.close()
        except Exception:
            pass
        conn.close()

def main():
    session = build_session()
    page = 1
    dfs = []

    while True:
        payload = fetch_page(session, page)
        reservations = extract_reservations(payload)

        # Diagnóstico do shape inesperado
        if not isinstance(payload, dict) or (
            "data" not in payload and "reservations" not in payload
        ):
            keys = list(payload.keys()) if isinstance(payload, dict) else type(payload).__name__
            print(f"[WARN] Shape inesperado recebido. keys={keys}")

        # fim da paginação quando vazio
        if not reservations:
            print(f"Sem reservas na página {page}. Encerrando paginação.")
            break

        df = pd.json_normalize(reservations)
        dfs.append(df)
        print(f"Página {page}: {len(df)} reservas.")

        # heurística simples: se voltou menos que o PAGE_SIZE, provavelmente acabou
        if len(reservations) < PAGE_SIZE:
            print("Última página identificada pela contagem < PAGE_SIZE.")
            break

        page += 1

    if not dfs:
        print("Nenhuma página com reservas. Nada a fazer.")
        return

    df_full = pd.concat(dfs, ignore_index=True)
    df_full = df_full.drop_duplicates(subset=['id'], keep='first')

    # coluna extra
    df_full['Administradora'] = "ONE VACATION HOME"

    # limpeza
    df_full = sanitize_columns(df_full)
    df_full = clean_values(df_full)

    print(f"Total de colunas: {len(df_full.columns)}")
    print(f"Total de linhas: {len(df_full)}")

    # persiste no MySQL
    upsert_full_table(df_full)

# === Airflow ===
SP_TZ = pendulum.timezone("America/Sao_Paulo")

with DAG(
    dag_id="OVH-tb_reservas",
    start_date=pendulum.datetime(2025, 9, 23, 8, 0, tz=SP_TZ),
    schedule="0 5 * * *",
    catchup=False,
    tags=["Tabelas - OVH"],
) as dag:

    @task(retries=3)
    def tb_reservas():
        main()

    tb_reservas()
