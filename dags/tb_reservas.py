import os
import json
import re
import numpy as np
import pandas as pd
import requests
from requests.adapters import HTTPAdapter, Retry
import mysql.connector
import pendulum
from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowException
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

API_URL = "https://web.streamlinevrs.com/api/json"
TOKEN_KEY = "a43cb1b5ed27cce283ab2bb4df540037"
TOKEN_SECRET = "72c7b8d5ba4b0ef14fe97a7e4179bdfa92cfc6ea"

DB_CFG = dict(
    host="host.docker.internal",
    user="root",
    password="Tfl1234@",
    database="ovh_bronze",
)

TB_NAME = "tb_reservas"
PAGE_SIZE = 5000

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
    resp = session.post(API_URL, json=payload, headers=headers, timeout=(10, 40))
    print("Resposta:", resp.status_code, resp.text[:500])

    try:
        resp.raise_for_status()
    except requests.HTTPError as e:
        raise AirflowException(f"HTTP error: {e} | body={resp.text[:500]}")

    try:
        data = resp.json()
    except ValueError:
        raise AirflowException(f"Resposta não-JSON: {resp.text[:500]}")

    if isinstance(data, dict) and data.get("error"):
        raise AirflowException(f"API retornou erro: {data.get('error')}")

    return data

def extract_reservations(payload: dict):
    if not isinstance(payload, dict):
        return []
    data = payload.get("data")
    if isinstance(data, dict) and "reservations" in data:
        return data.get("reservations") or []
    if "reservations" in payload:
        return payload.get("reservations") or []
    return []

# =========================
# Limpeza de colunas/valores
# =========================
def sanitize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.loc[:, df.columns.notna()]
    df.columns = [str(c) for c in df.columns]
    bad = {'nan', 'None', '', 'NaN', 'NAN'}
    keep = [c for c in df.columns if c not in bad]
    df = df[keep]

    cols = (pd.Series(df.columns)
            .str.strip()
            .str.replace(' ', '_', regex=False)
            .str.replace('.', '_', regex=False)
            .str.replace('-', '_', regex=False)
            .str.replace('(', '', regex=False)
            .str.replace(')', '', regex=False)
            .str.replace('[', '', regex=False)
            .str.replace(']', '', regex=False))

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
    df = df.replace({np.nan: None, 'nan': None, 'NaN': None, 'None': None, '': None})
    df = df.where(pd.notna(df), None)
    return df

# =========================
# Parser de datas robusto
# =========================
TZ_ABBR_TO_OFFSET = {
    "EST": "-05:00", "EDT": "-04:00",
    "CST": "-06:00", "CDT": "-05:00",
    "MST": "-07:00", "MDT": "-06:00",
    "PST": "-08:00", "PDT": "-07:00",
    "UTC": "+00:00", "GMT": "+00:00",
}

def _normalize_tz_to_offset(s: pd.Series) -> pd.Series:
    s = s.astype(str).str.strip().str.replace(r"\s+", " ", regex=True)
    def repl_tz(text: str) -> str:
        m = re.search(r"\b([A-Z]{2,4})$", text)
        if m:
            abbr = m.group(1)
            if abbr in TZ_ABBR_TO_OFFSET:
                return text[:m.start(1)] + TZ_ABBR_TO_OFFSET[abbr]
        return text
    return s.map(repl_tz)

def parse_dt_mixed(s: pd.Series, *, dayfirst: bool = True, col_name: str = "") -> pd.Series:
    s2 = _normalize_tz_to_offset(s)
    dt = pd.to_datetime(
        s2,
        format="mixed",
        dayfirst=dayfirst,
        utc=True,
        errors="coerce",
    )
    bad = s[dt.isna()]
    if not bad.empty:
        nome = col_name or getattr(s, "name", "unknown")
        print(f"[WARN] {len(bad)} valores de data não parseados em '{nome}'. Exemplos:", bad.head(5).tolist())
    return dt

# =========================
# Inferência de tipos MySQL
# =========================
PRICE_COLS = {"price_nightly", "price_total", "price_paidsum", "price_common", "price_balance"}

def _varchar_for_max_len(max_len: int) -> str:
    if max_len <= 255:
        return "VARCHAR(255)"
    if max_len <= 1024:
        return "VARCHAR(1024)"
    if max_len <= 4096:
        return "VARCHAR(4096)"
    return "MEDIUMTEXT"

def infer_mysql_type(col: str, s: pd.Series) -> str:
    # prioridade por nome conhecido
    if col in PRICE_COLS:
        return "DECIMAL(18,2)"

    # pandas dtype checks
    if pd.api.types.is_datetime64_any_dtype(s):
        return "DATETIME"  # guardaremos UTC sem tz

    if pd.api.types.is_bool_dtype(s):
        return "TINYINT(1)"

    if pd.api.types.is_integer_dtype(s):
        return "BIGINT"

    if pd.api.types.is_float_dtype(s):
        return "DOUBLE"

    # objetos/strings -> escolher VARCHAR pelo tamanho observado
    # (ignora None)
    lengths = s.dropna().astype(str).map(len)
    max_len = int(lengths.max()) if not lengths.empty else 0
    return _varchar_for_max_len(max_len)

def build_create_table_sql(table: str, df: pd.DataFrame) -> str:
    col_defs = []
    for c in df.columns:
        mysql_type = infer_mysql_type(c, df[c])
        col_defs.append(f"`{c}` {mysql_type} NULL")
    cols_sql = ", ".join(col_defs)
    return f"CREATE TABLE `{table}` ({cols_sql}) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci"

def coerce_for_mysql(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    # Datas: converter timezone-aware UTC -> naive (MySQL DATETIME não guarda TZ)
    for c in out.columns:
        if pd.api.types.is_datetime64_any_dtype(out[c]):
            # se for tz-aware (pandas >= 2.0 mantém dtype DatetimeTZDtype)
            try:
                out[c] = out[c].dt.tz_convert("UTC").dt.tz_localize(None)
            except Exception:
                # já deve estar naive; garantir sem nanos fora do range
                out[c] = pd.to_datetime(out[c], errors="coerce").dt.floor("s")

    # Bools -> int
    for c in out.columns:
        if pd.api.types.is_bool_dtype(out[c]):
            out[c] = out[c].astype("int8")

    # Inteiros muito grandes: BIGINT suporta ±9.22e18 (64-bit)
    # Floats mantidos como float (DOUBLE)
    # Strings: já ok

    return out

# =========================
# Persistência com SWAP tipado
# =========================
def create_and_swap_typed(df_full: pd.DataFrame, final_table: str):
    if df_full.empty:
        print("Nenhuma reserva retornada. Mantendo tabela final inalterada.")
        return

    tmp_table = f"{final_table}__tmp"
    backup_table = f"{final_table}__old"

    # Coerções finais para casar com os tipos MySQL
    df_sql = coerce_for_mysql(df_full)

    conn = mysql.connector.connect(**DB_CFG)
    try:
        cur = conn.cursor()

        # cria TMP com tipos inferidos
        cur.execute(f"DROP TABLE IF EXISTS `{tmp_table}`")
        create_sql = build_create_table_sql(tmp_table, df_sql)
        cur.execute(create_sql)

        cols_str = ", ".join([f"`{c}`" for c in df_sql.columns])
        placeholders = ", ".join(["%s"] * len(df_sql.columns))
        insert_sql = f"INSERT INTO `{tmp_table}` ({cols_str}) VALUES ({placeholders})"

        batch_size = 1000
        total = len(df_sql)
        total_inserted = 0
        for i in range(0, total, batch_size):
            batch = df_sql.iloc[i:i+batch_size]
            cur.executemany(insert_sql, [tuple(r) for r in batch.itertuples(index=False, name=None)])
            conn.commit()
            total_inserted += len(batch)
            print(f"[TMP] Inseridas {total_inserted}/{total} linhas")

        # swap atômico
        cur.execute(f"DROP TABLE IF EXISTS `{backup_table}`")
        cur.execute(
            "SELECT COUNT(*) FROM information_schema.tables "
            "WHERE table_schema = DATABASE() AND table_name = %s",
            (final_table,),
        )
        exists = cur.fetchone()[0]

        if exists:
            cur.execute(f"RENAME TABLE `{final_table}` TO `{backup_table}`, `{tmp_table}` TO `{final_table}`")
            conn.commit()
            cur.execute(f"DROP TABLE IF EXISTS `{backup_table}`")
        else:
            cur.execute(f"RENAME TABLE `{tmp_table}` TO `{final_table}`")
        conn.commit()

        print(f"Tabela `{final_table}` atualizada (tipos corretos) com {total} linhas.")
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
    session = build_session()
    page = 1
    dfs = []

    while True:
        payload = fetch_page(session, page)
        reservations = extract_reservations(payload)

        if not isinstance(payload, dict) or ("data" not in payload and "reservations" not in payload):
            keys = list(payload.keys()) if isinstance(payload, dict) else type(payload).__name__
            print(f"[WARN] Shape inesperado recebido. keys={keys}")

        if not reservations:
            print(f"Sem reservas na página {page}. Encerrando paginação.")
            break

        df = pd.json_normalize(reservations)
        dfs.append(df)
        print(f"Página {page}: {len(df)} reservas.")

        if len(reservations) < PAGE_SIZE:
            print("Última página identificada pela contagem < PAGE_SIZE.")
            break

        page += 1

    if not dfs:
        print("Nenhuma página com reservas. Nada a fazer.")
        return

    df_full = pd.concat(dfs, ignore_index=True)
    df_full = df_full.drop_duplicates(subset=['id'], keep='first')

    df_full['Administradora'] = "ONE VACATION HOME"

    df_full = sanitize_columns(df_full)
    df_full = clean_values(df_full)

    # Numéricos seguros
    for col in ["price_nightly", "price_total", "price_paidsum", "price_common", "price_balance"]:
        if col in df_full:
            df_full[col] = pd.to_numeric(df_full[col], errors="coerce")

    # Datas (UTC)
    if "creation_date" in df_full:
        df_full["creation_date"] = parse_dt_mixed(df_full["creation_date"], col_name="creation_date")
    if "startdate" in df_full:
        df_full["startdate"] = parse_dt_mixed(df_full["startdate"], col_name="startdate")
    if "enddate" in df_full:
        df_full["enddate"] = parse_dt_mixed(df_full["enddate"], col_name="enddate")
    if "last_updated" in df_full:
        df_full["last_updated"] = parse_dt_mixed(df_full["last_updated"], col_name="last_updated")

    print(f"Total de colunas: {len(df_full.columns)}")
    print(f"Total de linhas: {len(df_full)}")

    # Persistência tipada com swap seguro
    create_and_swap_typed(df_full, TB_NAME)

# === Airflow ===
SP_TZ = pendulum.timezone("America/Sao_Paulo")


with DAG(
    dag_id="OVH-tb_reservas",
    start_date=pendulum.datetime(2025, 9, 23, 8, 0, tz=SP_TZ),
    schedule="0 5 * * *",
    catchup=False,
    tags=["Tabelas - OVH"],
) as dag:

    @task(
        retries=4,
        retry_exponential_backoff=True,
    )
    def tb_reservas():
        # sua função atual
        main()

    trigger_check_reservas = TriggerDagRunOperator(
        task_id="trigger_OVH_Check_tb_reservas",
        trigger_dag_id="OVH-Check-tb_reservas",
        reset_dag_run=True,        # evita run duplicado com mesma logical_date
        wait_for_completion=False, # não bloqueia o produtor
        conf={"source": "OVH-tb_reservas"}  # opcional: envia contexto
    )

    tb_reservas() >> trigger_check_reservas
