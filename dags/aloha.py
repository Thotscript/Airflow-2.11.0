# -*- coding: utf-8 -*-
"""
ALOHA PIPELINE (SINGLE FILE) — Airflow-ready (Playwright only, no Selenium)
- Lê Aloha (atual) e Aloha_last (snapshot) de Google Sheets (aba: "Página1")
- Sobrescreve Aloha_last com o conteúdo anterior de Aloha (snapshot)
- Consome Streamline (GetHousekeepingCleaningReport) com HTTP resiliente (Retry)
- Processa flags, enriquece com MySQL, monta final_df
- Gera Excel local /tmp/Aloha.xlsx e /tmp/Aloha_upload.xlsx (para upload no Aloha via Playwright)
- Compara atual vs anterior e:
    - atualiza datas (Next Arrival / Cleaning Date) no Aloha Smart Services
    - remove reservas canceladas
- Faz upload para ONE VACATION e SNOW BIRD
- Envia e-mail via Gmail API
- Inclui DAG do Airflow no final

IMPORTANTE (Airflow):
- SEM login interativo OAuth. Tokens precisam existir no disco:
  - Sheets token:  /opt/airflow/dags/Tokens/sheets_token.json
  - Gmail token:   /opt/airflow/dags/Tokens/token.json
  - Credentials:   /opt/airflow/dags/Tokens/credentials.json (cliente OAuth)
- Se der "invalid_scope": token foi gerado com scopes diferentes. Gere novamente fora do Airflow.
"""

# =============================
# IMPORTS
# =============================
import os
import re
import base64
import time
from datetime import datetime, timedelta, date
from typing import List, Tuple, Optional

import numpy as np
import pandas as pd
import requests
from requests.adapters import HTTPAdapter, Retry

from sqlalchemy import create_engine
from sqlalchemy.engine import URL

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError

from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication

from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from google.auth.exceptions import RefreshError

import pendulum
from airflow import DAG
from airflow.decorators import task


# ==========================
# CONFIGURAÇÕES
# ==========================
SP_TZ = pendulum.timezone("America/Sao_Paulo")

# --- Streamline ---
STREAMLINE_URL = "https://web.streamlinevrs.com/api/json"
STREAMLINE_TOKEN_KEY = "3ef223d3bbf7086cfb86df7e98d6e5d2"
STREAMLINE_TOKEN_SECRET = "a88d05b895affb815cc8a4d96670698ee486ea30"
STREAMLINE_HEADERS = {"Content-Type": "application/json"}

REPORT_START_DAYS_BACK = 5
REPORT_END_DAYS_FORWARD = 85

# --- Google OAuth ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))  # /opt/airflow/dags
TOKENS_DIR = os.path.join(BASE_DIR, "Tokens")
CREDENTIALS_FILE = os.path.join(TOKENS_DIR, "credentials.json")  # mantido (útil p/ re-gerar token fora do Airflow)

# Sheets (MESMO padrão do WorkOrders)
SHEETS_SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]
SHEETS_TOKEN_FILE = os.path.join(TOKENS_DIR, "sheets_token.json")

# Gmail (MESMO token que já funciona)
GMAIL_SCOPES = [
    "https://www.googleapis.com/auth/gmail.modify",
    "https://www.googleapis.com/auth/gmail.send",
]
GMAIL_TOKEN_FILE = os.path.join(TOKENS_DIR, "token.json")
GMAIL_USER_ID = "me"

# --- Sheets IDs ---
SHEET_ID_ALOHA_ATUAL = "1pUpX9_zCQSYys8SUN7f52TK857ocJrphKVfbgtka5k8"
SHEET_ID_ALOHA_LAST = "1G6e4g3ZG4bH66kCqjihmX2yTX-kkojxunFffKRAEKgY"
SHEET_TAB_NAME = "Página1"

# --- Aloha ---
ALOHA_LOGIN_URL = "https://www.alohasmartservices.com/users/login"
ALOHA_ORDERS_URL = "https://www.alohasmartservices.com/orders"
ALOHA_EMAIL = "mkj2508@gmail.com"
ALOHA_SENHA = "250909"

# --- Excel ---
EXCEL_FULL_LOCAL = "/tmp/Aloha.xlsx"
EXCEL_UPLOAD_LOCAL = "/tmp/Aloha_upload.xlsx"

CLIENT_ONE = "ONE VACATION"
CLIENT_SNOW = "SNOW BIRD"

# --- MySQL ---
MYSQL_HOST = "host.docker.internal"
MYSQL_USER = "root"
MYSQL_PASSWORD = "Tfl1234@"
MYSQL_DATABASE = "ovh_silver"

# --- Service name rules ---
SERVICE_NAME_DEFAULT = "CHECK OUT - EXTERNAL LAUNDRY"
UNITS_NO_LAUNDRY = [
    "9312 SH - Champions Gate - Unique High end 8 Beds (8br/5ba)",
    "9332 SH - Champions Gate Unique Luxury 8BR (8br/5ba)",
]
SERVICE_NAME_NO_LAUNDRY = "CHECK OUT - NO LAUNDRY"

# --- Email ---
EMAIL_SENDER = "revenue@onevacationhome.com"
EMAIL_RECIPIENTS = ["revenue@onevacationhome.com", "keitianne@onevacationhome.com"]
EMAIL_SUBJECT = "Upload Aloha"

# --- DAG ---
DAG_ID = "ALOHA_PIPELINE"
DAG_SCHEDULE = "0 7 * * *"
DAG_START_DATE = pendulum.datetime(2025, 9, 23, 8, 0, tz=SP_TZ)


# ==========================
# GOOGLE AUTH (SEM LOGIN INTERATIVO)
# ==========================
def load_credentials(token_file: str, scopes: List[str]) -> Credentials:
    if not os.path.exists(token_file):
        raise RuntimeError(f"Token não encontrado: {token_file}")

    creds = Credentials.from_authorized_user_file(token_file, scopes)

    # Se expirado, tenta refresh
    if creds and creds.expired and creds.refresh_token:
        try:
            creds.refresh(Request())
            with open(token_file, "w") as f:
                f.write(creds.to_json())
        except RefreshError as e:
            raise RuntimeError(
                f"Falha ao dar refresh no token {token_file}. "
                f"Provável scopes divergentes do token. Erro: {repr(e)}"
            )

    if not creds or not creds.valid:
        raise RuntimeError(
            f"Credenciais inválidas para {token_file}. "
            "Recrie o token fora do Airflow com os scopes corretos."
        )

    return creds


def get_sheets_service():
    return build("sheets", "v4", credentials=load_credentials(SHEETS_TOKEN_FILE, SHEETS_SCOPES))


def get_gmail_service():
    return build("gmail", "v1", credentials=load_credentials(GMAIL_TOKEN_FILE, GMAIL_SCOPES))


# =========================
# HTTP resiliente (Retry)
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


# =========================
# GOOGLE SHEETS: READ / WRITE
# =========================
def read_sheet_as_df(sheet_id: str, tab_name: str) -> pd.DataFrame:
    service = get_sheets_service()
    resp = service.spreadsheets().values().get(
        spreadsheetId=sheet_id,
        range=tab_name,
        valueRenderOption="UNFORMATTED_VALUE",
    ).execute()

    values = resp.get("values", [])
    if not values:
        return pd.DataFrame()

    raw_header = values[0]
    rows = values[1:]

    # Header "limpo": garante string, remove None, e evita header vazio
    header = []
    for i, h in enumerate(raw_header):
        h = "" if h is None else str(h).strip()
        if not h:
            h = f"__col_{i+1}"
        header.append(h)

    # Se o header tiver duplicados, renomeia com sufixo
    seen = {}
    fixed_header = []
    for h in header:
        if h not in seen:
            seen[h] = 1
            fixed_header.append(h)
        else:
            seen[h] += 1
            fixed_header.append(f"{h}_{seen[h]}")
    header = fixed_header

    ncols = len(header)

    # Normaliza cada linha para ter exatamente ncols colunas
    norm_rows = []
    for r in rows:
        if r is None:
            r = []
        # garante lista
        if not isinstance(r, list):
            r = [r]

        if len(r) < ncols:
            r = r + [""] * (ncols - len(r))
        elif len(r) > ncols:
            r = r[:ncols]

        norm_rows.append(r)

    df = pd.DataFrame(norm_rows, columns=header)

    # Converte colunas de data (quando existirem)
    date_cols = ["Cleaning Date", "Check-In Date", "Check-Out Date", "Next Arrival"]
    for c in date_cols:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], errors="coerce", dayfirst=True)

    return df



def write_df_to_sheet(df: pd.DataFrame, sheet_id: str, tab_name: str):
    service = get_sheets_service()

    df_out = df.copy()
    for c in ["Cleaning Date", "Check-In Date", "Check-Out Date", "Next Arrival"]:
        if c in df_out.columns:
            df_out[c] = pd.to_datetime(df_out[c], errors="coerce").dt.strftime("%d/%m/%Y")

    values = [df_out.columns.tolist()] + df_out.fillna("").astype(str).values.tolist()

    service.spreadsheets().values().clear(spreadsheetId=sheet_id, range=tab_name).execute()
    service.spreadsheets().values().update(
        spreadsheetId=sheet_id,
        range=tab_name,
        valueInputOption="RAW",
        body={"values": values},
    ).execute()


# =========================
# STREAMLINE: GetHousekeepingCleaningReport
# =========================
def fetch_housekeeping_cleaning_report(session: requests.Session) -> dict:
    start_date = (date.today() - timedelta(days=REPORT_START_DAYS_BACK)).strftime("%m/%d/%Y")
    end_date = (date.today() + timedelta(days=REPORT_END_DAYS_FORWARD)).strftime("%m/%d/%Y")

    payload = {
        "methodName": "GetHousekeepingCleaningReport",
        "params": {
            "report_startdate": start_date,
            "report_enddate": end_date,
            "token_key": STREAMLINE_TOKEN_KEY,
            "token_secret": STREAMLINE_TOKEN_SECRET,
        },
    }

    resp = session.post(
        STREAMLINE_URL,
        json=payload,
        headers=STREAMLINE_HEADERS,
        timeout=(10, 40),
    )

    print("Streamline resposta:", resp.status_code, (resp.text[:500] if resp.text else ""))

    try:
        resp.raise_for_status()
    except requests.HTTPError as e:
        raise RuntimeError(f"HTTP error Streamline: {e} | body={resp.text[:500]}")

    try:
        data = resp.json()
    except ValueError:
        raise RuntimeError(f"Resposta não-JSON Streamline: {resp.text[:500]}")

    if isinstance(data, dict) and data.get("error"):
        raise RuntimeError(f"API Streamline retornou erro: {data.get('error')}")

    return data


# =========================
# FLAGS: mesma lógica
# =========================
def integrate_flags(property_list_wordpress: pd.DataFrame) -> pd.DataFrame:
    col_flags = "flags.flag"
    if col_flags not in property_list_wordpress.columns:
        col_flags = "flags_flag"

    if col_flags in property_list_wordpress.columns:
        flags_df = property_list_wordpress[["confirmation_id", col_flags]].copy()
        flags_df[col_flags] = flags_df[col_flags].apply(
            lambda x: x if isinstance(x, list) else ([] if pd.isnull(x) else [x])
        )

        flags_exploded = flags_df.explode(col_flags)
        flags_exploded = flags_exploded[flags_exploded[col_flags].notnull()]

        flags_details = pd.json_normalize(flags_exploded[col_flags])
        flags_details["confirmation_id"] = flags_exploded["confirmation_id"].values

        def format_flag(row):
            flag_name = row.get("name", "")
            flag_comments = row.get("comments", "")
            if isinstance(flag_comments, dict):
                flag_comments = ""
            if pd.isna(flag_comments):
                flag_comments = ""
            if flag_comments and str(flag_comments).strip():
                return f"{flag_name} ({flag_comments})"
            return flag_name

        flags_details["flag_final"] = flags_details.apply(format_flag, axis=1)
        flags_agg = (
            flags_details.groupby("confirmation_id")["flag_final"]
            .apply(lambda x: ", ".join([v for v in x if v]))
            .reset_index()
        )

        property_list_wordpress = property_list_wordpress.drop(columns=[col_flags])
        merged_df = property_list_wordpress.merge(flags_agg, on="confirmation_id", how="left")
        return merged_df

    return property_list_wordpress


# =========================
# MySQL enrich
# =========================
def build_mysql_engine():
    url = URL.create(
        drivername="mysql+pymysql",
        username=MYSQL_USER,
        password=MYSQL_PASSWORD,   # pode ter @, !, etc.
        host=MYSQL_HOST,
        database=MYSQL_DATABASE,
        query={"charset": "utf8mb4"},
    )
    return create_engine(url, pool_pre_ping=True, pool_recycle=3600)

def enrich_with_mysql(merged_df: pd.DataFrame) -> pd.DataFrame:
    engine = build_mysql_engine()

    df_type = pd.read_sql_query(
        """
        SELECT
            confirmation_id,
            type_name
        FROM tb_reservas
        """,
        engine,
    )
    merged_df = merged_df.merge(df_type, on="confirmation_id", how="left")

    df_conc = pd.read_sql_query(
        """
        SELECT
            id AS id_house,
            CONCAT(bedrooms_number, 'br/', bathrooms_number, 'ba') AS conc
        FROM tb_property_list_wordpress

        UNION

        SELECT
            id AS id_house,
            CONCAT(bedrooms_number, 'br/', bathrooms_number, 'ba') AS conc
        FROM tb_non_renting
        """,
        engine,
    )

    merged_df["unit_id"] = pd.to_numeric(merged_df.get("unit_id"), errors="coerce").astype("Int64")

    df_conc["id_house"] = pd.to_numeric(df_conc.get("id_house"), errors="coerce").astype("Int64")

    merged_df = merged_df.merge(
        df_conc,
        left_on="unit_id",
        right_on="id_house",
        how="left",
    )

    merged_df = merged_df.merge(df_conc, left_on="unit_id", right_on="id_house", how="left")

    merged_df["Unit"] = merged_df.apply(
        lambda row: f"{row.get('unit_name','')} ({row.get('conc','')})".strip(),
        axis=1,
    )

    merged_df["Unit"] = merged_df["Unit"].replace(
        r"412 OC  \- 5BR Home Bliss: Private Pool - Sleeps 12 \(5br/4ba\)",
        "412 OC 5BR Home Bliss Private Pool Sleeps 12 (5br/4ba)",
        regex=True,
    )

    merged_df["Unit"] = merged_df["Unit"].replace(
        r"^\*?1509 MC - Luxury 6BR Villa with Pool & Games \(6br/5ba\)$",
        "1509 MC - Exquisite 8BR Villa with Pool & Games (8br/5ba)",
        regex=True,
    )

    return merged_df


# =========================
# Montagem final_df
# =========================
def build_final_df(merged_df: pd.DataFrame) -> pd.DataFrame:
    merged_df["Service name"] = SERVICE_NAME_DEFAULT

    columns_mapping = {
        "Cleaning Date": "cleaning_date",
        "Cleaning Type": "cleaning_type",
        "Reservation #": "confirmation_id",
        "Flags": "flag_final",
        "Res Type": "type_name",
        "Unit": "Unit",
        "Cleaning Event": None,
        "Unit Status": None,
        "Check-In Date": "start_date",
        "Check-Out Date": "end_date",
        "Next Arrival": "next_arrival_date",
        "Next Arrival Type": None,
        "Next Arrival Flags": None,
        "Night Gap": None,
        "Service name": "Service name",
        "Observacao": None,
        "Staff": None,
    }

    final_df = pd.DataFrame()
    for final_col, orig_col in columns_mapping.items():
        if orig_col is not None and orig_col in merged_df.columns:
            final_df[final_col] = merged_df[orig_col]
        else:
            final_df[final_col] = ""

    date_columns = ["Cleaning Date", "Check-In Date", "Check-Out Date", "Next Arrival"]
    for col in date_columns:
        final_df[col] = pd.to_datetime(final_df[col], errors="coerce")

    final_df.loc[final_df["Unit"].isin(UNITS_NO_LAUNDRY), "Service name"] = SERVICE_NAME_NO_LAUNDRY

    return final_df


def write_excels_local(final_df: pd.DataFrame) -> pd.DataFrame:
    # salva full
    with pd.ExcelWriter(
        EXCEL_FULL_LOCAL,
        engine="xlsxwriter",
        date_format="dd/mm/yyyy",
        datetime_format="dd/mm/yyyy",
    ) as w:
        final_df.to_excel(w, sheet_name="Reservas", index=False)

    # salva upload (Cleaning Date >= hoje)
    today = datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)
    filtered_df = final_df[final_df["Cleaning Date"] >= today].copy()

    with pd.ExcelWriter(
        EXCEL_UPLOAD_LOCAL,
        engine="xlsxwriter",
        date_format="dd/mm/yyyy",
        datetime_format="dd/mm/yyyy",
    ) as w:
        filtered_df.to_excel(w, sheet_name="Reservas", index=False)

    return filtered_df

def normalize_reservation_key(df: pd.DataFrame, col: str = "Reservation #") -> pd.DataFrame:
    """
    Normaliza a coluna de chave do merge para string consistente.
    - Remove espaços
    - Converte floats tipo 33761.0 -> "33761"
    - Converte NaN -> ""
    """
    if df is None or df.empty:
        return df

    if col not in df.columns:
        # não cria coluna aqui, só retorna
        return df

    s = df[col]

    # Primeiro converte pra string preservando NaN
    s = s.astype("string")

    # Tira espaços e normaliza
    s = s.str.strip()

    # Corrige casos vindos como "33761.0"
    # (somente quando for número com .0 no final)
    s = s.str.replace(r"^(\d+)\.0$", r"\1", regex=True)

    # Se vier vazio/NA vira ""
    s = s.fillna("")

    df[col] = s
    return df


# =========================
# DIFF (comparação atual vs anterior)
# =========================
def compute_changes(df_atual: pd.DataFrame, df_anterior: pd.DataFrame) -> Tuple[pd.DataFrame, str]:
    if df_atual.empty:
        df_atual = pd.DataFrame(columns=df_anterior.columns)
    if df_anterior.empty:
        df_anterior = pd.DataFrame(columns=df_atual.columns)

    if "Reservation #" not in df_atual.columns:
        df_atual["Reservation #"] = pd.NA
    if "Reservation #" not in df_anterior.columns:
        df_anterior["Reservation #"] = pd.NA

    df_atual = normalize_reservation_key(df_atual, "Reservation #")
    df_anterior = normalize_reservation_key(df_anterior, "Reservation #")

    df_merged = pd.merge(
        df_atual,
        df_anterior,
        on="Reservation #",
        suffixes=("_atual", "_anterior"),
)

    colunas_datas = [
        "Check-In Date_atual", "Check-In Date_anterior",
        "Check-Out Date_atual", "Check-Out Date_anterior",
        "Next Arrival_atual", "Next Arrival_anterior",
        "Cleaning Date_atual", "Cleaning Date_anterior",
    ]
    for col in colunas_datas:
        if col not in df_merged.columns:
            df_merged[col] = pd.NaT
        df_merged[col] = pd.to_datetime(df_merged[col], errors="coerce").fillna(pd.Timestamp(0))

    df_merged["Unit_atual"] = df_merged.get("Unit_atual", "").fillna("")
    df_merged["Unit_anterior"] = df_merged.get("Unit_anterior", "").fillna("")

    hoje = pd.Timestamp("today").normalize()

    condicao = (
        (
            (df_merged["Next Arrival_atual"] != df_merged["Next Arrival_anterior"]) |
            (df_merged["Cleaning Date_atual"] != df_merged["Cleaning Date_anterior"]) |
            (df_merged["Unit_atual"] != df_merged["Unit_anterior"])
        )
        & (df_merged["Cleaning Date_atual"] >= hoje)
    )

    df_alterados = df_merged.loc[condicao].copy()

    colunas_selecionadas = [
        "Reservation #",
        "Check-In Date_atual", "Check-In Date_anterior",
        "Check-Out Date_atual", "Check-Out Date_anterior",
        "Next Arrival_atual", "Next Arrival_anterior",
        "Cleaning Date_atual", "Cleaning Date_anterior",
        "Unit_atual", "Unit_anterior",
    ]
    df_alterados = df_alterados[colunas_selecionadas]

    reservas_exclusivas = df_anterior[~df_anterior["Reservation #"].isin(df_atual["Reservation #"])].copy()
    reservas_exclusivas = reservas_exclusivas[pd.to_datetime(reservas_exclusivas["Cleaning Date"], errors="coerce") > hoje]

    reservas_exclusivas = reservas_exclusivas.rename(columns={
        "Check-In Date": "Check-In Date_anterior",
        "Check-Out Date": "Check-Out Date_anterior",
        "Next Arrival": "Next Arrival_anterior",
        "Cleaning Date": "Cleaning Date_anterior",
        "Unit": "Unit_anterior",
    })

    for col in [
        "Check-In Date_atual", "Check-Out Date_atual",
        "Next Arrival_atual", "Cleaning Date_atual",
        "Unit_atual",
    ]:
        reservas_exclusivas[col] = pd.NA

    reservas_exclusivas = reservas_exclusivas[[
        "Reservation #",
        "Check-In Date_atual", "Check-In Date_anterior",
        "Check-Out Date_atual", "Check-Out Date_anterior",
        "Next Arrival_atual", "Next Arrival_anterior",
        "Cleaning Date_atual", "Cleaning Date_anterior",
        "Unit_atual", "Unit_anterior",
    ]]

    df_final = pd.concat([df_alterados, reservas_exclusivas], ignore_index=True)

    mensagens = []
    for _, row in df_final.iterrows():
        res_num = row["Reservation #"]

        if pd.notna(row["Check-In Date_atual"]):
            msg = f"Reserva #{res_num} foi alterada."
            alteracoes = []

            if row["Check-In Date_atual"] != row["Check-In Date_anterior"]:
                alteracoes.append(
                    f"Check-In Date: de {pd.to_datetime(row['Check-In Date_anterior'], errors='coerce').date()} "
                    f"para {pd.to_datetime(row['Check-In Date_atual'], errors='coerce').date()}"
                )
            if row["Check-Out Date_atual"] != row["Check-Out Date_anterior"]:
                alteracoes.append(
                    f"Check-Out Date: de {pd.to_datetime(row['Check-Out Date_anterior'], errors='coerce').date()} "
                    f"para {pd.to_datetime(row['Check-Out Date_atual'], errors='coerce').date()}"
                )
            if row["Next Arrival_atual"] != row["Next Arrival_anterior"]:
                alteracoes.append(
                    f"Next Arrival: de {pd.to_datetime(row['Next Arrival_anterior'], errors='coerce').date()} "
                    f"para {pd.to_datetime(row['Next Arrival_atual'], errors='coerce').date()}"
                )
            if row["Cleaning Date_atual"] != row["Cleaning Date_anterior"]:
                alteracoes.append(
                    f"Cleaning Date: de {pd.to_datetime(row['Cleaning Date_anterior'], errors='coerce').date()} "
                    f"para {pd.to_datetime(row['Cleaning Date_atual'], errors='coerce').date()}"
                )
            if row["Unit_atual"] != row["Unit_anterior"]:
                alteracoes.append(f"Unit: de '{row['Unit_anterior']}' para '{row['Unit_atual']}'")

            if alteracoes:
                msg += "\n  Alterações: " + "; ".join(alteracoes)
            else:
                msg += "\n  Não foram encontradas alterações nas datas ou na unidade."
        else:
            cleaning_date = row["Cleaning Date_anterior"]
            msg = (
                f"Reserva #{res_num} foi cancelada e excluída, pois não consta no arquivo atual. "
                f"(Cleaning Date: {pd.to_datetime(cleaning_date, errors='coerce').date()}, "
                f"Unit: '{row['Unit_anterior']}')"
            )

        mensagens.append(msg)

    email_texto = "\n\n".join(mensagens)
    return df_final, email_texto


# =========================
# PLAYWRIGHT HELPERS (Aloha)
# =========================
def build_browser():
    """
    Retorna (playwright, browser, context, page)
    """
    playwright = sync_playwright().start()
    browser = playwright.chromium.launch(
        headless=True,
        args=["--no-sandbox", "--disable-dev-shm-usage"],
    )
    context = browser.new_context(viewport={"width": 1920, "height": 1080})
    page = context.new_page()
    return playwright, browser, context, page



def aloha_login(page):
    # 1) abre login
    page.goto(ALOHA_LOGIN_URL, wait_until="domcontentloaded", timeout=60000)

    # Debug básico (vai pro log do Airflow)
    print("[ALOHA] URL após goto:", page.url)
    try:
        print("[ALOHA] title:", page.title())
    except Exception:
        pass

    # 2) espera inputs existirem
    page.wait_for_selector("#UserUsername", timeout=60000)
    page.wait_for_selector("#UserPassword", timeout=60000)

    # 3) preenche
    page.fill("#UserUsername", ALOHA_EMAIL)
    page.fill("#UserPassword", ALOHA_SENHA)

    # 4) tenta clicar no botão de várias formas
    clicked = False
    selectors = [
        "//button[normalize-space()='Entrar']",
        "//button[contains(normalize-space(.), 'Entrar')]",
        "button[type='submit']",
        "input[type='submit']",
        "text=Entrar",
    ]

    for sel in selectors:
        try:
            page.wait_for_selector(sel, timeout=8000)
            page.click(sel)
            clicked = True
            break
        except PlaywrightTimeoutError:
            continue
        except Exception:
            continue

    if not clicked:
        # fallback: submit via Enter no campo senha
        try:
            page.focus("#UserPassword")
            page.keyboard.press("Enter")
            clicked = True
        except Exception:
            clicked = False

    # 5) confirma login
    try:
        page.wait_for_selector("a[data-click='NewOrderUploads']", timeout=60000)
        print("[ALOHA] Login OK")
        return True
    except PlaywrightTimeoutError:
        # DEBUG PESADO: salva evidências no /tmp (no container)
        ts = int(time.time())
        try:
            page.screenshot(path=f"/tmp/aloha_login_fail_{ts}.png", full_page=True)
            print(f"[ALOHA] screenshot salvo em /tmp/aloha_login_fail_{ts}.png")
        except Exception as e:
            print("[ALOHA] falhou screenshot:", repr(e))

        try:
            html = page.content()
            with open(f"/tmp/aloha_login_fail_{ts}.html", "w", encoding="utf-8") as f:
                f.write(html)
            print(f"[ALOHA] html salvo em /tmp/aloha_login_fail_{ts}.html")
        except Exception as e:
            print("[ALOHA] falhou salvar html:", repr(e))

        print("[ALOHA] URL final:", page.url)
        try:
            print("[ALOHA] title final:", page.title())
        except Exception:
            pass

        raise RuntimeError("Falha no login do Aloha: não encontrou NewOrderUploads após submit.")


# =========================
# ALOHA: aplicar alterações/cancelamentos
# =========================
def aloha_apply_changes(df_changes: pd.DataFrame):
    if df_changes.empty:
        print("Sem alterações/cancelamentos.")
        return

    playwright, browser, context, page = build_browser()

    try:
        aloha_login(page)

        for _, row in df_changes.iterrows():
            reserva = str(row["Reservation #"])
            print(f"Processando reserva {reserva}")

            page.goto(ALOHA_ORDERS_URL, timeout=60000)

            # limpa filtros e pesquisa
            page.click("a[href='/orders/index/clear']")
            page.select_option("#filterFilter2", "1")  # Aguardando
            page.fill("#filterFilter5", reserva)
            page.click("//button[contains(., 'search')]")

            # CANCELADA
            if pd.isna(row["Check-In Date_atual"]):
                try:
                    page.click(
                        "div.checkbox.text-center.m-t-0.m-b-0.checkbox-circle.check-primary input[type='checkbox']",
                        timeout=5000,
                    )
                except PlaywrightTimeoutError:
                    print(f"Reserva {reserva} não encontrada")
                    continue

                # IMPORTANTE: registrar handler antes do clique que abre o dialog
                page.once("dialog", lambda d: d.accept())
                page.click("//a[@data-toggle='delete' and contains(., 'Delete')]")
                print(f"Reserva {reserva} deletada")
                time.sleep(1)
                continue

            # ALTERADA: abre edição
            try:
                page.click(f"//small[@data-original-title='{reserva}']", timeout=10000)
                page.wait_for_selector("#OrderDtNextCheckinGuest", timeout=20000)
            except PlaywrightTimeoutError:
                print(f"Reserva {reserva} não encontrada para edição")
                continue

            # Atualiza Next Arrival
            if row["Next Arrival_atual"] != row["Next Arrival_anterior"]:
                dt = pd.to_datetime(row["Next Arrival_atual"], errors="coerce")
                if pd.notna(dt):
                    # formato esperado: ddmmyyyy (como seu fluxo antigo)
                    page.fill("#OrderDtNextCheckinGuest", dt.strftime("%d%m%Y"))

            # Atualiza Cleaning Date
            if row["Cleaning Date_atual"] != row["Cleaning Date_anterior"]:
                dt = pd.to_datetime(row["Cleaning Date_atual"], errors="coerce")
                if pd.notna(dt):
                    page.fill("#OrderDtCheckoutGuest", dt.strftime("%d%m%Y"))

            page.click("//button[@type='submit' and contains(@class,'btn-primary')]")
            print(f"Reserva {reserva} atualizada")
            time.sleep(1)

    finally:
        try:
            context.close()
        except Exception:
            pass
        try:
            browser.close()
        except Exception:
            pass
        try:
            playwright.stop()
        except Exception:
            pass


# =========================
# ALOHA: upload + casas não cadastradas
# =========================
def fazer_upload(page, cliente: str, caminho_arquivo: str, qtd_linhas: int):
    log_message = ""
    casas_nao_cadastradas: List[str] = []

    try:
        page.wait_for_selector("a[data-click='NewOrderUploads']", timeout=30000)
        page.click("a[data-click='NewOrderUploads']")

        page.wait_for_selector("#OrderUploadCompanyId", timeout=30000)
        page.select_option("#OrderUploadCompanyId", label=cliente)

        page.fill("#OrderUploadMaxRows", str(qtd_linhas))

        # upload arquivo
        page.set_input_files("#OrderUploadFile", caminho_arquivo)

        page.click("//button[contains(., 'continue')]")
        time.sleep(8)

        page.wait_for_selector("div.panel-body", timeout=60000)

        # casas não cadastradas aparecem nesses smalls
        page.wait_for_selector("div.row div.col-md-3 small", timeout=60000)
        casas_elements = page.query_selector_all("div.row div.col-md-3 small")
        casas_nao_cadastradas = [c.inner_text().strip() for c in casas_elements if c.inner_text().strip()]

        # tenta clicar Importar, se existir
        importar_btn = page.query_selector("//button[contains(., 'Importar')]")
        if importar_btn:
            importar_btn.click()
            log_message += f"Upload para o cliente {cliente} realizado com sucesso!\n"
        else:
            log_message += f"Sem novas reservas para importar para o cliente {cliente}.\n"

    except Exception as e:
        log_message += f"Ocorreu um erro durante o upload para {cliente}. Erro: {e}\n"

    return casas_nao_cadastradas, log_message


def aloha_upload_and_collect_missing():
    playwright, browser, context, page = build_browser()

    try:
        aloha_login(page)

        # qtd linhas = linhas do excel full + 50
        try:
            qtd_linhas = pd.read_excel(EXCEL_FULL_LOCAL).shape[0] + 50
        except Exception:
            qtd_linhas = 5000  # fallback
        print("qtd_linhas:", qtd_linhas)

        print("Iniciando upload para ONE VACATION...")
        casas_one, log_one = fazer_upload(page, CLIENT_ONE, EXCEL_UPLOAD_LOCAL, qtd_linhas)

        page.goto(ALOHA_ORDERS_URL, timeout=60000)
        page.wait_for_selector("a[data-click='NewOrderUploads']", timeout=60000)

        print("Iniciando upload para SNOW BIRD...")
        casas_snow, log_snow = fazer_upload(page, CLIENT_SNOW, EXCEL_UPLOAD_LOCAL, qtd_linhas)

        df_one = pd.DataFrame(casas_one, columns=["Casa"])
        df_snow = pd.DataFrame(casas_snow, columns=["Casa"])

        df_final = pd.merge(df_one, df_snow, on="Casa", how="inner").drop_duplicates().reset_index(drop=True)
        df_final.rename(columns={"Casa": "casas a cadastrar"}, inplace=True)

        print("\nCasas a cadastrar (presentes em ambos os uploads):")
        print(df_final)

        return df_final, log_one, log_snow

    finally:
        try:
            context.close()
        except Exception:
            pass
        try:
            browser.close()
        except Exception:
            pass
        try:
            playwright.stop()
        except Exception:
            pass


# =========================
# GMAIL: envio
# =========================
def send_custom_email_with_attachment(
    service,
    recipient,
    subject,
    message_html,
    attachment_path: Optional[str] = None,
    sender: str = EMAIL_SENDER,
):
    recipient_str = ", ".join(recipient) if isinstance(recipient, list) else recipient

    message = MIMEMultipart()
    message["to"] = recipient_str
    message["from"] = sender
    message["subject"] = subject
    message.attach(MIMEText(message_html, "html"))

    if attachment_path:
        with open(attachment_path, "rb") as f:
            attachment_data = f.read()
        attachment = MIMEApplication(attachment_data, _subtype="octet-stream")
        attachment.add_header("Content-Disposition", "attachment", filename=os.path.basename(attachment_path))
        message.attach(attachment)

    raw_message = base64.urlsafe_b64encode(message.as_bytes()).decode("utf-8")
    body = {"raw": raw_message}
    sent_message = service.users().messages().send(userId=GMAIL_USER_ID, body=body).execute()
    print("E-mail enviado com sucesso! ID:", sent_message.get("id"))
    return sent_message


# =========================
# PIPELINE PRINCIPAL
# =========================
def pipeline_run():
    # 0) lê sheets atual e last
    df_aloha_atual_old = read_sheet_as_df(SHEET_ID_ALOHA_ATUAL, SHEET_TAB_NAME)
    df_aloha_last_old = read_sheet_as_df(SHEET_ID_ALOHA_LAST, SHEET_TAB_NAME)  # mantido por compat

    # 1) snapshot: sobrescreve ALOHA_LAST com o antigo ALOHA atual
    write_df_to_sheet(df_aloha_atual_old, SHEET_ID_ALOHA_LAST, SHEET_TAB_NAME)
    print("ALOHA_LAST sobrescrita com snapshot do ALOHA atual (pré-atualização).")

    # 2) Streamline
    session = build_session()
    payload = fetch_housekeeping_cleaning_report(session)

    # 3) normaliza JSON em DataFrame
    # cuidado: se estrutura vier diferente, estoura KeyError -> preferível falhar e ver log
    property_list_wordpress = pd.json_normalize(
        payload["data"],
        record_path=["reservations", "reservation"],
        meta=["result_id", "past_count"],
        sep="_",
        max_level=1,
    )

    # 4) flags
    merged_df = integrate_flags(property_list_wordpress)

    # 5) enrich MySQL
    merged_df = enrich_with_mysql(merged_df)

    # 6) monta final_df
    final_df = build_final_df(merged_df)

    # 7) sobrescreve ALOHA atual no Sheets
    write_df_to_sheet(final_df, SHEET_ID_ALOHA_ATUAL, SHEET_TAB_NAME)
    print("ALOHA (atual) sobrescrita no Google Sheets.")

    # 8) gera excel local (necessário pro upload)
    filtered_df = write_excels_local(final_df)
    print(
        f"Excel local gerado: {EXCEL_FULL_LOCAL} | Upload: {EXCEL_UPLOAD_LOCAL} | "
        f"Linhas upload: {len(filtered_df)}"
    )

    # 9) diff: compara novo vs snapshot anterior (df_aloha_atual_old)
    df_changes, email_texto = compute_changes(final_df, df_aloha_atual_old)

    # 10) aplica alterações/cancelamentos no Aloha (Playwright)
    aloha_apply_changes(df_changes)

    # 11) upload + casas a cadastrar
    casas_df, log_one, log_snow = aloha_upload_and_collect_missing()

    # 12) email
    texto_html = (email_texto or "").replace("\n", "<br>")
    df_html = casas_df.to_html(index=False) if casas_df is not None and not casas_df.empty else "<p>(Nenhuma casa a cadastrar)</p>"

    body = f"""
    <html>
      <body>
        <p>Olá,</p>
        <p>Este é um e-mail enviado automaticamente pelo script.</p>
        <p><b>Upload Aloha foi finalizado:</b></p>
        <p>{(log_one or "").replace("\\n","<br>")}</p>
        <p>{(log_snow or "").replace("\\n","<br>")}</p>
        <p><b>Reservas que sofreram alterações no último upload:</b></p>
        <p>{texto_html}</p>
        <p><b>Casas a cadastrar (presentes em ambos os uploads):</b></p>
        {df_html}
      </body>
    </html>
    """

    service = get_gmail_service()
    send_custom_email_with_attachment(
        service=service,
        recipient=EMAIL_RECIPIENTS,
        subject=EMAIL_SUBJECT,
        message_html=body,
        attachment_path=EXCEL_UPLOAD_LOCAL,  # opcional
        sender=EMAIL_SENDER,
    )

    print("Pipeline concluído com sucesso.")


# =========================
# AIRFLOW DAG
# =========================
with DAG(
    dag_id=DAG_ID,
    start_date=DAG_START_DATE,
    schedule=DAG_SCHEDULE,
    catchup=False,
    tags=["ALOHA", "Streamline", "Sheets", "Playwright", "Gmail"],
) as dag:

    @task(retries=4, retry_exponential_backoff=True)
    def run_aloha_pipeline():
        pipeline_run()

    run_aloha_pipeline()


# =========================
# Execução local (fora do Airflow)
# =========================
if __name__ == "__main__":
    pipeline_run()
