# -*- coding: utf-8 -*-
"""
ALOHA PIPELINE (SINGLE FILE) — Airflow-ready
- Lê Aloha (atual) e Aloha_last (snapshot) de Google Sheets (aba: "Página1")
- Sobrescreve Aloha_last com o conteúdo anterior de Aloha (equivalente ao shutil.copy)
- Consome Streamline (GetHousekeepingCleaningReport) com HTTP resiliente (Retry)
- Processa flags, enriquece com SQL Server (mesma lógica), monta final_df
- Gera Excel local /tmp/Aloha_upload.xlsx para o upload via Selenium
- Compara atual vs anterior e:
    - atualiza datas (Next Arrival / Cleaning Date) no Aloha Smart Services
    - remove reservas canceladas
- Faz upload para ONE VACATION e SNOW BIRD
- Envia e-mail via Gmail API (mesmo padrão de token salvo)
- Inclui DAG do Airflow no final

OBS:
- Você pediu tokens inline e variáveis no topo: está tudo aqui.
- Recomendação de segurança: não versionar este arquivo com senhas/tokens em plaintext.
"""

# =============================
# IMPORTS
# =============================
import os
import re
import json
import base64
import time
import numpy as np
import pandas as pd
import requests
from datetime import datetime, timedelta, date

from requests.adapters import HTTPAdapter, Retry

# SQL Server
from sqlalchemy import create_engine

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError

# Gmail / Google OAuth
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication

from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from google.auth.exceptions import RefreshError

# Airflow
import pendulum
from airflow import DAG
from airflow.decorators import task


# ==========================
# CONFIGURAÇÕES INLINE (TOPO)
# ==========================

# --- TIMEZONE ---
SP_TZ = pendulum.timezone("America/Sao_Paulo")

# --- STREAMLINE ---
STREAMLINE_URL = "https://web.streamlinevrs.com/api/json"
STREAMLINE_TOKEN_KEY = "3ef223d3bbf7086cfb86df7e98d6e5d2"
STREAMLINE_TOKEN_SECRET = "a88d05b895affb815cc8a4d96670698ee486ea30"
STREAMLINE_HEADERS = {"Content-Type": "application/json"}

# Janela dinâmica (mesma regra do seu código)
REPORT_START_DAYS_BACK = 5
REPORT_END_DAYS_FORWARD = 85

# --- GOOGLE (Sheets + Gmail) ---
# Sheets
SHEETS_SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

# Gmail
GMAIL_SCOPES = [
    "https://www.googleapis.com/auth/gmail.modify",
    "https://www.googleapis.com/auth/gmail.send",
]
GMAIL_USER_ID = "me"

# Pastas/arquivos de credenciais (mesmo padrão do seu exemplo)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))  # /opt/airflow/dags
TOKENS_DIR = os.path.join(BASE_DIR, "Tokens")

GOOGLE_CREDENTIALS_FILE = os.path.join(TOKENS_DIR, "credentials.json")
GOOGLE_TOKEN_FILE = os.path.join(TOKENS_DIR, "token.json")

GMAIL_CREDENTIALS_FILE = os.path.join(TOKENS_DIR, "credentials.json")
GMAIL_TOKEN_FILE = os.path.join(TOKENS_DIR, "token.json")

# --- GOOGLE SHEETS IDs (VOCÊ VAI PREENCHER) ---
# ALOHA (ATUAL) e ALOHA_LAST (SNAPSHOT)
SHEET_ID_ALOHA_ATUAL = "1pUpX9_zCQSYys8SUN7f52TK857ocJrphKVfbgtka5k8"
SHEET_ID_ALOHA_LAST = "1G6e4g3ZG4bH66kCqjihmX2yTX-kkojxunFffKRAEKgY"

# Aba padrão genérica
SHEET_TAB_NAME = "Página1"

# --- ALOHA SMART SERVICES (LOGIN) ---
ALOHA_LOGIN_URL = "https://www.alohasmartservices.com/users/login"
ALOHA_ORDERS_URL = "https://www.alohasmartservices.com/orders"

ALOHA_EMAIL = "mkj2508@gmail.com"
ALOHA_SENHA = "250909"

# --- EXCEL LOCAL (necessário para upload via Selenium file_input.send_keys) ---
EXCEL_FULL_LOCAL = "/tmp/Aloha.xlsx"
EXCEL_UPLOAD_LOCAL = "/tmp/Aloha_upload.xlsx"

# --- CLIENTES DO UPLOAD ---
CLIENT_ONE = "ONE VACATION"
CLIENT_SNOW = "SNOW BIRD"

# =========================
# MYSQL (somente leitura)
# =========================
MYSQL_HOST = "host.docker.internal"
MYSQL_USER = "root"
MYSQL_PASSWORD = "Tfl1234@"
MYSQL_DATABASE = "ovh_silver"

# --- SERVICE NAME RULES ---
SERVICE_NAME_DEFAULT = "CHECK OUT - EXTERNAL LAUNDRY"
UNITS_NO_LAUNDRY = [
    "9312 SH - Champions Gate - Unique High end 8 Beds (8br/5ba)",
    "9332 SH - Champions Gate Unique Luxury 8BR (8br/5ba)",
]
SERVICE_NAME_NO_LAUNDRY = "CHECK OUT - NO LAUNDRY"

# --- E-MAIL (ENVIO) ---
EMAIL_SENDER = "revenue@onevacationhome.com"
EMAIL_RECIPIENTS = ["revenue@onevacationhome.com", "keitianne@onevacationhome.com"]
EMAIL_SUBJECT = "Upload Aloha"

# --- DAG AIRFLOW ---
DAG_ID = "ALOHA_PIPELINE"
DAG_SCHEDULE = "0 7 * * *"  # ajuste como quiser
DAG_START_DATE = pendulum.datetime(2025, 9, 23, 8, 0, tz=SP_TZ)


# =========================
# HTTP resiliente (EXATAMENTE o padrão que você passou)
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
# GOOGLE AUTH (mesmo padrão do seu exemplo)
# =========================
def _do_interactive_login(credentials_file: str, scopes: list[str], token_file: str) -> Credentials:
    flow = InstalledAppFlow.from_client_secrets_file(credentials_file, scopes)
    creds = flow.run_local_server(port=0, access_type="offline", prompt="consent")
    os.makedirs(os.path.dirname(token_file), exist_ok=True)
    with open(token_file, "w") as f:
        f.write(creds.to_json())
    return creds


def get_credentials(credentials_file: str, scopes: list[str], token_file: str) -> Credentials:
    creds = None

    if os.path.exists(token_file):
        try:
            creds = Credentials.from_authorized_user_file(token_file, scopes)
        except Exception:
            try:
                os.remove(token_file)
            except Exception:
                pass
            creds = None

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            try:
                creds.refresh(Request())
                with open(token_file, "w") as f:
                    f.write(creds.to_json())
                return creds
            except RefreshError:
                if os.path.exists(token_file):
                    try:
                        os.remove(token_file)
                    except Exception:
                        pass
                return _do_interactive_login(credentials_file, scopes, token_file)
        else:
            return _do_interactive_login(credentials_file, scopes, token_file)

    return creds


def get_sheets_service():
    creds = get_credentials(GOOGLE_CREDENTIALS_FILE, SHEETS_SCOPES, GOOGLE_TOKEN_FILE)
    return build("sheets", "v4", credentials=creds)


def get_gmail_service():
    creds = get_credentials(GMAIL_CREDENTIALS_FILE, GMAIL_SCOPES, GMAIL_TOKEN_FILE)
    return build("gmail", "v1", credentials=creds)


# =========================
# GOOGLE SHEETS: READ / WRITE
# =========================
def read_sheet_as_df(sheet_id: str, tab_name: str) -> pd.DataFrame:
    service = get_sheets_service()
    resp = service.spreadsheets().values().get(
        spreadsheetId=sheet_id,
        range=tab_name,
    ).execute()

    values = resp.get("values", [])
    if not values:
        return pd.DataFrame()

    header, *rows = values
    df = pd.DataFrame(rows, columns=header)

    # tenta converter colunas que parecem datas (mesma ideia do read_excel)
    # o diff depende de datas como Timestamp
    date_cols = ["Cleaning Date", "Check-In Date", "Check-Out Date", "Next Arrival"]
    for c in date_cols:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], errors="coerce", dayfirst=True)

    return df


def write_df_to_sheet(df: pd.DataFrame, sheet_id: str, tab_name: str):
    service = get_sheets_service()

    # transforma para strings (Sheets é textual); datas vão como dd/mm/yyyy para ficar legível
    df_out = df.copy()
    for c in ["Cleaning Date", "Check-In Date", "Check-Out Date", "Next Arrival"]:
        if c in df_out.columns:
            df_out[c] = pd.to_datetime(df_out[c], errors="coerce").dt.strftime("%d/%m/%Y")

    values = [df_out.columns.tolist()] + df_out.fillna("").astype(str).values.tolist()

    # clear e update (sobrescreve inteiro)
    service.spreadsheets().values().clear(
        spreadsheetId=sheet_id,
        range=tab_name,
    ).execute()

    service.spreadsheets().values().update(
        spreadsheetId=sheet_id,
        range=tab_name,
        valueInputOption="RAW",
        body={"values": values},
    ).execute()


# =========================
# STREAMLINE: GetHousekeepingCleaningReport (com session resiliente)
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

    # log leve (similar ao seu exemplo)
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
# FLAGS: mesma lógica do seu código
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
# SQL Server enrich: mesma lógica do seu código
# =========================
def build_mysql_engine():
    conn_str = (
        f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}"
        f"@{MYSQL_HOST}/{MYSQL_DATABASE}"
        "?charset=utf8mb4"
    )
    return create_engine(conn_str)


def enrich_with_mysql(merged_df: pd.DataFrame) -> pd.DataFrame:
    engine = build_mysql_engine()

    # ---- tb_reservas (antes: tb_reservas_full) ----
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

    # ---- tb_property_list_wordpress + tb_non_renting ----
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

    merged_df = merged_df.merge(
        df_conc,
        left_on="unit_id",
        right_on="id_house",
        how="left",
    )

    merged_df["Unit"] = merged_df.apply(
        lambda row: f"{row['unit_name']} ({row['conc']})",
        axis=1,
    )

    # ---- correções específicas (mantidas iguais) ----
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
# Montagem final_df (mesma lógica)
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

    # datas como datetime (sem strftime)
    date_columns = ["Cleaning Date", "Check-In Date", "Check-Out Date", "Next Arrival"]
    for col in date_columns:
        final_df[col] = pd.to_datetime(final_df[col], format="%m/%d/%Y", errors="coerce")

    # regra NO LAUNDRY
    final_df.loc[final_df["Unit"].isin(UNITS_NO_LAUNDRY), "Service name"] = SERVICE_NAME_NO_LAUNDRY
    return final_df


def write_excels_local(final_df: pd.DataFrame):
    # full
    with pd.ExcelWriter(EXCEL_FULL_LOCAL, engine="xlsxwriter", date_format="dd/mm/yyyy", datetime_format="dd/mm/yyyy") as w:
        final_df.to_excel(w, sheet_name="Reservas", index=False)

    # upload filtrado (Cleaning Date >= hoje)
    today = datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)
    filtered_df = final_df[final_df["Cleaning Date"] >= today]
    with pd.ExcelWriter(EXCEL_UPLOAD_LOCAL, engine="xlsxwriter", date_format="dd/mm/yyyy", datetime_format="dd/mm/yyyy") as w:
        filtered_df.to_excel(w, sheet_name="Reservas", index=False)

    return filtered_df


# =========================
# DIFF (mesma lógica do seu merge/condições)
# =========================
def compute_changes(df_atual: pd.DataFrame, df_anterior: pd.DataFrame) -> tuple[pd.DataFrame, str]:
    # garante colunas
    if df_atual.empty:
        df_atual = pd.DataFrame(columns=df_anterior.columns)
    if df_anterior.empty:
        df_anterior = pd.DataFrame(columns=df_atual.columns)

    # merge pelo Reservation #
    df_merged = pd.merge(df_atual, df_anterior, on="Reservation #", suffixes=("_atual", "_anterior"))

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

    # Unit pode estar vazio
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

    df_alterados = df_merged.loc[condicao]

    colunas_selecionadas = [
        "Reservation #",
        "Check-In Date_atual", "Check-In Date_anterior",
        "Check-Out Date_atual", "Check-Out Date_anterior",
        "Next Arrival_atual", "Next Arrival_anterior",
        "Cleaning Date_atual", "Cleaning Date_anterior",
        "Unit_atual", "Unit_anterior",
    ]
    df_alterados = df_alterados[colunas_selecionadas]

    # canceladas (sumiram do atual)
    reservas_exclusivas = df_anterior[~df_anterior["Reservation #"].isin(df_atual["Reservation #"])]
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

    # mensagens
    mensagens = []
    for _, row in df_final.iterrows():
        res_num = row["Reservation #"]

        if pd.notna(row["Check-In Date_atual"]):
            msg = f"Reserva #{res_num} foi alterada."
            alteracoes = []

            if row["Check-In Date_atual"] != row["Check-In Date_anterior"]:
                alteracoes.append(f"Check-In Date: de {row['Check-In Date_anterior'].date()} para {row['Check-In Date_atual'].date()}")
            if row["Check-Out Date_atual"] != row["Check-Out Date_anterior"]:
                alteracoes.append(f"Check-Out Date: de {row['Check-Out Date_anterior'].date()} para {row['Check-Out Date_atual'].date()}")
            if row["Next Arrival_atual"] != row["Next Arrival_anterior"]:
                alteracoes.append(f"Next Arrival: de {row['Next Arrival_anterior'].date()} para {row['Next Arrival_atual'].date()}")
            if row["Cleaning Date_atual"] != row["Cleaning Date_anterior"]:
                alteracoes.append(f"Cleaning Date: de {row['Cleaning Date_anterior'].date()} para {row['Cleaning Date_atual'].date()}")
            if row["Unit_atual"] != row["Unit_anterior"]:
                alteracoes.append(f"Unit: de '{row['Unit_anterior']}' para '{row['Unit_atual']}'")

            if alteracoes:
                msg += "\n  Alterações: " + "; ".join(alteracoes)
            else:
                msg += "\n  Não foram encontradas alterações nas datas ou na unidade."
        else:
            cleaning_date = row["Cleaning Date_anterior"]
            msg = (
                f"Reserva #{res_num} foi cancelada e excluída, "
                f"pois não consta no arquivo atual. "
                f"(Cleaning Date: {pd.to_datetime(cleaning_date, errors='coerce').date()}, "
                f"Unit: '{row['Unit_anterior']}')"
            )

        mensagens.append(msg)

    email_texto = "\n\n".join(mensagens)
    return df_final, email_texto


# =========================
# SELENIUM: delete/update (mesma lógica)
# =========================
ARROW_LEFT = u"\ue012"
ARROW_RIGHT = u"\ue014"
HOME = u"\ue011"


def build_browser():
    playwright = sync_playwright().start()
    browser = playwright.chromium.launch(
        headless=True,
        args=["--no-sandbox", "--disable-dev-shm-usage"],
    )
    context = browser.new_context(viewport={"width": 1920, "height": 1080})
    page = context.new_page()
    return playwright, browser, page


def aloha_login(page):
    page.goto(ALOHA_LOGIN_URL, timeout=60000)

    page.fill("#UserUsername", ALOHA_EMAIL)
    page.fill("#UserPassword", ALOHA_SENHA)

    page.click("//button[text()='Entrar']")

    # garante login
    page.wait_for_selector("a[data-click='NewOrderUploads']", timeout=60000)



def aloha_apply_changes(df_changes: pd.DataFrame):
    if df_changes.empty:
        print("Sem alterações/cancelamentos.")
        return

    playwright, browser, page = build_browser()

    try:
        aloha_login(page)

        for _, row in df_changes.iterrows():
            reserva = str(row["Reservation #"])
            print(f"Processando reserva {reserva}")

            page.goto(ALOHA_ORDERS_URL, timeout=60000)

            page.click("a[href='/orders/index/clear']")
            page.select_option("#filterFilter2", "1")
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

                page.click("//a[@data-toggle='delete' and contains(., 'Delete')]")
                page.once("dialog", lambda d: d.accept())
                print(f"Reserva {reserva} deletada")
                continue

            # ALTERADA
            page.click(f"//small[@data-original-title='{reserva}']")

            if row["Next Arrival_atual"] != row["Next Arrival_anterior"]:
                dt = pd.to_datetime(row["Next Arrival_atual"], errors="coerce")
                if pd.notna(dt):
                    page.fill("#OrderDtNextCheckinGuest", dt.strftime("%d%m%Y"))

            if row["Cleaning Date_atual"] != row["Cleaning Date_anterior"]:
                dt = pd.to_datetime(row["Cleaning Date_atual"], errors="coerce")
                if pd.notna(dt):
                    page.fill("#OrderDtCheckoutGuest", dt.strftime("%d%m%Y"))

            page.click("//button[@type='submit' and contains(@class,'btn-primary')]")
            print(f"Reserva {reserva} atualizada")

    finally:
        browser.close()
        playwright.stop()

# =========================
# UPLOAD (mesma lógica)
# =========================
def fazer_upload(driver, wait, cliente: str, caminho_arquivo: str, qtd_linhas: int):
    log_message = ""
    casas_nao_cadastradas = []

    try:
        upload_btn = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "a[data-click='NewOrderUploads']")))
        upload_btn.click()

        select_element = wait.until(EC.presence_of_element_located((By.ID, "OrderUploadCompanyId")))
        select = Select(select_element)
        select.select_by_visible_text(cliente)

        max_rows_input = driver.find_element(By.ID, "OrderUploadMaxRows")
        max_rows_input.clear()
        max_rows_input.send_keys(qtd_linhas)

        file_input = driver.find_element(By.ID, "OrderUploadFile")
        page.set_input_files("#OrderUploadFile", caminho_arquivo)

        continue_btn = driver.find_element(By.XPATH, "//button[contains(., 'continue')]")
        continue_btn.click()
        time.sleep(10)

        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "div.panel-body")))
        wait.until(EC.visibility_of_all_elements_located((By.CSS_SELECTOR, "div.row div.col-md-3 small")))
        casas_elements = driver.find_elements(By.CSS_SELECTOR, "div.row div.col-md-3 small")
        casas_nao_cadastradas = [c.text.strip() for c in casas_elements if c.text.strip()]

        try:
            importar_btn = wait.until(EC.element_to_be_clickable((By.XPATH, "//button[contains(., 'Importar')]")))
            importar_btn.click()
            log_message += f"Upload para o cliente {cliente} realizado com sucesso!\n"
        except Exception:
            log_message += f"Sem novas reservas para importar para o cliente {cliente}.\n"

    except Exception as e:
        log_message += f"Ocorreu um erro durante o upload para {cliente}. Erro: {e}\n"

    return casas_nao_cadastradas, log_message


def aloha_upload_and_collect_missing():
    driver = build_driver()
    wait = WebDriverWait(driver, 10)

    try:
        aloha_login(driver, wait)

        # qtd linhas = linhas do "atual" + 50 (mesma lógica)
        # aqui usamos o Excel local full para contar
        qtd_linhas = pd.read_excel(EXCEL_FULL_LOCAL).shape[0] + 50
        print("qtd_linhas:", qtd_linhas)

        print("Iniciando upload para ONE VACATION...")
        casas_one, log_one = fazer_upload(driver, wait, CLIENT_ONE, EXCEL_UPLOAD_LOCAL, qtd_linhas)

        driver.get(ALOHA_ORDERS_URL)
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "a[data-click='NewOrderUploads']")))

        print("Iniciando upload para SNOW BIRD...")
        casas_snow, log_snow = fazer_upload(driver, wait, CLIENT_SNOW, EXCEL_UPLOAD_LOCAL, qtd_linhas)

        df_one = pd.DataFrame(casas_one, columns=["Casa"])
        df_snow = pd.DataFrame(casas_snow, columns=["Casa"])

        df_final = pd.merge(df_one, df_snow, on="Casa", how="inner").drop_duplicates().reset_index(drop=True)
        df_final.rename(columns={"Casa": "casas a cadastrar"}, inplace=True)

        print("\nCasas a cadastrar (presentes em ambos os uploads):")
        print(df_final)

        return df_final, log_one, log_snow

    finally:
        try:
            driver.quit()
        except Exception:
            pass


# =========================
# GMAIL: envio (mesmo padrão)
# =========================
def send_custom_email_with_attachment(
    service,
    recipient,
    subject,
    message_html,
    attachment_path=None,
    sender=EMAIL_SENDER,
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
# PIPELINE PRINCIPAL (ordem equivalente ao seu fluxo)
# =========================
def pipeline_run():
    # 0) Lê ALOHA atual e ALOHA last do Sheets
    df_aloha_atual_old = read_sheet_as_df(SHEET_ID_ALOHA_ATUAL, SHEET_TAB_NAME)
    df_aloha_last_old = read_sheet_as_df(SHEET_ID_ALOHA_LAST, SHEET_TAB_NAME)

    # 1) Snapshot: sobrescreve ALOHA_LAST com o que estava no ALOHA atual (equivalente ao shutil.copy)
    #    (você pediu: "aloha_last é sempre sobescrita")
    write_df_to_sheet(df_aloha_atual_old, SHEET_ID_ALOHA_LAST, SHEET_TAB_NAME)
    print("ALOHA_LAST sobrescrita com snapshot do ALOHA atual (pré-atualização).")

    # 2) Streamline (com Retry/Rate limit do exemplo)
    session = build_session()
    payload = fetch_housekeeping_cleaning_report(session)

    # 3) Normaliza JSON em DataFrame (mesma lógica)
    property_list_wordpress = pd.json_normalize(
        payload["data"],
        record_path=["reservations", "reservation"],
        meta=["result_id", "past_count"],
        sep="_",
        max_level=1,
    )

    # 4) Flags (mesma lógica)
    merged_df = integrate_flags(property_list_wordpress)

    # 5) Enriquecimento SQL Server (mesma lógica)
    merged_df = enrich_with_mysql(merged_df)

    # 6) Monta final_df (mesma lógica)
    final_df = build_final_df(merged_df)

    # 7) Escreve ALOHA (atual) no Sheets (substitui salvar Aloha.xlsx como “fonte de verdade”)
    write_df_to_sheet(final_df, SHEET_ID_ALOHA_ATUAL, SHEET_TAB_NAME)
    print("ALOHA (atual) sobrescrita no Google Sheets.")

    # 8) Ainda gera Excel local porque o upload Selenium precisa de arquivo
    filtered_df = write_excels_local(final_df)
    print(f"Excel local gerado: {EXCEL_FULL_LOCAL} | Upload: {EXCEL_UPLOAD_LOCAL} | Linhas upload: {len(filtered_df)}")

    # 9) Diff: compara o “novo” vs “snapshot anterior”
    #    Snapshot anterior = df_aloha_atual_old (antes da atualização), equivalente ao Aloha_last antigo
    df_changes, email_texto = compute_changes(final_df, df_aloha_atual_old)

    # 10) Aplica alterações/cancelamentos no Aloha via Selenium
    aloha_apply_changes(df_changes)

    # 11) Upload + casas a cadastrar
    casas_df, log_one, log_snow = aloha_upload_and_collect_missing()

    # 12) E-mail
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
        attachment_path=EXCEL_UPLOAD_LOCAL,  # opcional (ajuda auditoria)
        sender=EMAIL_SENDER,
    )

    print("Pipeline concluído com sucesso.")


# =========================
# AIRFLOW DAG (como você pediu)
# =========================
with DAG(
    dag_id=DAG_ID,
    start_date=DAG_START_DATE,
    schedule=DAG_SCHEDULE,
    catchup=False,
    tags=["ALOHA", "Streamline", "Sheets", "Selenium", "Gmail"],
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
