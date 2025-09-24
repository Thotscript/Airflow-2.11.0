import os
import re
import json
import math
import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import pendulum

from airflow import DAG
from airflow.decorators import task

from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from google.auth.exceptions import RefreshError

# ——— Datas dinâmicas ———
hoje = datetime.now().date()
maio = "05/11/2025"
dt_maio = datetime.strptime(maio,'%m/%d/%Y')
START_DATE = (hoje - timedelta(days=10)).strftime('%m/%d/%Y')
END_DATE   = (hoje - timedelta(days=0)).strftime('%m/%d/%Y')

# ——— Config Streamline ———
STREAMLINE_URL   = "https://web.streamlinevrs.com/api/json"
TOKEN_KEY        = 'a43cb1b5ed27cce283ab2bb4df540037'
TOKEN_SECRET     = '72c7b8d5ba4b0ef14fe97a7e4179bdfa92cfc6ea'
SELECTED_COLUMNS = ['unit_id', 'unit_name', 'title', 'creation_date', 'id', 'processor_name', 'status_name']

# ——— Config Google Sheets ———
SCOPES           = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]

BASE_DIR = os.path.dirname(os.path.abspath(__file__))  # /opt/airflow/dags
CREDENTIALS_FILE = os.path.join(BASE_DIR, 'Tokens', 'credentials.json')
TOKEN_FILE       = os.path.join(BASE_DIR, 'Tokens', 'sheets_token.json')

# Planilha de destino e intervalo para leitura
DEST_SS_ID       = '1uCbvQz-PeLhFmT7Ah1IxaHyqMVwrly0ddmwlRi5i3xo'
DEST_SHEET_NAME  = 'TESTE1'
DEST_READ_RANGE  = f'{DEST_SHEET_NAME}!A:I'


def _do_interactive_login() -> Credentials:
    """
    Executa o fluxo OAuth local, garantindo refresh_token.
    """
    flow = InstalledAppFlow.from_client_secrets_file(CREDENTIALS_FILE, SCOPES)
    # Garante refresh_token mesmo se já houver consentimento anterior
    creds = flow.run_local_server(port=0, access_type='offline', prompt='consent')
    with open(TOKEN_FILE, 'w') as f:
        f.write(creds.to_json())
    return creds


def get_credentials() -> Credentials:
    """
    Lê o token salvo; se expirado tenta refresh.
    Em caso de RefreshError (inclui 'deleted_client', 'invalid_grant'), apaga o token e refaz o login.
    """
    creds = None

    # Carrega token, se existir
    if os.path.exists(TOKEN_FILE):
        try:
            creds = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)
        except Exception:
            # Token corrompido
            try:
                os.remove(TOKEN_FILE)
            except Exception:
                pass
            creds = None

    # Se não há credenciais válidas, executa login interativo
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            try:
                creds.refresh(Request())
                # Salva novamente (pode atualizar expiry/token)
                with open(TOKEN_FILE, 'w') as f:
                    f.write(creds.to_json())
                return creds
            except RefreshError as e:
                # Trata 'deleted_client', 'invalid_grant', etc.: apaga token e refaz login
                if os.path.exists(TOKEN_FILE):
                    try:
                        os.remove(TOKEN_FILE)
                    except Exception:
                        pass
                return _do_interactive_login()
        else:
            # Sem token/refresh_token: login interativo
            return _do_interactive_login()

    return creds


def get_sheets_service():
    return build('sheets', 'v4', credentials=get_credentials())


def get_last_row_column_a(svc, ss_id: str, sheet_name: str) -> int:
    """
    Encontra a última linha com dados na coluna A especificamente.
    Retorna o número da linha (1-indexed).
    """
    try:
        # Busca dados apenas da coluna A
        result = svc.spreadsheets().values().get(
            spreadsheetId=ss_id,
            range=f'{sheet_name}!A:A',
            valueRenderOption='UNFORMATTED_VALUE'
        ).execute()
        
        values = result.get('values', [])
        
        if not values:
            print("Coluna A está vazia")
            return 0
        
        # Procura a última linha com dados na coluna A
        last_row_with_data = 0
        for i, row in enumerate(values):
            if row and len(row) > 0 and str(row[0]).strip():  # Verifica se a célula A não está vazia
                last_row_with_data = i + 1
        
        print(f"Última linha com dados na coluna A: {last_row_with_data}")
        return last_row_with_data
        
    except Exception as e:
        print(f"Erro ao verificar coluna A: {e}")
        return 0


def clean_and_df() -> pd.DataFrame:
    status_ids = list(range(1, 8))   # [1,2,3,4,5,6,7]
    if 3 in status_ids:
        status_ids.remove(3)         # agora: [1,2,4,5,6,7]
    dfs = []

    for status in status_ids:
        payload = {
            "methodName": "GetWorkOrders",
            "params": {
                "token_key": TOKEN_KEY,
                "token_secret": TOKEN_SECRET,
                "show_labor_amount_charged_to_owner_guest": 1,
                "show_owner_description": 1,
                "date_type": "creation_date",
                "status_id": status,
                "start_date": START_DATE,
                "end_date": END_DATE
            }
        }
        resp = requests.post(STREAMLINE_URL, json=payload, headers={"Content-Type": "application/json"})
        resp.raise_for_status()
        json_resp = resp.json()

        try:
            maint_list = json_resp["data"]["maintenances"]["maintenance"]
        except Exception:
            maint_list = json_resp if isinstance(json_resp, list) else []

        if maint_list:
            df_status = pd.json_normalize(maint_list)
            cols = [c for c in SELECTED_COLUMNS if c in df_status.columns]
            if cols:
                dfs.append(df_status[cols])

    if not dfs:
        return pd.DataFrame(columns=SELECTED_COLUMNS)

    df = pd.concat(dfs, ignore_index=True).drop_duplicates()

    df['title'] = (
        df['title']
          .astype(str)
          .str.replace(r'[\r\n]+', ' ', regex=True)
          .str.replace(r'\s+', ' ', regex=True)
          .str.strip()
    )

    unwanted = [
        "CHECKLIST", "turn pool", "Manutenção Geral",
        "Abrir owner closet", "Fotografar e trancar owner closet", "DOOR CODE"
    ]
    pattern = "|".join(map(re.escape, unwanted))
    df = df[~df['title'].str.contains(pattern, case=False, na=False)]
    df = df[df['unit_name'].astype(str).str.strip() != ""]

    df['creation_date'] = (
        df['creation_date']
          .astype(str)
          .str.replace(r'\s?[A-Z]{2,4}$', '', regex=True)
    )
    df['creation_date'] = pd.to_datetime(df['creation_date'], errors='coerce')

    return df


def filter_existing(df: pd.DataFrame) -> pd.DataFrame:
    svc = get_sheets_service()
    result = svc.spreadsheets().values().get(
        spreadsheetId=DEST_SS_ID,
        range=f'{DEST_SHEET_NAME}!G:G'
    ).execute()
    vals = result.get('values', [])
    existing_ids = {row[0] for row in vals if row}
    return df[~df['id'].astype(str).isin(existing_ids)].reset_index(drop=True)


def build_append_rows(df: pd.DataFrame) -> list:
    rows = []
    mapping = {
        'inspeção': 'VIRGINIA',
        'guest in': 'GUEST',
        'edy': 'EDY',
        'limpeza': 'LIMPEZA',
        'manutenção': 'MANUTENÇÃO',
        'ale': 'ALE',
        'owner': 'OWNER'
    }

    for _, row in df.iterrows():
        unit_name = row.get('unit_name') or ''
        desc      = row.get('title') or ''
        date_val  = row.get('creation_date')
        date_str  = '' if pd.isna(date_val) else date_val.strftime('%Y-%m-%d %H:%M:%S')
        id_val    = row.get('id')
        id_str    = '' if pd.isna(id_val) else str(int(id_val)) if str(id_val).isdigit() else str(id_val)
        processor_name  = row.get('processor_name') or ''
        status_name     = row.get('status_name') or ''

        match = re.search(r'\(([^)]+)\)\s*$', desc.strip(), flags=re.IGNORECASE)
        area = ''
        if match:
            key = match.group(1).lower()
            area = mapping.get(key, '')

        # Ajuste: fallback para "ALOHA Keity"
        if not area and processor_name == "ALOHA Keity":
            area = 'MANUTENÇÃO'

        mapped = [
            unit_name,
            desc,
            area,
            '',
            date_str,
            '',
            id_str,
            processor_name,
            status_name
        ]
        rows.append(mapped)
    return rows


def append_to_sheet(ss_id: str, start_row: int, values: list):
    svc = get_sheets_service()
    value_ranges = []
    for i, row in enumerate(values):
        row_number = start_row + i
        unit_name, desc, area, _, date_str, _, id_str, processor_name, status_name = row

        ranges = [
            {"range": f"{DEST_SHEET_NAME}!A{row_number}", "values": [[unit_name]]},
            {"range": f"{DEST_SHEET_NAME}!B{row_number}", "values": [[desc]]},
        ]
        if area:
            ranges.append({"range": f"{DEST_SHEET_NAME}!C{row_number}", "values": [[area]]})

        ranges.extend([
            {"range": f"{DEST_SHEET_NAME}!E{row_number}", "values": [[date_str]]},
            {"range": f"{DEST_SHEET_NAME}!G{row_number}", "values": [[id_str]]},
            {"range": f"{DEST_SHEET_NAME}!H{row_number}", "values": [[processor_name]]},
            {"range": f"{DEST_SHEET_NAME}!I{row_number}", "values": [[status_name]]},
        ])

        value_ranges.extend(ranges)

    body = {"valueInputOption": "RAW", "data": value_ranges}
    svc.spreadsheets().values().batchUpdate(
        spreadsheetId=ss_id,
        body=body
    ).execute()


def main():
    df = clean_and_df()
    df = filter_existing(df)
    print(f"{len(df)} novas linhas para inserir.")

    if df.empty:
        print("Nada a inserir. Encerrando.")
        return

    svc = get_sheets_service()
    
    # Verifica a última linha com dados na coluna A
    last_row_with_data = get_last_row_column_a(svc, DEST_SS_ID, DEST_SHEET_NAME)
    
    start_row = last_row_with_data + 1
    print(f"Iniciando inserção na linha: {start_row}")

    to_append = build_append_rows(df)
    append_to_sheet(DEST_SS_ID, start_row, to_append)
    print(f"Inseridas {len(to_append)} linhas a partir da linha {start_row} em '{DEST_SHEET_NAME}'.")


if __name__ == "__main__":
    main()


SP_TZ = pendulum.timezone("America/Sao_Paulo")

with DAG(
    dag_id="WorkOrders",  # Nome da dag (task/tarefa)
    start_date=pendulum.datetime(2025, 9, 23, 0, 0, tz=SP_TZ),
    schedule="0 8,18 * * *",
    catchup=False,
    tags=["WorkOrders - OVH"],
) as dag:

    @task()
    def run_main():
        main()  # executa seu fluxo exatamente como está definido acima

    run_main()
