import os
import pandas as pd
import numpy as np
import mysql.connector

from airflow import DAG
from airflow.decorators import task
import pendulum

from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from google.auth.exceptions import RefreshError

# ============================================================================
# CONFIGURAÇÕES
# ============================================================================

# Configuração MySQL (mesmas credenciais do projeto)
MYSQL_CONFIG = {
    'host': 'host.docker.internal',
    'user': 'root',
    'password': 'Tfl1234@',
    'database': 'ovh_silver'
}

# Nova planilha
PLANILHA_ID = '1fzhazYMG8SlEV1-tcaWC8ZgaXKFcb1psFkhDPHaZWfg'
ABA_METAS_BI = 'Metas BI'
TABELA_DESTINO = 'tb_metas_bi'

# Config Google Sheets OAuth
SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]

# Aponta para os mesmos tokens do projeto original
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CREDENTIALS_FILE = os.path.join(BASE_DIR, 'Tokens', 'credentials.json')
TOKEN_FILE = os.path.join(BASE_DIR, 'Tokens', 'sheets_token.json')

# ============================================================================
# FUNÇÕES MYSQL
# ============================================================================

def get_mysql_connection():
    """Cria e retorna uma conexão MySQL"""
    return mysql.connector.connect(**MYSQL_CONFIG)


def save_to_mysql(df: pd.DataFrame, table_name: str, if_exists: str = 'replace'):
    """
    Salva DataFrame no MySQL usando mysql.connector

    Args:
        df: DataFrame a ser salvo
        table_name: Nome da tabela
        if_exists: 'replace' ou 'append'
    """
    if df.empty:
        print(f"⚠️  DataFrame vazio, tabela '{table_name}' não foi criada")
        return

    conn = get_mysql_connection()
    cursor = conn.cursor()

    try:
        if if_exists == 'replace':
            cursor.execute(f"DROP TABLE IF EXISTS {table_name}")

            columns_def = []
            for col in df.columns:
                dtype = df[col].dtype
                if dtype == 'int64':
                    col_type = 'BIGINT'
                elif dtype == 'float64':
                    col_type = 'DOUBLE'
                elif dtype == 'datetime64[ns]':
                    col_type = 'DATETIME'
                elif dtype == 'bool':
                    col_type = 'BOOLEAN'
                else:
                    col_type = 'TEXT'

                columns_def.append(f"`{col}` {col_type}")

            create_table_sql = f"CREATE TABLE {table_name} ({', '.join(columns_def)})"
            cursor.execute(create_table_sql)
            print(f"✓ Tabela '{table_name}' criada")

        cols = ', '.join([f"`{col}`" for col in df.columns])
        placeholders = ', '.join(['%s'] * len(df.columns))
        insert_sql = f"INSERT INTO {table_name} ({cols}) VALUES ({placeholders})"

        data = [tuple(row) for row in df.replace({np.nan: None}).values]
        cursor.executemany(insert_sql, data)
        conn.commit()

        print(f"✓ {len(df)} linhas inseridas na tabela '{table_name}' ({if_exists})")

    except mysql.connector.Error as e:
        print(f"✗ Erro ao salvar tabela '{table_name}': {str(e)}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()


# ============================================================================
# FUNÇÕES GOOGLE SHEETS
# ============================================================================

def _do_interactive_login() -> Credentials:
    flow = InstalledAppFlow.from_client_secrets_file(CREDENTIALS_FILE, SCOPES)
    creds = flow.run_local_server(port=0, access_type='offline', prompt='consent')
    with open(TOKEN_FILE, 'w') as f:
        f.write(creds.to_json())
    return creds


def get_credentials() -> Credentials:
    creds = None
    if os.path.exists(TOKEN_FILE):
        try:
            creds = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)
        except Exception:
            try:
                os.remove(TOKEN_FILE)
            except Exception:
                pass
            creds = None

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            try:
                creds.refresh(Request())
                with open(TOKEN_FILE, 'w') as f:
                    f.write(creds.to_json())
                return creds
            except RefreshError:
                if os.path.exists(TOKEN_FILE):
                    try:
                        os.remove(TOKEN_FILE)
                    except Exception:
                        pass
                return _do_interactive_login()
        else:
            return _do_interactive_login()
    return creds


def get_sheets_service():
    return build('sheets', 'v4', credentials=get_credentials())


def read_sheet_to_df(sheet_name: str, spreadsheet_id: str) -> pd.DataFrame:
    service = get_sheets_service()
    try:
        result = service.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id,
            range=f"'{sheet_name}'!A:ZZ"
        ).execute()

        values = result.get('values', [])
        if not values:
            print(f"⚠️  Aba '{sheet_name}' está vazia")
            return pd.DataFrame()

        headers = values[0]
        data = values[1:] if len(values) > 1 else []

        max_cols = len(headers)
        normalized_data = []
        for row in data:
            normalized_row = row + [''] * (max_cols - len(row))
            normalized_data.append(normalized_row[:max_cols])

        df = pd.DataFrame(normalized_data, columns=headers)
        print(f"✓ Aba '{sheet_name}': {len(df)} linhas × {len(df.columns)} colunas")
        return df

    except Exception as e:
        print(f"✗ Erro ao ler aba '{sheet_name}': {str(e)}")
        return pd.DataFrame()


# ============================================================================
# TASKS DO AIRFLOW
# ============================================================================

SP_TZ = pendulum.timezone("America/Sao_Paulo")

with DAG(
    dag_id="OVH-tb_metas_bi",
    start_date=pendulum.datetime(2025, 9, 23, 0, 0, tz=SP_TZ),
    schedule="0 0 * * *",
    catchup=False,
    tags=["Tabelas - OVH"],
) as dag:

    @task
    def extract_metas_bi():
        """Lê a aba 'Metas BI' do Google Sheets"""
        print(f"📊 Lendo aba '{ABA_METAS_BI}' da planilha {PLANILHA_ID}...")
        df_metas = read_sheet_to_df(ABA_METAS_BI, PLANILHA_ID)

        if df_metas.empty:
            print("⚠️  Nenhum dado encontrado na aba 'Metas BI'.")
        else:
            print(f"\n📋 Preview dos dados:")
            print(df_metas.head())
            print(f"\nShape: {df_metas.shape}")
            print(f"Colunas: {list(df_metas.columns)}")

        return df_metas

    @task
    def load_metas_bi(df_metas: pd.DataFrame):
        """Salva os dados no MySQL com replace"""
        if df_metas.empty:
            print("❌ DataFrame vazio. Nenhum dado será salvo.")
            return

        print(f"\n💾 Salvando em '{TABELA_DESTINO}' no banco '{MYSQL_CONFIG['database']}'...")
        save_to_mysql(df_metas, TABELA_DESTINO, if_exists='replace')
        print(f"\n✅ Tabela '{TABELA_DESTINO}' atualizada com sucesso!")

    metas_data = extract_metas_bi()
    load_metas_bi(metas_data)


# ============================================================================
# EXECUÇÃO PRINCIPAL
# ============================================================================

def main():
    print("\n🚀 Iniciando carga da aba 'Metas BI'...\n")

    # 1. Lê a aba do Google Sheets
    print(f"📊 Lendo aba '{ABA_METAS_BI}' da planilha {PLANILHA_ID}...")
    df_metas = read_sheet_to_df(ABA_METAS_BI, PLANILHA_ID)

    if df_metas.empty:
        print("❌ Nenhum dado encontrado na aba 'Metas BI'. Encerrando.")
        return

    print(f"\n📋 Preview dos dados:")
    print(df_metas.head())
    print(f"\nShape: {df_metas.shape}")
    print(f"Colunas: {list(df_metas.columns)}")

    # 2. Salva no MySQL com replace
    print(f"\n💾 Salvando em '{TABELA_DESTINO}' no banco '{MYSQL_CONFIG['database']}'...")
    save_to_mysql(df_metas, TABELA_DESTINO, if_exists='replace')

    print(f"\n✅ Carga finalizada com sucesso! Tabela '{TABELA_DESTINO}' atualizada.")


if __name__ == "__main__":
    main()