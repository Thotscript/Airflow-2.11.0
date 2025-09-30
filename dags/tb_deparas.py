import os
import pandas as pd
import numpy as np
import mysql.connector
from datetime import datetime, timedelta
import requests
import json
import time

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

# Configuração MySQL
MYSQL_CONFIG = {
    'host': 'host.docker.internal',
    'user': 'root',
    'password': 'Tfl1234@',
    'database': 'ovh_silver'
}

# Planilha Google Sheets
PLANILHA_ID = '1eOzcWWMSbO49ACaGuSpLd4ZcHcGRwDvMuZ4x21kxsM0'

# Nomes das abas
MAKE_TYPE = 'make_type'
CANAIS = 'canais'
NON_RENTING = 'non-renting'
STATUS_VENDA = 'status_venda'
ADMINISTRADORA = 'administradora'
DIA_AJUSTADO = 'dia_ajustado'

# Config Streamline
STREAMLINE_URL = "https://web.streamlinevrs.com/api/json"
SELECTED_COLUMNS = ['id', 'name', 'location_name', 'bedrooms_number', 'bathrooms_number', 
                   'condo_type_name', 'condo_type_group_name', 'max_adults', 'renting_type']

# Config Google Sheets OAuth
SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]

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
        # Se for replace, dropa a tabela existente
        if if_exists == 'replace':
            cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
            
            # Cria a tabela com base nas colunas do DataFrame
            columns_def = []
            for col in df.columns:
                # Define o tipo baseado no dtype do pandas
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
        
        # Insere os dados
        cols = ', '.join([f"`{col}`" for col in df.columns])
        placeholders = ', '.join(['%s'] * len(df.columns))
        insert_sql = f"INSERT INTO {table_name} ({cols}) VALUES ({placeholders})"
        
        # Converte DataFrame para lista de tuplas
        data = [tuple(row) for row in df.replace({np.nan: None}).values]
        
        # Insere em lote
        cursor.executemany(insert_sql, data)
        conn.commit()
        
        print(f"✓ {len(df)} linhas inseridas na tabela '{table_name}' ({if_exists})")
        
    except mysql.connector.Error as e:
        print(f"✗ Erro ao salvar tabela '{table_name}': {str(e)}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()


def execute_sql(sql_query: str):
    """Executa query SQL no MySQL"""
    conn = get_mysql_connection()
    cursor = conn.cursor()
    
    try:
        # Divide queries múltiplas por ponto-e-vírgula
        queries = [q.strip() for q in sql_query.split(';') if q.strip()]
        
        for query in queries:
            cursor.execute(query)
        
        conn.commit()
        print("✓ Query SQL executada com sucesso")
        
    except mysql.connector.Error as e:
        print(f"✗ Erro ao executar SQL: {str(e)}")
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


def read_sheet_to_df(sheet_name: str, spreadsheet_id: str = PLANILHA_ID) -> pd.DataFrame:
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


def load_all_sheets() -> dict:
    sheet_names = [MAKE_TYPE, CANAIS, NON_RENTING, STATUS_VENDA, ADMINISTRADORA, DIA_AJUSTADO]
    dataframes = {}
    
    print(f"\n{'='*60}")
    print(f"Carregando planilha: {PLANILHA_ID}")
    print(f"{'='*60}\n")
    
    for sheet_name in sheet_names:
        df = read_sheet_to_df(sheet_name)
        dataframes[sheet_name] = df
    
    print(f"{'='*60}")
    print(f"✓ Total de abas carregadas: {len(dataframes)}")
    print(f"{'='*60}\n")
    
    return dataframes

# ============================================================================
# FUNÇÕES DE PROCESSAMENTO
# ============================================================================

def get_property_data_from_streamline(df_administradora: pd.DataFrame) -> pd.DataFrame:
    """
    Busca dados de propriedades da API Streamline
    """
    print("\n📡 Buscando dados do Streamline...")
    
    now = datetime.now().strftime('%Y/%m/%d %H:%M:%S')
    headers = {"Content-Type": "application/json"}
    property_list_total = pd.DataFrame()
    
    for _, row in df_administradora.iterrows():
        try:
            data = {
                "methodName": "GetPropertyListWordPress",
                "params": {
                    "token_key": row["token_key"],
                    "token_secret": row["token_secret"]
                }
            }
            
            response = requests.post(STREAMLINE_URL, data=json.dumps(data), headers=headers)
            data_dict = json.loads(response.content)
            
            if 'data' in data_dict and 'property' in data_dict['data']:
                property_list = pd.json_normalize(data_dict["data"]["property"])
                property_list = property_list.loc[:, SELECTED_COLUMNS]
                property_list['Administradora'] = row["Administradora"]
                property_list['Data_Carga'] = now
                property_list_total = pd.concat([property_list_total, property_list], ignore_index=True)
            
            time.sleep(0.5)  # Evita sobrecarga na API
            
        except Exception as e:
            print(f"⚠️  Erro ao buscar dados da administradora {row.get('Administradora', 'N/A')}: {str(e)}")
    
    print(f"✓ Total de propriedades coletadas: {len(property_list_total)}")
    return property_list_total


def create_calendario(df_dia_ajustado: pd.DataFrame) -> pd.DataFrame:
    """
    Cria calendário com flags YTD, MTD, L7D, SEMANA
    """
    print("\n📅 Criando calendário...")
    
    start_date = datetime(2018, 1, 1)
    end_date = datetime.now().replace(month=12, day=31) + timedelta(days=365*3)
    date_range = pd.date_range(start=start_date, end=end_date, freq="D")
    
    calendario = pd.DataFrame({'data': date_range})
    calendario['ano'] = calendario['data'].dt.year
    calendario['mês'] = calendario['data'].dt.month
    calendario['AnoMes'] = calendario['ano'] * 100 + calendario['mês']
    
    data_ajustada = datetime.now().date()
    calendario['Ano Comparação'] = 'Y-' + (data_ajustada.year - calendario['ano']).astype(str)
    calendario['Hoje'] = data_ajustada
    
    # Formata data para merge ANTES de fazer o merge
    calendario['data_formatada'] = calendario['data'].dt.strftime('%d-%m-%Y')
    
    # Realiza o merge com dia_ajustado
    if not df_dia_ajustado.empty and 'data' in df_dia_ajustado.columns:
        # Renomeia a coluna 'data' de df_dia_ajustado para evitar conflito
        df_dia_ajustado_copy = df_dia_ajustado.copy()
        df_dia_ajustado_copy.rename(columns={'data': 'data_ajustada_str'}, inplace=True)
        
        calendario = pd.merge(
            calendario, 
            df_dia_ajustado_copy, 
            left_on='data_formatada', 
            right_on='data_ajustada_str', 
            how='left'
        )
        
        # Remove colunas auxiliares
        calendario.drop(['data_formatada', 'data_ajustada_str'], axis=1, inplace=True, errors='ignore')
    else:
        calendario.drop('data_formatada', axis=1, inplace=True, errors='ignore')
    
    # Calcula flags
    def calculate_flags(row):
        hoje = pd.to_datetime(row["Hoje"])
        dia_ajustado = pd.to_datetime(row.get("Dia_Ajustado", row["Hoje"]))
        
        row["YTD"] = "YTD" if dia_ajustado < hoje else "YTG"
        row["MTD"] = "MTD" if dia_ajustado < hoje and hoje.month == dia_ajustado.month else "MTG"
        
        days_diff = (hoje - dia_ajustado).days
        row["L7D"] = "L7D" if 0 < days_diff <= 7 else "-"
        row["SEMANA"] = "S" if 0 < days_diff <= 7 else "S-1" if 7 < days_diff <= 14 else "-"
        
        return row
    
    calendario = calendario.apply(calculate_flags, axis=1)
    
    # Formata data final - AGORA a coluna 'data' ainda existe
    calendario['data'] = calendario['data'].dt.strftime('%d-%m-%Y')
    
    print(f"✓ Calendário criado: {len(calendario)} dias")
    return calendario


def create_depara_casas():
    """
    Cria a tabela tb_depara_casas no MySQL
    """
    print("\n🔗 Criando tabela de depara de casas...")
    
    sql_query = """
    DROP TABLE IF EXISTS tb_depara_casas;
    
    CREATE TABLE tb_depara_casas AS
    SELECT A.*, COALESCE(B.renting_type, 'NON-RENTING') AS renting_type 
    FROM (
        SELECT DISTINCT id, name, location_name, bedrooms_number, bathrooms_number, 
               condo_type_name, condo_type_group_name, Administradora
        FROM (
            SELECT id, name, location_name, bedrooms_number, bathrooms_number, 
                   condo_type_name, condo_type_group_name, renting_type, Administradora 
            FROM property_list_wordpress
            
            UNION
            
            SELECT id, 
                   MIN(name) AS name, 
                   MIN(location_name) AS location_name, 
                   MIN(bedrooms_number) AS bedrooms_number,
                   MIN(bathrooms_number) AS bathrooms_number,
                   MIN(condo_type_name) AS condo_type_name, 
                   MIN(condo_type_group_name) AS condo_type_group_name,
                   MIN(renting_type) AS renting_type,
                   MIN(Administradora) AS Administradora
            FROM (
                SELECT *, 
                       ROW_NUMBER() OVER (PARTITION BY id ORDER BY Data_Carga DESC) AS RowNum
                FROM property_list_wordpress_append
            ) AS SortedResults
            WHERE RowNum = 1
            GROUP BY id
            
            UNION 
            
            SELECT id, name, location_name, bedrooms_number, bathrooms_number, 
                   condo_type_name, condo_type_group_name, renting_type, Administradora 
            FROM tb_non_renting
        ) AS CombinedData
    ) AS A
    LEFT JOIN property_list_wordpress AS B ON A.id = B.id
    """
    
    execute_sql(sql_query)

# ============================================================================
# TASKS DO AIRFLOW
# ============================================================================

SP_TZ = pendulum.timezone("America/Sao_Paulo")

with DAG(
    dag_id="OVH-tb_deparas",
    start_date=pendulum.datetime(2025, 9, 23, 8, 0, tz=SP_TZ),
    schedule="0 5 * * *",
    catchup=False,
    tags=["Tabelas - OVH"],
) as dag:

    @task
    def extract_sheets_data():
        """Extrai dados do Google Sheets"""
        return load_all_sheets()


    @task
    def extract_streamline_data(dfs: dict):
        """Extrai dados do Streamline"""
        df_administradora = dfs[ADMINISTRADORA]
        property_list = get_property_data_from_streamline(df_administradora)
        return property_list


    @task
    def process_and_save_tables(dfs: dict, property_list: pd.DataFrame):
        """Processa e salva todas as tabelas no MySQL"""
        
        print("\n💾 Salvando tabelas no MySQL...")
        
        # 1. Salvar tabelas de depara do Google Sheets
        save_to_mysql(dfs[MAKE_TYPE], 'tb_make_type_id', if_exists='replace')
        save_to_mysql(dfs[STATUS_VENDA], 'tb_status_vendas', if_exists='replace')
        save_to_mysql(dfs[CANAIS], 'tb_depara_canais', if_exists='replace')
        save_to_mysql(dfs[NON_RENTING], 'tb_non_renting', if_exists='replace')
        
        # 2. Salvar dados do Streamline
        save_to_mysql(property_list, 'tb_property_list_wordpress', if_exists='replace')
        save_to_mysql(property_list, 'tb_property_list_wordpress_append', if_exists='append')
        
        # 3. Criar e salvar calendário
        calendario = create_calendario(dfs[DIA_AJUSTADO])
        save_to_mysql(calendario, 'tb_calendario', if_exists='replace')
        
        # 4. Criar tabela de depara de casas
        create_depara_casas()
        
        print("\n✅ Todas as tabelas foram processadas e salvas com sucesso!")

    sheets_data = extract_sheets_data()
    streamline_data = extract_streamline_data(sheets_data)
    process_and_save_tables(sheets_data, streamline_data)


# ============================================================================
# EXECUÇÃO DIRETA (FORA DO AIRFLOW)
# ============================================================================

if __name__ == "__main__":
    print("\n🚀 Iniciando processamento completo...\n")
    
    # 1. Carregar dados do Google Sheets
    dfs = load_all_sheets()
    
    # 2. Buscar dados do Streamline
    property_list = get_property_data_from_streamline(dfs[ADMINISTRADORA])
    
    # 3. Processar e salvar todas as tabelas
    process_and_save_tables(dfs, property_list)
    
    print("\n✅ Processamento concluído com sucesso!")