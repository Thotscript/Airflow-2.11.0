import os
import json
import re
import numpy as np
import pandas as pd
import requests
from requests.adapters import HTTPAdapter, Retry
import mysql.connector
import pendulum
from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowException
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

API_URL = "https://web.streamlinevrs.com/api/json"
TOKEN_KEY = "3ef223d3bbf7086cfb86df7e98d6e5d2"
TOKEN_SECRET = "a88d05b895affb815cc8a4d96670698ee486ea30"

DB_CFG = dict(
    host="host.docker.internal",
    user="root",
    password="Tfl1234@",
    database="ovh_bronze",
)

TB_NAME = "tb_occupancy_houses"

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

def make_payload_calendar(unit_id: int, startdate: str, enddate: str):
    """Payload para GetPropertyAvailabilityCalendarRawData"""
    return {
        "methodName": "GetPropertyAvailabilityCalendarRawData",
        "params": {
            "token_key": TOKEN_KEY,
            "token_secret": TOKEN_SECRET,
            "unit_id": unit_id,
            "startdate": startdate,
            "enddate": enddate,
        },
    }

def extract_blocked_period(data):
    """Extrai blocked_period do response"""
    if "Response" in data:
        return data["Response"]["data"].get("blocked_period", [])
    return data.get("data", {}).get("blocked_period", [])

def fetch_calendar(session: requests.Session, unit_id: int, startdate: str, enddate: str) -> list:
    """Busca o calendário de uma casa para um período específico"""
    payload = make_payload_calendar(unit_id, startdate, enddate)
    headers = {"Content-Type": "application/json"}
    
    try:
        resp = session.post(API_URL, json=payload, headers=headers, timeout=(10, 40))
        print(f"[Calendar] unit_id={unit_id}, {startdate} a {enddate} | Status: {resp.status_code}")
        
        resp.raise_for_status()
        data = resp.json()
        
        if isinstance(data, dict) and data.get("error"):
            print(f"[WARN] API erro para unit_id={unit_id}: {data.get('error')}")
            return []
        
        blocked = extract_blocked_period(data)
        return blocked if blocked else []
    
    except requests.HTTPError as e:
        print(f"[ERROR] HTTP error para unit_id={unit_id}: {e}")
        return []
    except ValueError as e:
        print(f"[ERROR] JSON parse error para unit_id={unit_id}: {e}")
        return []
    except Exception as e:
        print(f"[ERROR] Erro inesperado para unit_id={unit_id}: {e}")
        return []

def calculate_occupancy(blocked: list, startdate: str) -> tuple:
    """
    Calcula a ocupação de um mês específico baseado nas reservas bloqueadas
    
    Args:
        blocked: Lista de períodos bloqueados do GetPropertyAvailabilityCalendarRawData
        startdate: Data no formato MM/DD/YYYY (primeiro dia do mês)
    
    Returns:
        tuple: (taxa_ocupacao, dias_ocupados, dias_no_mes)
    """
    if not blocked:
        # Calcular dias no mês mesmo sem reservas
        mes_obj = datetime.strptime(startdate, "%m/%d/%Y")
        if mes_obj.month == 12:
            last_day_month = datetime(mes_obj.year + 1, 1, 1) - timedelta(days=1)
        else:
            last_day_month = datetime(mes_obj.year, mes_obj.month + 1, 1) - timedelta(days=1)
        dias_no_mes = last_day_month.day
        return (0.0, 0, dias_no_mes)
    
    # Mês alvo (derivado do payload)
    mes_alvo = datetime.strptime(startdate, "%m/%d/%Y").strftime("%Y-%m")
    mes_obj = datetime.strptime(startdate, "%m/%d/%Y")
    
    # Último dia do mês alvo
    if mes_obj.month == 12:
        last_day_month = datetime(mes_obj.year + 1, 1, 1) - timedelta(days=1)
    else:
        last_day_month = datetime(mes_obj.year, mes_obj.month + 1, 1) - timedelta(days=1)
    
    dias_no_mes = last_day_month.day
    dias_ocupados = 0
    
    for b in blocked:
        try:
            start = datetime.strptime(b["startdate"], "%m/%d/%Y")
            end = datetime.strptime(b["enddate"], "%m/%d/%Y")
            
            # IGNORA reservas que não começam no mês alvo
            if start.strftime("%Y-%m") != mes_alvo:
                continue
            
            # Limita ao mês
            end_limited = min(end, last_day_month)
            dias = (end_limited - start).days + 1
            dias = max(dias, 0)
            dias_ocupados += dias
        
        except Exception as e:
            print(f"[WARN] Erro ao processar bloqueio: {e}")
            continue
    
    ocupacao = min(dias_ocupados / dias_no_mes, 1.0)
    return (ocupacao, dias_ocupados, dias_no_mes)

# =========================
# Buscar casas ativas
# =========================
def get_active_houses():
    """Busca todas as casas ativas"""
    conn = mysql.connector.connect(**DB_CFG)
    try:
        # Primeiro tenta tb_active_houses
        cur = conn.cursor()
        cur.execute("""
            SELECT COUNT(*) FROM information_schema.tables 
            WHERE table_schema = DATABASE() 
            AND table_name = 'tb_active_houses'
        """)
        table_exists = cur.fetchone()[0]
        cur.close()
        
        if table_exists:
            query = """
                SELECT id 
                FROM ovh_silver.tb_active_houses 
                WHERE renting_type = 'RENTING'
            """
            print("Usando tabela: tb_active_houses")
        else:
            # Se não existir, busca da tb_reservas os unit_id únicos
            query = """
                SELECT DISTINCT unit_id as id
                FROM tb_reservas
                WHERE unit_id IS NOT NULL
                ORDER BY unit_id
            """
            print("Usando tabela: tb_reservas (unit_id únicos)")
        
        df = pd.read_sql(query, conn)
        print(f"Total de casas ativas encontradas: {len(df)}")
        return df['id'].tolist()
    finally:
        conn.close()

# =========================
# Criar tabela de ocupação
# =========================
def create_occupancy_table():
    """Cria a tabela tb_occupancy_houses se não existir"""
    conn = mysql.connector.connect(**DB_CFG)
    try:
        cur = conn.cursor()
        create_sql = f"""
        CREATE TABLE IF NOT EXISTS `{TB_NAME}` (
            `unit_id` BIGINT NOT NULL,
            `year` INT NOT NULL,
            `month` INT NOT NULL,
            `month_str` VARCHAR(7) NOT NULL,
            `occupancy_rate` DECIMAL(5,4) NULL,
            `days_occupied` INT NULL,
            `days_in_month` INT NULL,
            `extraction_date` DATETIME NOT NULL,
            PRIMARY KEY (`unit_id`, `year`, `month`),
            INDEX `idx_month` (`year`, `month`),
            INDEX `idx_unit` (`unit_id`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """
        cur.execute(create_sql)
        conn.commit()
        print(f"Tabela `{TB_NAME}` verificada/criada com sucesso.")
    finally:
        try:
            cur.close()
        except Exception:
            pass
        conn.close()

# =========================
# Salvar dados de ocupação
# =========================
def save_occupancy_data(df: pd.DataFrame):
    """Salva os dados de ocupação no banco usando REPLACE INTO"""
    if df.empty:
        print("DataFrame vazio, nada para salvar.")
        return
    
    conn = mysql.connector.connect(**DB_CFG)
    try:
        cur = conn.cursor()
        
        insert_sql = f"""
        REPLACE INTO `{TB_NAME}` 
        (unit_id, year, month, month_str, occupancy_rate, days_occupied, days_in_month, extraction_date)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        batch_size = 1000
        total = len(df)
        total_inserted = 0
        
        for i in range(0, total, batch_size):
            batch = df.iloc[i:i+batch_size]
            cur.executemany(insert_sql, [tuple(r) for r in batch.itertuples(index=False, name=None)])
            conn.commit()
            total_inserted += len(batch)
            print(f"Inseridas/Atualizadas {total_inserted}/{total} linhas")
        
        print(f"Tabela `{TB_NAME}` atualizada com {total} registros.")
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
    # Criar tabela se não existir
    create_occupancy_table()
    
    # Buscar casas ativas
    unit_ids = get_active_houses()
    
    if not unit_ids:
        print("Nenhuma casa ativa encontrada.")
        return
    
    # Gerar lista de meses de 2026
    months_2026 = []
    for month in range(1, 13):
        # Primeiro dia do mês
        startdate = f"{month:02d}/01/2026"
        
        # Último dia do mês
        if month == 12:
            last_day = 31
        else:
            next_month = datetime(2026, month + 1, 1)
            last_day = (next_month - timedelta(days=1)).day
        
        enddate = f"{month:02d}/{last_day}/2026"
        
        months_2026.append({
            'startdate': startdate,
            'enddate': enddate,
            'year': 2026,
            'month': month,
            'month_str': f"2026-{month:02d}"
        })
    
    print(f"Processando {len(unit_ids)} casas para {len(months_2026)} meses de 2026...")
    
    session = build_session()
    results = []
    extraction_date = datetime.now()
    
    total_requests = len(unit_ids) * len(months_2026)
    processed = 0
    
    for unit_id in unit_ids:
        print(f"\n=== Processando unit_id: {unit_id} ===")
        
        for month_info in months_2026:
            startdate = month_info['startdate']
            enddate = month_info['enddate']
            
            # Buscar calendário do mês completo
            blocked = fetch_calendar(session, unit_id, startdate, enddate)
            
            # Calcular ocupação
            occupancy_rate, days_occupied, days_in_month = calculate_occupancy(blocked, startdate)
            
            results.append({
                'unit_id': unit_id,
                'year': month_info['year'],
                'month': month_info['month'],
                'month_str': month_info['month_str'],
                'occupancy_rate': round(occupancy_rate, 4),
                'days_occupied': days_occupied,
                'days_in_month': days_in_month,
                'extraction_date': extraction_date
            })
            
            processed += 1
            if processed % 10 == 0:
                print(f"Progresso: {processed}/{total_requests} requisições processadas ({processed/total_requests*100:.1f}%)")
            
            print(f"  Casa {unit_id} | {month_info['month_str']} | Ocupação: {occupancy_rate:.2%}")
    
    # Criar DataFrame e salvar
    df_results = pd.DataFrame(results)
    print(f"\n=== Resumo Final ===")
    print(f"Total de registros: {len(df_results)}")
    print(f"Ocupação média 2026: {df_results['occupancy_rate'].mean():.2%}")
    print(f"\nOcupação por mês:")
    print(df_results.groupby('month_str')['occupancy_rate'].mean().apply(lambda x: f"{x:.2%}"))
    
    save_occupancy_data(df_results)
    print("\n✓ Processamento concluído com sucesso!")

# === Airflow ===
SP_TZ = pendulum.timezone("America/Sao_Paulo")

with DAG(
    dag_id="OVH-tb_occupancy_houses",
    start_date=pendulum.datetime(2025, 12, 19, 8, 0, tz=SP_TZ),
    schedule="0 2 * * *",  # Roda às 2h da manhã todos os dias
    catchup=False,
    tags=["Tabelas - OVH", "Ocupacao"],
) as dag:

    @task(retries=4, retry_exponential_backoff=True)
    def calculate_occupancy_2026():
        main()

    calculate_occupancy_2026()