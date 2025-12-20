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
    """
    Extrai blocked_period do response - trata múltiplos formatos
    
    Formatos suportados:
    1. Lista direta: [{'startdate': ..., 'enddate': ...}, ...]
    2. Dict com Response.data.blocked_period
    3. Dict com data.blocked_period
    4. Dict com apenas status (sem reservas)
    """
    
    # Formato 1: Se data for uma lista, retorna ela diretamente
    if isinstance(data, list):
        return data
    
    # Se não for dict, retorna lista vazia
    if not isinstance(data, dict):
        return []
    
    # Formato 2: Tenta extrair de Response.data.blocked_period
    if "Response" in data:
        response_data = data.get("Response")
        if isinstance(response_data, dict):
            inner_data = response_data.get("data")
            if isinstance(inner_data, dict):
                blocked = inner_data.get("blocked_period", [])
                if blocked:
                    return blocked
    
    # Formato 3: Tenta extrair de data.blocked_period
    data_field = data.get("data")
    if isinstance(data_field, dict):
        blocked = data_field.get("blocked_period", [])
        if blocked:
            return blocked
    
    # Formato 4: Se data_field for lista, retorna ela
    if isinstance(data_field, list):
        return data_field
    
    # Formato 5: Apenas status (sem reservas)
    if "status" in data and not data_field:
        return []
    
    return []

def fetch_calendar(session: requests.Session, unit_id: int, startdate: str, enddate: str) -> list:
    """Busca o calendário de uma casa para um período específico"""
    payload = make_payload_calendar(unit_id, startdate, enddate)
    headers = {"Content-Type": "application/json"}
    
    try:
        resp = session.post(API_URL, json=payload, headers=headers, timeout=(10, 40))
        
        resp.raise_for_status()
        data = resp.json()
        
        # DEBUG DETALHADO
        print(f"[DEBUG] unit_id={unit_id}, período={startdate} a {enddate}")
        print(f"[DEBUG] Response type: {type(data)}")
        
        if isinstance(data, list):
            print(f"[DEBUG] É uma lista com {len(data)} itens")
            return data
        
        if isinstance(data, dict):
            print(f"[DEBUG] É um dict com keys: {list(data.keys())}")
        
        if isinstance(data, dict) and data.get("error"):
            print(f"[WARN] API erro para unit_id={unit_id}: {data.get('error')}")
            return []
        
        blocked = extract_blocked_period(data)
        print(f"[DEBUG] extract_blocked_period retornou: {len(blocked)} itens")
        
        if blocked and len(blocked) > 0:
            print(f"[DEBUG] Primeira reserva: {blocked[0]}")
        
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
    # Mês alvo (derivado do payload)
    mes_obj = datetime.strptime(startdate, "%m/%d/%Y")
    
    # Primeiro e último dia do mês alvo
    first_day_month = mes_obj
    if mes_obj.month == 12:
        last_day_month = datetime(mes_obj.year + 1, 1, 1) - timedelta(days=1)
    else:
        last_day_month = datetime(mes_obj.year, mes_obj.month + 1, 1) - timedelta(days=1)
    
    dias_no_mes = last_day_month.day
    
    if not blocked:
        return (0.0, 0, dias_no_mes)
    
    # DEBUG: mostrar quantas reservas estão sendo processadas
    print(f"[DEBUG calculate_occupancy] Processando {len(blocked)} reservas para {mes_obj.strftime('%m/%Y')}")
    
    # Usar set para evitar contagem duplicada de dias
    dias_ocupados_set = set()
    
    for b in blocked:
        try:
            start = datetime.strptime(b["startdate"], "%m/%d/%Y")
            end = datetime.strptime(b["enddate"], "%m/%d/%Y")
            
            # Verifica se há sobreposição com o mês alvo
            if end < first_day_month or start > last_day_month:
                continue
            
            # Calcula a interseção da reserva com o mês
            # IMPORTANTE: Garante que não ultrapasse o último dia do mês
            overlap_start = max(start, first_day_month)
            overlap_end = min(end, last_day_month)
            
            # Adiciona cada dia ocupado ao set (apenas dentro do mês)
            current_day = overlap_start
            while current_day <= overlap_end:
                # Segurança extra: só adiciona se estiver dentro do mês
                if first_day_month <= current_day <= last_day_month:
                    dias_ocupados_set.add(current_day.date())
                current_day += timedelta(days=1)
        
        except Exception as e:
            print(f"[WARN] Erro ao processar bloqueio {b}: {e}")
            continue
    
    dias_ocupados = len(dias_ocupados_set)
    
    # Garantir que nunca ultrapasse 100%
    dias_ocupados = min(dias_ocupados, dias_no_mes)
    ocupacao = dias_ocupados / dias_no_mes
    
    return (ocupacao, dias_ocupados, dias_no_mes)

# =========================
# Buscar casas ativas
# =========================
def get_active_houses():
    """Busca todas as casas ativas com renting_type = 'RENTING'"""
    conn = mysql.connector.connect(**DB_CFG)
    try:
        query = """
            SELECT id 
            FROM ovh_silver.tb_active_houses 
            WHERE renting_type = 'RENTING'
        """
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
            
            # IMPORTANTE: Buscar dados de 90 dias antes até 90 dias depois
            # para capturar reservas que começam antes ou terminam depois do mês alvo
            mes_obj = datetime.strptime(startdate, "%m/%d/%Y")
            fetch_start = mes_obj - timedelta(days=90)
            fetch_end_obj = datetime.strptime(enddate, "%m/%d/%Y") + timedelta(days=90)
            
            fetch_startdate = fetch_start.strftime("%m/%d/%Y")
            fetch_enddate = fetch_end_obj.strftime("%m/%d/%Y")
            
            # Buscar calendário com range expandido
            blocked = fetch_calendar(session, unit_id, fetch_startdate, fetch_enddate)
            
            # Calcular ocupação para o mês específico
            occupancy_rate, days_occupied, days_in_month = calculate_occupancy(blocked, startdate)
            
            # Log detalhado para debug
            if len(blocked) > 0 and days_occupied == 0:
                print(f"[ALERTA] Casa {unit_id} tem {len(blocked)} reservas mas ocupação zerada em {month_info['month_str']}")
                for idx, b in enumerate(blocked[:3], 1):  # Mostra até 3 reservas
                    print(f"  Reserva {idx}: {b['startdate']} até {b['enddate']}")
            
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
            
            print(f"  Casa {unit_id} | {month_info['month_str']} | Ocupação: {occupancy_rate:.2%} ({days_occupied}/{days_in_month} dias)")
    
    # Criar DataFrame e salvar
    df_results = pd.DataFrame(results)
    print(f"\n=== Resumo Final ===")
    print(f"Total de registros: {len(df_results)}")
    print(f"Ocupação média 2026: {df_results['occupancy_rate'].mean():.2%}")
    print(f"\nOcupação por mês:")
    monthly_summary = df_results.groupby('month_str').agg({
        'occupancy_rate': 'mean',
        'days_occupied': 'mean'
    })
    for month, row in monthly_summary.iterrows():
        print(f"  {month}: {row['occupancy_rate']:.2%} (média {row['days_occupied']:.1f} dias)")
    
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