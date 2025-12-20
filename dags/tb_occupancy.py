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
        print(f"[DEBUG] Formato: Lista direta com {len(data)} itens")
        return data
    
    # Se não for dict, retorna lista vazia
    if not isinstance(data, dict):
        print(f"[DEBUG] Formato inesperado: {type(data)}")
        return []
    
    # Formato 2: Tenta extrair de Response.data.blocked_period
    if "Response" in data:
        response_data = data.get("Response")
        if isinstance(response_data, dict):
            inner_data = response_data.get("data")
            if isinstance(inner_data, dict):
                blocked = inner_data.get("blocked_period", [])
                if blocked:
                    print(f"[DEBUG] Formato: Response.data.blocked_period com {len(blocked)} itens")
                    return blocked
    
    # Formato 3: Tenta extrair de data.blocked_period
    data_field = data.get("data")
    if isinstance(data_field, dict):
        blocked = data_field.get("blocked_period", [])
        if blocked:
            print(f"[DEBUG] Formato: data.blocked_period com {len(blocked)} itens")
            return blocked
    
    # Formato 4: Se data_field for lista, retorna ela
    if isinstance(data_field, list):
        print(f"[DEBUG] Formato: data como lista com {len(data_field)} itens")
        return data_field
    
    # Formato 5: Apenas status (sem reservas)
    if "status" in data and not data_field:
        print(f"[DEBUG] Formato: Apenas status ('{data.get('status')}') - sem reservas")
        return []
    
    print(f"[DEBUG] Nenhum blocked_period encontrado. Keys: {list(data.keys())}")
    return []


def fetch_calendar(session, unit_id: int, startdate: str, enddate: str) -> list:
    """Busca o calendário de uma casa para um período específico"""
    payload = make_payload_calendar(unit_id, startdate, enddate)
    headers = {"Content-Type": "application/json"}
    
    try:
        resp = session.post(API_URL, json=payload, headers=headers, timeout=(10, 40))
        resp.raise_for_status()
        
        # Parse JSON
        data = resp.json()
        
        # DEBUG: Log do tipo de resposta
        print(f"[DEBUG] unit_id={unit_id}, período={startdate} a {enddate}")
        print(f"[DEBUG] Response type: {type(data)}")
        if isinstance(data, dict):
            print(f"[DEBUG] Response keys: {list(data.keys())}")
        elif isinstance(data, list):
            print(f"[DEBUG] Response: lista com {len(data)} itens")
        
        # Verifica erro na resposta
        if isinstance(data, dict) and data.get("error"):
            print(f"[WARN] API erro para unit_id={unit_id}: {data.get('error')}")
            return []
        
        # Extrai blocked_period
        blocked = extract_blocked_period(data)
        print(f"[DEBUG] Blocked periods encontrados: {len(blocked)}")
        
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


# =========================
# TESTE COMPLETO
# =========================
import json
import requests
from datetime import datetime, timedelta

API_URL = "https://web.streamlinevrs.com/api/json"
TOKEN_KEY = "3ef223d3bbf7086cfb86df7e98d6e5d2"
TOKEN_SECRET = "a88d05b895affb815cc8a4d96670698ee486ea30"

def make_payload_calendar(unit_id: int, startdate: str, enddate: str):
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

def calculate_occupancy_corrected(blocked: list, startdate: str) -> tuple:
    """Calcula ocupação com interseção correta"""
    mes_obj = datetime.strptime(startdate, "%m/%d/%Y")
    
    if mes_obj.month == 12:
        last_day_month = datetime(mes_obj.year + 1, 1, 1) - timedelta(days=1)
    else:
        last_day_month = datetime(mes_obj.year, mes_obj.month + 1, 1) - timedelta(days=1)
    
    dias_no_mes = last_day_month.day
    
    if not blocked:
        return (0.0, 0, dias_no_mes)
    
    first_day_month = mes_obj
    dias_ocupados_set = set()
    
    for b in blocked:
        try:
            start = datetime.strptime(b["startdate"], "%m/%d/%Y")
            end = datetime.strptime(b["enddate"], "%m/%d/%Y")
            
            if end < first_day_month or start > last_day_month:
                continue
            
            overlap_start = max(start, first_day_month)
            overlap_end = min(end, last_day_month)
            
            current_day = overlap_start
            while current_day <= overlap_end:
                dias_ocupados_set.add(current_day.date())
                current_day += timedelta(days=1)
        except Exception as e:
            print(f"[WARN] Erro ao processar bloqueio: {e}")
            continue
    
    dias_ocupados = len(dias_ocupados_set)
    ocupacao = min(dias_ocupados / dias_no_mes, 1.0)
    
    return (ocupacao, dias_ocupados, dias_no_mes)


# TESTE
print("="*80)
print("TESTE COM CASA 890641 - JANEIRO 2026")
print("="*80)

session = requests.Session()
unit_id = 890641

# Buscar dados
blocked = fetch_calendar(session, unit_id, "01/01/2026", "01/31/2026")

print(f"\n📊 Reservas encontradas: {len(blocked)}")
if blocked:
    print("\nDetalhes das reservas:")
    for idx, b in enumerate(blocked, 1):
        print(f"  {idx}. {b['startdate']} até {b['enddate']} - {b.get('reason', 'N/A')}")

# Calcular ocupação CORRETA
ocupacao, dias_ocupados, dias_no_mes = calculate_occupancy_corrected(blocked, "01/01/2026")

print(f"\n{'='*80}")
print(f"RESULTADO FINAL:")
print(f"  Dias ocupados em JAN/2026: {dias_ocupados}/{dias_no_mes}")
print(f"  Taxa de ocupação: {ocupacao:.2%}")
print(f"{'='*80}")

print("\n⚠️  COMPARAÇÃO COM MÉTODO ANTIGO (ERRADO):")
print("  Método antigo: 19 dias (63.33%) - contava pelo startdate")
print(f"  Método novo: {dias_ocupados} dias ({ocupacao:.2%}) - conta interseção real")