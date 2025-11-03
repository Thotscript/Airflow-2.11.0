import os
import base64
import time
import json
import random
import requests
import pandas as pd
from datetime import datetime, timedelta
import mysql.connector
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication

from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from google.auth.exceptions import RefreshError

# Airflow / Scheduler
import pendulum
from airflow import DAG
from airflow.decorators import task

# ================================
# CONFIGURAÇÕES
# ================================
# Gmail API
GMAIL_SCOPES = [
    "https://www.googleapis.com/auth/gmail.modify",
    "https://www.googleapis.com/auth/gmail.send",
]
USER_ID = "me"

# Pastas de tokens
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CREDENTIALS_FILE = os.path.join(BASE_DIR, "Tokens", "credentials.json")  # não usado no modo estrito
GMAIL_TOKEN_FILE = os.path.join(BASE_DIR, "Tokens", "gmail_token.json")

# MySQL (ovh_silver)
MYSQL_CONFIG = {
    "host": "host.docker.internal",
    "user": "root",
    "password": "Tfl1234@",
    "database": "ovh_silver",
}

# Streamline
STREAMLINE_URL = "https://web.streamlinevrs.com/api/json"
TOKEN_KEY = "a43cb1b5ed27cce283ab2bb4df540037"
TOKEN_SECRET = "72c7b8d5ba4b0ef14fe97a7e4179bdfa92cfc6ea"
HEADERS_JSON = {"Content-Type": "application/json"}

# Saída
EXCEL_FILE = os.path.join("/tmp", "Uniglobe.xlsx")  # caminho seguro no container

# ================================
# GMAIL AUTH (modo estrito/headless)
# ================================
def get_gmail_credentials_strict() -> Credentials:
    """
    Modo estrito para ambientes headless (Airflow):
      - NÃO tenta login interativo.
      - Exige Tokens/gmail_token.json previamente provisionado com refresh_token.
      - Faz refresh se necessário.
      - Falha (raise) se o token não existir ou não puder ser renovado.
    """
    if not os.path.exists(GMAIL_TOKEN_FILE):
        raise RuntimeError(
            "[GMAIL] Token ausente em Tokens/gmail_token.json. "
            "Gere o token fora do Airflow e monte o arquivo no container."
        )

    try:
        creds = Credentials.from_authorized_user_file(GMAIL_TOKEN_FILE, GMAIL_SCOPES)
    except Exception as e:
        raise RuntimeError(f"[GMAIL] Token corrompido: {e}")

    if not creds.valid:
        if creds.expired and creds.refresh_token:
            try:
                creds.refresh(Request())
                # persiste novo access_token/expiry
                with open(GMAIL_TOKEN_FILE, "w") as f:
                    f.write(creds.to_json())
            except RefreshError as e:
                raise RuntimeError(f"[GMAIL] Falha ao renovar token: {e}")
        else:
            raise RuntimeError(
                "[GMAIL] Token inválido e sem refresh_token. Reprovisione o gmail_token.json."
            )
    return creds


def get_gmail_service_strict():
    return build("gmail", "v1", credentials=get_gmail_credentials_strict())


# ================================
# EMAIL (com retry/backoff)
# ================================
def send_custom_email_with_attachment(
    service,
    recipient,
    subject,
    message_text,
    attachment_path,
    sender="revenue@onevacationhome.com",
    max_retries=5,
):
    """
    Envia e-mail e faz retry/backoff em 429/5xx.
    Falha (raise) se não conseguir enviar.
    """
    recipient_str = ", ".join(recipient) if isinstance(recipient, list) else recipient

    message = MIMEMultipart()
    message["to"] = recipient_str
    message["from"] = sender
    message["subject"] = subject
    message.attach(MIMEText(message_text, "html"))

    with open(attachment_path, "rb") as f:
        attachment_data = f.read()
    attachment = MIMEApplication(attachment_data, _subtype="octet-stream")
    attachment.add_header(
        "Content-Disposition", "attachment", filename=os.path.basename(attachment_path)
    )
    message.attach(attachment)

    raw_message = base64.urlsafe_b64encode(message.as_bytes()).decode("utf-8")
    body = {"raw": raw_message}

    attempt = 0
    backoff = 1.0
    while True:
        attempt += 1
        try:
            sent_message = (
                service.users().messages().send(userId=USER_ID, body=body).execute()
            )
            print("E-mail enviado com sucesso! ID da mensagem:", sent_message.get("id"))
            return sent_message
        except Exception as e:
            # tenta extrair código de erro
            err_txt = str(e)
            is_retryable = any(code in err_txt for code in ["429", "500", "502", "503", "504"])
            if attempt >= max_retries or not is_retryable:
                raise
            sleep_for = backoff + random.uniform(0, 0.5)
            print(f"[GMAIL] Falha tentativa {attempt}: {e}. Retentando em {sleep_for:.1f}s...")
            time.sleep(sleep_for)
            backoff *= 2.0


# ================================
# MYSQL
# ================================
def get_mysql_connection():
    return mysql.connector.connect(**MYSQL_CONFIG)


def fetch_confirmation_ids_from_mysql():
    """
    Busca confirmation_id em ovh_silver.tb_reservas conforme regra:
      - Seg–Qui: startdate = hoje
      - Sex: startdate IN (hoje, amanhã, depois)
    Sempre com: status_code NOT IN (6,8,10) e confirmation_id > 0
    """
    today = datetime.today().date()
    tomorrow = today + timedelta(days=1)
    day_after = today + timedelta(days=2)

    if today.weekday() == 4:  # sexta-feira
        dates = [today, tomorrow, day_after]
    else:  # seg-qui (mantém só hoje; se quiser incluir fim de semana, ajuste aqui)
        dates = [today]

    conn = get_mysql_connection()
    try:
        cur = conn.cursor()
        placeholders = ", ".join(["%s"] * len(dates))
        sql = f"""
            SELECT DISTINCT confirmation_id
            FROM tb_reservas
            WHERE DATE(startdate) IN ({placeholders})
              AND status_code NOT IN (6,8,10)
              AND confirmation_id > 0
        """
        cur.execute(sql, tuple(dates))
        ids = [r[0] for r in cur.fetchall()]
        print(f"Confirmation IDs obtidos do MySQL ({len(ids)}): {ids}")
        return ids
    finally:
        try:
            cur.close()
        except Exception:
            pass
        conn.close()


# ================================
# STREAMLINE (GetReservationInfo)
# ================================
def print_api_error_response(label, response, max_chars=2000):
    """Loga somente a resposta em caso de erro."""
    try:
        body = response.text
    except Exception:
        body = "<sem .text disponível>"
    if body and len(body) > max_chars:
        body = body[:max_chars] + f"... [truncado em {max_chars} chars]"
    print(f"[ERRO {label}] status={getattr(response, 'status_code', 'N/A')}")
    print(f"[ERRO {label}] body:\n{body}\n")


def fetch_reservation(cid: int) -> pd.DataFrame:
    """
    Chama GetReservationInfo e retorna 1 DataFrame (1 linha) com campos principais + additional_fields.
    Em erro, retorna DataFrame vazio.
    """
    payload = {
        "methodName": "GetReservationInfo",
        "params": {
            "token_key": TOKEN_KEY,
            "token_secret": TOKEN_SECRET,
            "confirmation_id": cid,
            "return_address": 1,
            "return_flags": 1,
            "show_owner_charges": 1,
            "show_taxes_and_fees": 1,
            "show_commission_information": 1,
            "return_payments": 1,
            "return_additional_fields": 1,
            "show_payments_folio_history": 1,
            "include_security_deposit": 1,
            "return_housekeeping_schedule": 1,
            "return_happystays_code": 1,
            "show_guest_feedback_url": 1,
        },
    }
    try:
        resp = requests.post(STREAMLINE_URL, data=json.dumps(payload), headers=HEADERS_JSON, timeout=60)
    except requests.RequestException as e:
        print(f"[ERRO confirmation_id={cid}] falha na requisição: {repr(e)}")
        return pd.DataFrame()

    if not resp.ok:
        print_api_error_response(f"confirmation_id={cid}", resp)
        return pd.DataFrame()

    try:
        data = resp.json()
    except ValueError:
        print_api_error_response(f"confirmation_id={cid} (JSON inválido)", resp)
        return pd.DataFrame()

    if "data" not in data or "reservation" not in data["data"]:
        print_api_error_response(f"confirmation_id={cid} (estrutura inesperada)", resp)
        return pd.DataFrame()

    reservation = data["data"]["reservation"]
    nested_fields = [
        "flags",
        "additional_fields",
        "expected_charges",
        "housekeeping_schedule",
        "taxes_and_fees",
        "commission_information",
        "payments_folio_history",
    ]
    main_data = {k: v for k, v in reservation.items() if k not in nested_fields}
    df_main = pd.json_normalize(main_data)

    additional_fields_list = reservation.get("additional_fields", {}).get("additional_field", [])
    df_add = pd.DataFrame()
    if additional_fields_list:
        add_dict = {item.get("name"): item.get("value") for item in additional_fields_list}
        df_add = pd.DataFrame([add_dict])

    return pd.concat([df_main, df_add], axis=1)


# ================================
# PLANILHA (Excel) + ENVIO
# ================================
def build_excel_and_send(final_df: pd.DataFrame, excel_path: str = EXCEL_FILE):
    # colunas alvo
    final_columns = ["startdate", "unit_name", "Client Name", "phone", "door code", "Emergency Code"]

    def door_code_rule(row):
        try:
            code = float(row.get("Reservation: EasyHub Code", 0))
        except (ValueError, TypeError):
            code = 0
        return (str(row.get("Reservation: EasyHub Code", "")) + "#") if code > 0 else row.get("confirmation_id", "")

    def emergency_code_rule(row):
        try:
            code = float(row.get("Reservation: EasyHub Code", 0))
        except (ValueError, TypeError):
            code = 0
        return "252500#" if code > 0 else "2525/7676/6965"

    if not final_df.empty:
        # formatar data apenas para visual (não forçar fuso)
        if "startdate" in final_df.columns:
            final_df["startdate"] = pd.to_datetime(final_df["startdate"], format="%m/%d/%Y", errors="coerce").dt.date

        final_df["Client Name"] = (
            final_df.get("first_name", "").fillna("") + " " + final_df.get("last_name", "").fillna("")
        ).str.strip()
        final_df["door code"] = final_df.apply(door_code_rule, axis=1)
        final_df["Emergency Code"] = final_df.apply(emergency_code_rule, axis=1)

        for col in final_columns:
            if col not in final_df.columns:
                final_df[col] = ""

        out = final_df[final_columns].copy()
        if "startdate" in out.columns:
            out = out.sort_values(by="startdate", ascending=True)
    else:
        out = pd.DataFrame(columns=final_columns)

    out.to_excel(excel_path, index=False)
    print(f"Arquivo Excel salvo com sucesso em: {excel_path}")

    # Envia e-mail (obrigatório). Se falhar, levanta exceção para Airflow marcar como failed.
    service = get_gmail_service_strict()
    recipient = "booking@onevacationhome.com"
    subject = "Planilha Uniglobe"
    message_text = """
    <html>
      <body>
        <p>Boa tarde,</p>
        <p>Segue a planilha da Uniglobe.</p>
        <p>Obrigado</p>
      </body>
    </html>
    """
    send_custom_email_with_attachment(service, recipient, subject, message_text, attachment_path=excel_path)


# ================================
# MAIN
# ================================
def main():
    # 1) IDs do MySQL com regra de datas
    ids = fetch_confirmation_ids_from_mysql()
    if not ids:
        # Falha "limpa": sem IDs não há o que enviar; mas o e-mail é essencial — podemos optar por
        # enviar um e-mail vazio OU falhar a task.
        # Aqui, vamos falhar explicitamente para evidenciar a situação.
        raise RuntimeError("Nenhum confirmation_id retornado do MySQL para as regras de data.")

    # 2) Chama API para cada ID
    frames = []
    for cid in ids:
        df_one = fetch_reservation(cid)
        if not df_one.empty:
            frames.append(df_one)

    final_df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    print(f"Registros coletados: {len(final_df)}")

    # 3) Excel + e-mail (obrigatório)
    build_excel_and_send(final_df, EXCEL_FILE)


if __name__ == "__main__":
    main()

# ================================
# AIRFLOW DAG
# ================================
SP_TZ = pendulum.timezone("America/Sao_Paulo")

with DAG(
    dag_id="Uniglobe",  # Nome da DAG
    start_date=pendulum.datetime(2025, 9, 23, 8, 0, tz=SP_TZ),
    schedule="0 8,18 * * *",
    catchup=False,
    tags=["Uniglobe - OVH"],
) as dag:

    @task()
    def run_uniglobe():
        # Em Airflow, modo estrito: se não houver token válido, a task falha.
        main()

    run_uniglobe()
