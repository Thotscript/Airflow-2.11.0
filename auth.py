# -*- coding: utf-8 -*-
import os
import json
from pathlib import Path

from google_auth_oauthlib.flow import InstalledAppFlow

# ========= CONFIG (INLINE) =========
# 1) Onde está o credentials.json baixado do Google Cloud (OAuth Client: Desktop App)
CREDENTIALS_FILE = r"C:\temp\credentials.json"  # <-- TROQUE AQUI (Windows)
# Ex Linux/Mac: "/home/seu_usuario/credentials.json"

# 2) Onde você quer salvar o token.json gerado
OUTPUT_DIR = r"C:\temp"  # <-- TROQUE AQUI
TOKEN_FILE = os.path.join(OUTPUT_DIR, "token.json")

# 3) Scopes do Gmail (iguais aos do seu DAG)
SCOPES = [
    "https://www.googleapis.com/auth/gmail.modify",
    "https://www.googleapis.com/auth/gmail.send",
]
# ===================================


def main():
    cred_path = Path(CREDENTIALS_FILE)
    if not cred_path.exists():
        raise FileNotFoundError(f"Não achei o credentials.json em: {cred_path}")

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Cria o fluxo OAuth
    flow = InstalledAppFlow.from_client_secrets_file(str(cred_path), SCOPES)

    # Força refresh_token (importante!):
    # - access_type="offline" + prompt="consent" garante refresh_token na maioria dos casos
    creds = flow.run_local_server(
        host="localhost",
        port=0,                 # escolhe uma porta livre automaticamente
        open_browser=True,      # abre o navegador no seu PC
        authorization_prompt_message="Abra o link no navegador para autorizar.",
        success_message="OK! Token gerado. Pode fechar essa aba.",
        access_type="offline",
        prompt="consent",
    )

    # Salva token
    with open(TOKEN_FILE, "w", encoding="utf-8") as f:
        f.write(creds.to_json())

    print("\n✅ Token gerado com sucesso:")
    print(TOKEN_FILE)

    # Checagem rápida (mostra se veio refresh_token)
    data = json.loads(creds.to_json())
    has_refresh = bool(data.get("refresh_token"))
    print(f"\nrefresh_token presente? {'SIM' if has_refresh else 'NÃO'}")
    if not has_refresh:
        print(
            "\n⚠️ Atenção: veio sem refresh_token.\n"
            "Normalmente resolve apagando qualquer token antigo, e rodando de novo, ou garantindo prompt='consent'.\n"
            "Se ainda assim não vier, pode ser porque a conta já autorizou antes e o Google não devolve de novo."
        )


if __name__ == "__main__":
    main()
