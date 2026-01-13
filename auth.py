# -*- coding: utf-8 -*-
import os
from google_auth_oauthlib.flow import InstalledAppFlow

SCOPES = [
    "https://www.googleapis.com/auth/gmail.modify",
    "https://www.googleapis.com/auth/gmail.send",
]

def main():
    # Ajuste o caminho do credentials.json NO SEU PC
    CREDENTIALS_FILE = r"C:\\Users\\Juras\\credentials.json"  # Windows exemplo
    # CREDENTIALS_FILE = "/home/seuuser/credentials.json"   # Linux/Mac exemplo

    OUT_TOKEN_FILE = os.path.abspath("token.json")

    if not os.path.exists(CREDENTIALS_FILE):
        raise FileNotFoundError(f"credentials.json não encontrado em: {CREDENTIALS_FILE}")

    flow = InstalledAppFlow.from_client_secrets_file(CREDENTIALS_FILE, SCOPES)

    # Isso abre o browser e usa redirect em http://localhost:<porta>/
    creds = flow.run_local_server(
        host="localhost",
        port=8089,                # pode trocar se 8089 estiver ocupado
        open_browser=True,
        access_type="offline",
        prompt="consent",
        authorization_prompt_message="Abra o navegador para autorizar...",
        success_message="✅ OK! Token gerado. Pode fechar esta aba.",
    )

    with open(OUT_TOKEN_FILE, "w", encoding="utf-8") as f:
        f.write(creds.to_json())

    print("✅ Token gerado em:", OUT_TOKEN_FILE)

if __name__ == "__main__":
    main()
