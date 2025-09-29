#Funcionalidade: Utiliza a API do streamline para retornar todas as reservas existentes 
#Bases geradas: tb_reservas

#Código

import pandas as pd
import requests
import json
import mysql.connector
import pendulum
from airflow import DAG
from airflow.decorators import task

def main(): 
    url = "https://web.streamlinevrs.com/api/json"

    data_payload = {
        "methodName": "GetReservations",
        "params": {
            "token_key": "a43cb1b5ed27cce283ab2bb4df540037",
            "token_secret": "72c7b8d5ba4b0ef14fe97a7e4179bdfa92cfc6ea",
            "return_full": True,
            #"status_ids": "4,5,7",
            "page_results_number": 5000, 
            "page_number": 1 
        }
    }

    headers = {
        "Content-Type": "application/json"
    }

    response = requests.post(url, data=json.dumps(data_payload), headers=headers)
    print("Resposta:", response.status_code, response.text[:500])
    
    data_dict = json.loads(response.content)

    df_list = []


    while data_dict["data"]["reservations"]:
        

        df = pd.json_normalize(data_dict["data"]["reservations"])
        df_list.append(df)
        data_payload["params"]["page_number"] += 1
        response = requests.post(url, data=json.dumps(data_payload), headers=headers)
        data_dict = json.loads(response.content)

        
    df_full = pd.concat(df_list, ignore_index=True)
    df_full = df_full.drop_duplicates(subset=['id'])


    ## Conexao com o mysql

    conn = mysql.connector.connect(
        host="host.docker.internal",
        user="root",
        password="Tfl1234@",
        database="tfl_silver"
    )

    cursor = conn.cursor()
    tb_name = "tb_reservas"

    # Drop e recria a tabela com todas as colunas como TEXT (mais simples possível)
    cursor.execute(f"DROP TABLE IF EXISTS {tb_name}")

    cols = ", ".join([f"`{c}` TEXT" for c in df_full.columns])
    cursor.execute(f"CREATE TABLE {tb_name} ({cols})")

    # Substitui NaN por None (para não dar erro no INSERT)
    df_full = df_full.where(pd.notna(df_full), None)

    # Monta INSERT
    cols_str = ", ".join([f"`{c}`" for c in df_full.columns])
    placeholders = ", ".join(["%s"] * len(df_full.columns))
    insert_sql = f"INSERT INTO {tb_name} ({cols_str}) VALUES ({placeholders})"

    # Insere todas as linhas
    cursor.executemany(insert_sql, df_full.values.tolist())

    conn.commit()
    print(f"Tabela {tb_name} recriada e {len(df_full)} linhas inseridas.")

    cursor.close()
    conn.close()

if __name__ == "__main__":
    main()

SP_TZ = pendulum.timezone("America/Sao_Paulo")

with DAG(
    dag_id="OVH-tb_reservas",  # Nome da dag (task/tarefa)
    start_date=pendulum.datetime(2025, 9, 23, 8, 0, tz=SP_TZ),
    schedule=None,
    catchup=False,
    tags=["Tabelas - OVH"],
) as dag:

    @task()
    def tb_reservas():
        main()  # executa seu fluxo exatamente como está definido acima

    tb_reservas()