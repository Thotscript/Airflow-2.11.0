import pandas as pd
import requests
import json
import mysql.connector
import pendulum
import numpy as np
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

    # Adiciona coluna Administradora
    df_full['Administradora'] = "ONE VACATION HOME"

    # LIMPEZA DE COLUNAS
    df_full = df_full.loc[:, df_full.columns.notna()]
    df_full.columns = [str(col) for col in df_full.columns]
    valid_columns = [col for col in df_full.columns 
                     if col not in ['nan', 'None', '', 'NaN', 'NAN']]
    df_full = df_full[valid_columns]
    
    df_full.columns = (df_full.columns
                       .str.strip()
                       .str.replace(' ', '_', regex=False)
                       .str.replace('.', '_', regex=False)
                       .str.replace('-', '_', regex=False)
                       .str.replace('(', '', regex=False)
                       .str.replace(')', '', regex=False)
                       .str.replace('[', '', regex=False)
                       .str.replace(']', '', regex=False))
    
    print(f"Total de colunas: {len(df_full.columns)}")
    print(f"Total de linhas: {len(df_full)}")

    # LIMPEZA DOS DADOS (VALORES) - AQUI ESTÁ A CORREÇÃO PRINCIPAL
    # Substitui NaN, None e strings 'nan' por None
    df_full = df_full.replace({np.nan: None, 'nan': None, 'NaN': None, 'None': None, '': None})
    
    # Garante que não há mais NaN
    df_full = df_full.where(pd.notna(df_full), None)

    ## Conexao com o mysql
    conn = mysql.connector.connect(
        host="host.docker.internal",
        user="root",
        password="Tfl1234@",
        database="tfl_silver"
    )

    cursor = conn.cursor()
    tb_name = "tb_reservas"

    # Drop e recria a tabela
    cursor.execute(f"DROP TABLE IF EXISTS {tb_name}")

    cols = ", ".join([f"`{c}` TEXT" for c in df_full.columns])
    cursor.execute(f"CREATE TABLE {tb_name} ({cols})")

    # Monta INSERT
    cols_str = ", ".join([f"`{c}`" for c in df_full.columns])
    placeholders = ", ".join(["%s"] * len(df_full.columns))
    insert_sql = f"INSERT INTO {tb_name} ({cols_str}) VALUES ({placeholders})"

    # Insere em lotes
    batch_size = 1000
    total_inserted = 0
    
    for i in range(0, len(df_full), batch_size):
        batch = df_full.iloc[i:i+batch_size]
        cursor.executemany(insert_sql, batch.values.tolist())
        conn.commit()
        total_inserted += len(batch)
        print(f"Inseridas {total_inserted}/{len(df_full)} linhas")

    print(f"Tabela {tb_name} recriada e {len(df_full)} linhas inseridas com sucesso!")

    cursor.close()
    conn.close()

if __name__ == "__main__":
    main()

SP_TZ = pendulum.timezone("America/Sao_Paulo")

with DAG(
    dag_id="OVH-tb_reservas",
    start_date=pendulum.datetime(2025, 9, 23, 8, 0, tz=SP_TZ),
    schedule="0 5 * * *",
    catchup=False,
    tags=["Tabelas - OVH"],
) as dag:

    @task()
    def tb_reservas():
        main()

    tb_reservas()