import json
from datetime import datetime, timedelta
from collections import deque
import time
import numpy as np
import pandas as pd
import requests
from sqlalchemy import create_engine
from urllib.parse import quote_plus
from airflow import DAG
from airflow.decorators import task
from airflow.sensors.external_task import ExternalTaskSensor
import pendulum


def comp_linhas(df1:pd.DataFrame, df2:pd.DataFrame):
    """
    Compara o número de linhas de dois DataFrames com base em regras específicas.

    Args:
        df1 (pd.DataFrame): O DataFrame a ser comparado (df_bronze).
        df2 (pd.DataFrame): O DataFrame de referência (df_silver).

    Retorna:
        bool: Retorna True se df1 tiver a mesma quantidade de linhas ou
              até 5% a mais do que df2. Retorna False em todos os outros casos.
    """
    i_df_silver = len(df2)
    i_df_bronze = len(df1)

    # df1 deve ter mais ou a mesma quantidade de linhas que df2.
    if i_df_bronze < i_df_silver:
        return False
    
    # Se df2 for vazio, df1 também precisa ser.
    if i_df_silver == 0:
        return i_df_bronze == 0

    # Calcula a diferença percentual de df1 em relação a df2.
    diferenca_percentual = (i_df_bronze - i_df_silver) / i_df_silver

    # Define o limite de tolerância (5%) para mais linhas.
    limite = 0.05
    
    # Retorna True se a diferença percentual estiver dentro do limite para mais linhas,
    # caso contrário, retorna False.
    comp = diferenca_percentual <= limite

    return comp


def connection_df(database: str, table_name: str):
    """
    Conecta ao banco de dados e retorna um DataFrame a partir da tabela especificada.
    """
    def get_engine():

        DB_USER = "root"
        DB_PASS = "Tfl1234@"
        DB_HOST = "host.docker.internal"
        DB_PORT = 3306
        DB_NAME = database

        url = f"mysql+pymysql://{DB_USER}:{quote_plus(DB_PASS)}@{DB_HOST}:{DB_PORT}/{DB_NAME}?charset=utf8mb4"
        engine = create_engine(
            url,
            pool_pre_ping=True,
            future=True
        )
        return engine

    engine = get_engine()

    with engine.begin() as conn:
        df = pd.read_sql(f"SELECT * FROM {table_name};", conn)
    return df


def replace_silver_table(engine, df_bronze, table_name):
    """
    Substitui o conteúdo da tabela Silver pelo conteúdo do DataFrame Bronze.
    """
    df_bronze.to_sql(
        name=table_name,
        con=engine,
        if_exists='replace',
        index=False
    )


SP_TZ = pendulum.timezone("America/Sao_Paulo")

with DAG(
    dag_id="OVH-Check-tb_reservas",
    start_date=pendulum.datetime(2025, 9, 23, 8, 0, tz=SP_TZ),
    schedule=None,  # Este DAG será acionado externamente
    catchup=False,
    tags=["Check - tb_reservas"],
) as dag:

    @task
    def check_data_quality():
        """
        Executa as checagens de infraestrutura e qualidade dos dados.
        Retorna True se as verificações passarem, False caso contrário.
        """
        df_silver = connection_df("ovh_silver", "tb_reservas")
        df_bronze = connection_df("ovh_bronze", "tb_reservas")

        # Check de colunas
        columns_ok = set(df_bronze.columns) == set(df_silver.columns)
        if not columns_ok:
            print(f"[COLUMNS] Número de colunas divergentes! [ovh_silver] = {len(df_silver.columns)} | [ovh_bronze] = {len(df_bronze.columns)}")
            return False
        else:
            print(f"[COLUMNS] Número de colunas dentro do esperado!")

        # Check de tolerância de linhas
        lines_ok = comp_linhas(df_bronze, df_silver)
        if not lines_ok:
            print(f"[LINHAS] Número de linhas Bronze -> Silver acima da tolerância definida [5%]")
            return False
        else:
            print(f"[LINHAS] Número de linhas dentro da normalidade!")

        # Check de tolerância no valor total (price_nightly)
        total_price_nightly_df1 = df_silver['price_nightly'].sum()
        total_price_nightly_df2 = df_bronze['price_nightly'].sum()
        limit = 0.10
        diferenca_percentual = (total_price_nightly_df2 - total_price_nightly_df1) / total_price_nightly_df1
        
        price_ok = abs(diferenca_percentual) <= limit
        if not price_ok:
            print(f"[PRICE] Diferença no preço total excede o limite de 10%!")
            return False
        else:
            print(f"[PRICE] Preço total dentro da tolerância!")

        return True

    @task
    def update_silver_table(check_result: bool):
        """
        Substitui a tabela silver se as checagens passarem.
        """
        if check_result:
            print("Checagens de qualidade de dados aprovadas. Iniciando a substituição da tabela...")
            engine = connection_df("ovh_silver", "tb_reservas").get_engine()
            df_bronze = connection_df("ovh_bronze", "tb_reservas")
            replace_silver_table(engine, df_bronze, "tb_reservas")
            print("Substituição da tabela ovh_silver.tb_reservas concluída com sucesso.")
        else:
            print("Checagens de qualidade de dados falharam. A tabela não será atualizada.")


    # Define a dependência de DAGs usando ExternalTaskSensor
    wait_for_reservas_dag = ExternalTaskSensor(
        task_id="wait_for_reservas_dag",
        external_dag_id="OVH-tb_reservas",
        external_task_id=None, # Espera pela conclusão do DAG inteiro
        mode="poke", # Modo de espera (re-executa a cada 5s)
        timeout=60 * 60, # Timeout de 1 hora
        poke_interval=15 # Intervalo de 15 segundos para verificar o status
    )

    # Fluxo das tasks:
    # 1. Espera pela conclusão do DAG "OVH-tb_reservas"
    # 2. Executa a checagem de qualidade dos dados
    # 3. Atualiza a tabela silver se a checagem passar
    wait_for_reservas_dag >> check_data_quality() >> update_silver_table(check_data_quality())