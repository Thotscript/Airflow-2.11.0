from __future__ import annotations
from datetime import datetime
from airflow import DAG
from airflow.decorators import task

with DAG(
    dag_id="teste_basico", # Nome da dag (task/tarefa)
    start_date=datetime(2025, 8, 1), # data de inicio da tarefa
    schedule=None,   # rode manualmente
    catchup=False,
    tags=["test"],
) as dag:

    @task()
    def say():
        print("Olá, Airflow!")

    say()
