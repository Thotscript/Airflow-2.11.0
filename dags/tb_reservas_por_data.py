import json
import traceback
from urllib.parse import quote_plus

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError

from airflow import DAG
from airflow.decorators import task
import pendulum


# =========================
# Configs de Banco (MySQL)
# =========================
DB_USER = "root"
DB_PASS = "Tfl1234@"
DB_HOST = "host.docker.internal"
DB_PORT = 3306
DB_NAME = "ovh_silver"

# =========================
# Consultas (MySQL)
# =========================
SQL_RESERVAS = """
SELECT *
FROM tb_reservas
WHERE status_id IN (4,5,6,7);
"""

SQL_PRECOS = """
SELECT *
FROM tb_reservas_price_day;
"""

def sanitize_for_sql(df: pd.DataFrame) -> pd.DataFrame:
    """
    Converte valores não-escalares (dict, list, set, tuple) para JSON string.
    Também lida com objetos mistos em colunas 'object'.
    Retorna um novo DataFrame.
    """
    def needs_json(v):
        return isinstance(v, (dict, list, set, tuple))

    out = df.copy()
    cols_converted = []

    for col in out.columns:
        if out[col].dtype == "object":
            # Só converte se existir ao menos um valor dict/list/set/tuple
            if out[col].map(needs_json).any():
                out[col] = out[col].apply(
                    lambda v: json.dumps(list(v), ensure_ascii=False)
                    if isinstance(v, set)
                    else (json.dumps(v, ensure_ascii=False) if needs_json(v) else v)
                )
                cols_converted.append(col)

    if cols_converted:
        print(f"[sanitize_for_sql] Colunas convertidas para JSON string: {cols_converted}")

    return out


def main():
    # ---- Engine MySQL ----
    engine = create_engine(
        f"mysql+pymysql://{DB_USER}:{quote_plus(DB_PASS)}@{DB_HOST}:{DB_PORT}/{DB_NAME}?charset=utf8mb4",
        pool_pre_ping=True,
    )

    try:
        # ---- Leitura das tabelas ----
        reservas = pd.read_sql(SQL_RESERVAS, con=engine)
        precos   = pd.read_sql(SQL_PRECOS,   con=engine)

        if reservas.empty:
            print("Aviso: tb_reservas não retornou linhas com status_id em (4,5,6,7). Encerrando.")
            return

        # ---- Merge (LEFT) reservas x preços ----
        resultado = pd.merge(
            reservas,
            precos,
            left_on="id",
            right_on="reservation_id",
            how="left",
            suffixes=("", "_price")
        )

        # ---- Substituições de 'casas espelhos' em unit_id ----
        casas_espelhos = {
            567515: 451446, 747169: 345511, 588400: 747861, 588406: 747861, 441403: 747861,
            614452: 341242, 747735: 341242, 747171: 379860, 379240: 747177, 747215: 334111,
            747220: 380233, 614526: 488453, 747747: 488453, 614527: 488453, 575254: 511116,
            747224: 445230, 747230: 334112, 563466: 590935, 747235: 443999, 614955: 495437,
            747754: 495437, 747249: 598346, 747092: 379248, 747254: 425652, 746111: 514404,
            735680: 498139
        }

        if "unit_id" in resultado.columns:
            resultado["unit_id"] = pd.to_numeric(resultado["unit_id"], errors="coerce").astype("Int64")
            resultado["unit_id"] = resultado["unit_id"].replace(casas_espelhos)

        # ---- Atualização de datas de criação por ID ----
        # Se esta tabela tiver 'creation_date' e você precisar ajustar datas manualmente, faça aqui:
        # Exemplo:
        # novas_datas = { 33956627: '10/21/2023 00:00:00', ... }
        # if "creation_date" in resultado.columns:
        #     for _id, nova_data in novas_datas.items():
        #         mask = resultado["id"] == _id
        #         if mask.any():
        #             resultado.loc[mask, "creation_date"] = pd.to_datetime(
        #                 nova_data, format="%m/%d/%Y %H:%M:%S", errors="coerce"
        #             )
        #     resultado["creation_date"] = pd.to_datetime(resultado["creation_date"], errors="coerce")

        # ---- Sanitização para evitar "dict can not be used as parameter" ----
        resultado = sanitize_for_sql(resultado)

        # ---- Escrita transacional segura ----
        table_name = "tb_reservas_por_data"
        try:
            with engine.begin() as conn:
                resultado.to_sql(
                    table_name,
                    con=conn,
                    if_exists="replace",
                    index=False,
                    chunksize=100_000,
                    method="multi"
                )
            print(f"Dados Gravados Com Sucesso em {table_name}. Linhas: {len(resultado)}")
        except SQLAlchemyError as e:
            print("Erro ao gravar no MySQL via to_sql:")
            print(e)
            traceback.print_exc()
            raise
    finally:
        engine.dispose()


# =================
# Airflow DAG
# =================
SP_TZ = pendulum.timezone("America/Sao_Paulo")

with DAG(
    dag_id="OVH-tb_reservas_por_data",
    start_date=pendulum.datetime(2025, 9, 23, 8, 0, tz=SP_TZ),
    schedule="0 5 * * *",  # diariamente 05:00
    catchup=False,
    tags=["Tabelas - OVH"],
) as dag:

    @task()
    def build_tb_reservas_por_data():
        main()

    build_tb_reservas_por_data()
