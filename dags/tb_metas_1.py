import pandas as pd
from sqlalchemy import create_engine
from urllib.parse import quote_plus
from airflow import DAG
from airflow.decorators import task
import pendulum

# ---------------------------------------------------------
# CONFIG BANCO (MySQL)
# ---------------------------------------------------------
DB_USER = "root"
DB_PASS = "Tfl1234@"
DB_HOST = "host.docker.internal"
DB_PORT = 3306
DB_NAME = "ovh_silver"


def get_engine():
    """
    Cria e retorna uma engine SQLAlchemy para MySQL.
    """
    engine = create_engine(
        f"mysql+pymysql://{DB_USER}:{quote_plus(DB_PASS)}@{DB_HOST}:{DB_PORT}/{DB_NAME}?charset=utf8mb4"
    )
    return engine


# ---------------------------------------------------------
# HELPER: LER GOOGLE SHEETS (URL pubhtml -> CSV)
# ---------------------------------------------------------
def read_google_sheet(pubhtml_url: str) -> pd.DataFrame:
    """
    Recebe a URL 'pubhtml' do Google Sheets e lê direto como CSV.
    """
    url_csv = pubhtml_url.replace("pubhtml", "pub?output=csv")
    df = pd.read_csv(url_csv)
    return df


# ---------------------------------------------------------
# MAIN – faz o download das planilhas e grava no MySQL
# ---------------------------------------------------------
def main():
    engine = get_engine()

    try:
        # --------- 1) Ocupação -> tb_ocupacao_meta ---------
        url_ocupacao = "https://docs.google.com/spreadsheets/d/e/2PACX-1vQsakbeNFlSZ1IwAx2Xf7jeVlXT-3EYApjcw6c8hArAp664P0MHQljI79X6y_fsbt-JHJ1bPOtzdGsh/pubhtml"
        df_ocupacao = read_google_sheet(url_ocupacao)
        df_ocupacao.to_sql("tb_ocupacao_meta", con=engine, if_exists="replace", index=False)
        print("Tabela tb_ocupacao_meta atualizada a partir do Google Sheets!")

        # --------- 2) ADVP -> tb_advp_meta ---------
        url_advp = "https://docs.google.com/spreadsheets/d/e/2PACX-1vQDphFFvV9pt8DbRKFzCK7QPGv1TQYZU88k5p2kMNVbUXJ8IdBJZYls48b_wFuq73u12Ds04rj_vve9/pubhtml"
        df_advp = read_google_sheet(url_advp)
        df_advp.to_sql("tb_advp_meta", con=engine, if_exists="replace", index=False)
        print("Tabela tb_advp_meta atualizada a partir do Google Sheets!")

        # --------- 3) Percentual Meta Dia -> tb_percentual_meta_dia ---------
        url_percentual_dia = "https://docs.google.com/spreadsheets/d/e/2PACX-1vS8iN82axND2vpuHYbmgr_GS-OpgUR8tQvHhIKbf8bJ6v_i5z-L-qoMSHD44oAmD2I92_cjLIys_DWT/pubhtml"
        df_percentual_dia = read_google_sheet(url_percentual_dia)
        df_percentual_dia.to_sql("tb_percentual_meta_dia", con=engine, if_exists="replace", index=False)
        print("Tabela tb_percentual_meta_dia atualizada a partir do Google Sheets!")

        # --------- 4) Deflator Meta -> tb_deflator_meta ---------
        url_deflator = "https://docs.google.com/spreadsheets/d/e/2PACX-1vQnbPu_ua18ghjvdWmjnj9XXU2kdo2-V7PV1Ek_QE86wwF-8kVQtPje2WhVpI5zCAxxb5gB2TGCTYBE/pubhtml"
        df_deflator = read_google_sheet(url_deflator)
        df_deflator.to_sql("tb_deflator_meta", con=engine, if_exists="replace", index=False)
        print("Tabela tb_deflator_meta atualizada a partir do Google Sheets!")

    except Exception as e:
        print(f"Erro ao atualizar tabelas a partir do Google Sheets: {e}")

    finally:
        engine.dispose()
        print("Conexão com MySQL fechada.")
        print("Processo concluído.")


# ---------------------------------------------------------
# AIRFLOW DAG
# ---------------------------------------------------------
SP_TZ = pendulum.timezone("America/Sao_Paulo")

with DAG(
    dag_id="OVH-tabelas_google_sheets_meta",  # pode ajustar o nome
    start_date=pendulum.datetime(2025, 9, 23, 8, 0, tz=SP_TZ),
    schedule="30 5 * * *",  # todos os dias 05:30
    catchup=False,
    tags=["Tabelas - OVH", "Google Sheets"],
):
    @task()
    def atualizar_tabelas_meta():
        main()

    atualizar_tabelas_meta()
