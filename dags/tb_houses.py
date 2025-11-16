import logging
from urllib.parse import quote_plus

from sqlalchemy import create_engine, text
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


def main():
    engine = get_engine()

    logging.basicConfig()
    logging.getLogger("sqlalchemy.engine").setLevel(logging.INFO)

    # ---------------------------------------------------------
    # 1) tb_houses
    # ---------------------------------------------------------
    sql_drop_tb_houses = """
        DROP TABLE IF EXISTS tb_houses;
    """

    # IMPORTANTE:
    # - Assumindo colunas de data/datetime já no tipo DATE/DATETIME.
    # - Se ainda estiverem como texto (ex: 'MM/dd/yyyy'), usar STR_TO_DATE():
    #   DATE_FORMAT(STR_TO_DATE(a.date, '%m/%d/%Y'), '%Y%m%d')
    sql_create_tb_houses = """
        CREATE TABLE tb_houses AS
        SELECT 
            A.id AS unit_id,
            A.Administradora,
            A.date AS Date_House,
            B.id AS id_reserva,
            B.confirmation_id AS confirmation_id,
            B.client_id AS client_id,
            B.occupants AS occupants,
            B.occupants_small AS occupants_small,
            B.email AS email,
            B.first_name AS first_name,
            B.last_name AS last_name,
            B.days_number AS days_number,
            B.maketype_description AS maketype_description,
            B.type_name AS type_name,
            B.status_code AS status_code,
            B.price_nightly AS price_nightly,
            B.price_total AS price_total,
            B.status_id AS status_id,
            B.hear_about_name AS hear_about_name,
            B.travelagent_name AS travelagent_name,
            B.creation_date AS creation_date,
            B.startdate AS startdate,
            B.enddate AS enddate,
            B.last_updated AS last_updated,
            B.season AS season,
            B.price AS price,
            B.extra AS extra,
            B.discount AS discount,
            B.original_cost AS original_cost,
            B.reservation_id AS reservation_id,
            C.`rate_type.season` AS season_cadastro,
            C.`rate_type.rate` AS rate_cadastro,
            C.`rate_type.minStay` AS minstay_cadastro,
            D.name,
            D.location_name,
            D.bedrooms_number,
            D.bathrooms_number,
            D.condo_type_name,
            D.condo_type_group_name,
            D.max_adults,
            D.renting_type,
            F.Ocupação,
            F.Season AS temporada,
            G.minStay AS minstay_actual,
            G.rate AS rate_actual,
            NOW() AS atualizacao_
        FROM tb_unit_dates AS A
        LEFT JOIN tb_reservas_por_data AS B
            ON CONCAT(A.id, A.Administradora, DATE_FORMAT(A.date, '%Y%m%d')) =
               CONCAT(B.unit_id, B.Administradora_x, DATE_FORMAT(B.Date, '%Y%m%d'))
        LEFT JOIN tb_metas AS C
            ON CONCAT(A.id, A.Administradora, DATE_FORMAT(A.date, '%Y%m%d')) =
               CONCAT(C.id_unit, C.Administradora, DATE_FORMAT(C.date, '%Y%m%d'))
        LEFT JOIN tb_property_list_wordpress AS D
            ON CONCAT(A.id, A.Administradora) = CONCAT(D.id, D.Administradora)
        LEFT JOIN tb_ocupacao_meta AS F
            ON CONCAT(DATE_FORMAT(F.`Data`, '%Y%m%d'), F.Administradora) =
               CONCAT(DATE_FORMAT(A.date, '%Y%m%d'), A.Administradora)
        LEFT JOIN tb_price AS G
            ON CONCAT(A.id, A.Administradora, DATE_FORMAT(A.date, '%Y%m%d')) =
               CONCAT(G.id_unit, G.Administradora, DATE_FORMAT(G.date, '%Y%m%d'));
    """

    # ---------------------------------------------------------
    # 2) tb_houses_geral
    # ---------------------------------------------------------
    sql_drop_tb_houses_geral = """
        DROP TABLE IF EXISTS tb_houses_geral;
    """

    # Aqui uso CTE (WITH), suportado em MySQL 8+.
    sql_create_tb_houses_geral = """
        CREATE TABLE tb_houses_geral AS
        WITH tb_houses_parcial AS (
            SELECT
                A.id AS unit_id,
                A.Administradora,
                A.date AS Date_House,
                B.id AS id_reserva,
                B.confirmation_id AS confirmation_id,
                B.client_id AS client_id,
                B.occupants AS occupants,
                B.occupants_small AS occupants_small,
                B.email AS email,
                B.first_name AS first_name,
                B.last_name AS last_name,
                B.days_number AS days_number,
                B.maketype_description AS maketype_description,
                B.type_name AS type_name,
                B.status_code AS status_code,
                B.price_nightly AS price_nightly,
                B.price_total AS price_total,
                B.status_id AS status_id,
                B.hear_about_name AS hear_about_name,
                B.travelagent_name AS travelagent_name,
                B.creation_date AS creation_date,
                B.startdate AS startdate,
                B.enddate AS enddate,
                B.last_updated AS last_updated,
                B.season AS season,
                B.price AS price,
                B.extra AS extra,
                B.discount AS discount,
                B.original_cost AS original_cost,
                B.reservation_id AS reservation_id,
                C.`rate_type.season` AS season_cadastro,
                C.`rate_type.rate` AS rate_cadastro,
                C.`rate_type.minStay` AS minstay_cadastro,
                D.name,
                B.location_name AS name_of_sales,
                D.location_name,
                D.bedrooms_number,
                D.bathrooms_number,
                D.condo_type_name,
                D.condo_type_group_name,
                COALESCE(D.renting_type, 'NON-RENTING') AS renting_type,
                F.Ocupação,
                F.`Holiday for Year` AS Holiday_for_Year,
                F.`Name of Holiday` AS Name_of_Holiday,
                F.`Number Of Day Holidays` AS Number_Of_Day_Holidays,
                F.Season AS temporada,
                F.Temporada AS Temporada_Intervalo,
                G.minStay AS minstay_actual,
                G.rate AS rate_actual,
                COALESCE(H.Status, 'Disponivel') AS Status_Vendas,
                YEAR(A.date) AS Ano,
                MONTH(A.date) AS Mes,
                CASE
                    WHEN A.date >= CURDATE() THEN 1
                    ELSE 0
                END AS Considerar,
                COALESCE(I.value, 'NÃO') AS valido_para_meta,
                J.Canal,
                DATEDIFF(A.date, B.creation_date) AS Advp_Dias,
                CASE
                    WHEN DATEDIFF(A.date, B.creation_date) IS NULL THEN '-'
                    WHEN DATEDIFF(A.date, B.creation_date) <= 30 THEN '<= 30 days'
                    WHEN DATEDIFF(A.date, B.creation_date) <= 60 THEN '<= 60 days'
                    WHEN DATEDIFF(A.date, B.creation_date) <= 90 THEN '<= 90 days'
                    WHEN DATEDIFF(A.date, B.creation_date) <= 120 THEN '<= 120 days'
                    WHEN DATEDIFF(A.date, B.creation_date) <= 180 THEN '<= 180 days'
                    ELSE 'Acima de 180 days'
                END AS Advp_Faixas,
                CASE
                    WHEN DATEDIFF(A.date, B.creation_date) IS NULL THEN '-'
                    WHEN DATEDIFF(A.date, B.creation_date) <= 30 THEN 1
                    WHEN DATEDIFF(A.date, B.creation_date) <= 60 THEN 2
                    WHEN DATEDIFF(A.date, B.creation_date) <= 90 THEN 3
                    WHEN DATEDIFF(A.date, B.creation_date) <= 120 THEN 4
                    WHEN DATEDIFF(A.date, B.creation_date) <= 180 THEN 5
                    ELSE 6
                END AS Advp_Ordem,
                CASE
                    WHEN B.days_number IS NULL THEN '-'
                    WHEN B.days_number <= 3 THEN '<= 3 nigths'
                    WHEN B.days_number <= 5 THEN '<= 5 nigths'
                    WHEN B.days_number <= 7 THEN '<= 7 nigths'
                    WHEN B.days_number <= 10 THEN '<= 10 nigths'
                    WHEN B.days_number <= 15 THEN '<= 15 nigths'
                    ELSE 'Acima de 15 nigths'
                END AS Days_Number_Faixa,
                K.AnoMes,
                K.Dia_Ajustado,
                DATE_FORMAT(K.Dia_Ajustado, '%m. %b') AS Mes_Classificado,
                K.YTD,
                K.MTD,
                K.L7D,
                K.SEMANA,
                K.`Ano Comparação` AS Ano_Comparacao,
                L.`Ano Comparação` AS Ano_Comparacao_Check_In,
                DATE_FORMAT(A.date, '%m. %b') AS Mes_Classificado_Check_In,
                L.Dia_Ajustado AS Dia_Ajustado_Check_in,
                NOW() AS atualizacao_
            FROM tb_unit_dates AS A
            LEFT JOIN tb_reservas_por_data AS B
                ON CONCAT(A.id, A.Administradora, DATE_FORMAT(A.date, '%Y%m%d')) =
                   CONCAT(B.unit_id, B.Administradora_x, DATE_FORMAT(B.Date, '%Y%m%d'))
            LEFT JOIN tb_metas AS C
                ON CONCAT(A.id, A.Administradora, DATE_FORMAT(A.date, '%Y%m%d')) =
                   CONCAT(C.id_unit, C.Administradora, DATE_FORMAT(C.date, '%Y%m%d'))
            LEFT JOIN tb_depara_casas AS D
                ON CONCAT(A.id, A.Administradora) = CONCAT(D.id, D.Administradora)
            LEFT JOIN tb_ocupacao_meta AS F
                ON CONCAT(DATE_FORMAT(F.`Data`, '%Y%m%d'), F.Administradora) =
                   CONCAT(DATE_FORMAT(A.date, '%Y%m%d'), A.Administradora)
            LEFT JOIN tb_price AS G
                ON CONCAT(A.id, A.Administradora, DATE_FORMAT(A.date, '%Y%m%d')) =
                   CONCAT(G.id_unit, G.Administradora, DATE_FORMAT(G.date, '%Y%m%d'))
            LEFT JOIN tb_status_vendas AS H
                ON B.type_name = H.type_name
            LEFT JOIN tb_filtro_metas AS I
                ON A.id = I.id_unit
            LEFT JOIN tb_depara_canais AS J
                ON B.type_name = J.Status
            LEFT JOIN tb_calendario AS K
                ON DATE_FORMAT(B.creation_date, '%Y%m%d') =
                   DATE_FORMAT(K.data, '%Y%m%d')
            LEFT JOIN tb_calendario AS L
                ON DATE_FORMAT(A.date, '%Y%m%d') =
                   DATE_FORMAT(L.data, '%Y%m%d')
        ),
        primeira_venda AS (
            SELECT
                unit_id,
                Administradora,
                MIN(creation_date) AS earliest_creation_date,
                MAX(Date_House) AS latest_date_house_with_reserva,
                MIN(Date_House) AS earliest_date_house_with_reserva,
                NOW() AS atualizacao
            FROM tb_houses
            WHERE id_reserva > 0
              AND creation_date IS NOT NULL
              AND Date_House IS NOT NULL
            GROUP BY unit_id, Administradora
        )
        SELECT
            A.*,
            B.earliest_creation_date,
            B.latest_date_house_with_reserva,
            B.earliest_date_house_with_reserva,
            NOW() AS atualizacao
        FROM tb_houses_parcial AS A
        LEFT JOIN primeira_venda AS B
            ON CONCAT(A.unit_id, A.Administradora) = CONCAT(B.unit_id, B.Administradora)
        WHERE A.unit_id NOT IN (
            747169, 567515, 588400, 588406, 441403, 614452, 747735, 747169, 
            747171, 379240, 747215, 747220, 614526, 747747, 614527, 575254, 
            747224, 747230, 563466, 747235, 614955, 747754, 747249, 747092, 
            747254, 746111, 735680
        );
    """

    # ---------------------------------------------------------
    # EXECUÇÃO
    # ---------------------------------------------------------
    with engine.connect() as connection:
        # tb_houses
        connection.execute(text(sql_drop_tb_houses))
        connection.commit()

        connection.execute(text(sql_create_tb_houses))
        connection.commit()
        print("tb_houses criada/atualizada com sucesso (MySQL).")

        # tb_houses_geral
        connection.execute(text(sql_drop_tb_houses_geral))
        connection.commit()

        connection.execute(text(sql_create_tb_houses_geral))
        connection.commit()
        print("tb_houses_geral criada/atualizada com sucesso (MySQL).")

    engine.dispose()
    print("Conexão MySQL encerrada.")


# ---------------------------------------------------------
# AIRFLOW DAG
# ---------------------------------------------------------
SP_TZ = pendulum.timezone("America/Sao_Paulo")

with DAG(
    dag_id="OVH-tabelas_houses",   # ajuste o nome se quiser
    start_date=pendulum.datetime(2025, 9, 23, 8, 0, tz=SP_TZ),
    schedule="30 6 * * *",         # ex.: todo dia às 06:30 (ajuste se preferir)
    catchup=False,
    tags=["Tabelas - OVH", "tb_houses"],
):

    @task()
    def atualizar_tb_houses_e_geral():
        """
        Task que executa o fluxo principal para recriar
        tb_houses e tb_houses_geral.
        """
        main()

    atualizar_tb_houses_e_geral()


# Se quiser manter a possibilidade de rodar standalone:
if __name__ == "__main__":
    main()
