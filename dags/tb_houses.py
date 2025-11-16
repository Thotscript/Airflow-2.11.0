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

    # Normalização de datas em subqueries A, B, C, F, G
    sql_create_tb_houses = """
        CREATE TABLE tb_houses AS
        SELECT 
            A.id AS unit_id,
            A.Administradora,
            A.date_norm AS Date_House,
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
            B.creation_date_norm AS creation_date,
            B.startdate_norm AS startdate,
            B.enddate_norm AS enddate,
            B.last_updated_norm AS last_updated,
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
            F.Ocupacao AS Ocupação,
            F.Season AS temporada,
            G.minStay AS minstay_actual,
            G.rate AS rate_actual,
            NOW() AS atualizacao_
        FROM (
            SELECT
                id,
                Administradora,
                CASE
                    WHEN `date` REGEXP '^[0-9]{2}/[0-9]{2}/[0-9]{4}$' THEN
                        CASE
                            WHEN CAST(SUBSTRING_INDEX(`date`, '/', 1) AS UNSIGNED) > 12
                            THEN STR_TO_DATE(`date`, '%d/%m/%Y')
                            ELSE STR_TO_DATE(`date`, '%m/%d/%Y')
                        END
                    WHEN `date` REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' THEN
                        STR_TO_DATE(`date`, '%Y-%m-%d')
                    WHEN `date` REGEXP '^[0-9]{2}-[0-9]{2}-[0-9]{4}$' THEN
                        CASE
                            WHEN CAST(SUBSTRING_INDEX(`date`, '-', 1) AS UNSIGNED) > 12
                            THEN STR_TO_DATE(`date`, '%d-%m-%Y')
                            ELSE STR_TO_DATE(`date`, '%m-%d-%Y')
                        END
                    ELSE NULL
                END AS date_norm
            FROM tb_unit_dates
        ) AS A
        LEFT JOIN (
            SELECT
                id,
                unit_id,
                Administradora_x,
                confirmation_id,
                client_id,
                occupants,
                occupants_small,
                email,
                first_name,
                last_name,
                days_number,
                maketype_description,
                type_name,
                status_code,
                price_nightly,
                price_total,
                status_id,
                hear_about_name,
                travelagent_name,
                season,
                price,
                extra,
                discount,
                original_cost,
                reservation_id,
                location_name,
                CASE
                    WHEN `Date` REGEXP '^[0-9]{2}/[0-9]{2}/[0-9]{4}$' THEN
                        CASE
                            WHEN CAST(SUBSTRING_INDEX(`Date`, '/', 1) AS UNSIGNED) > 12
                            THEN STR_TO_DATE(`Date`, '%d/%m/%Y')
                            ELSE STR_TO_DATE(`Date`, '%m/%d/%Y')
                        END
                    WHEN `Date` REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' THEN
                        STR_TO_DATE(`Date`, '%Y-%m-%d')
                    WHEN `Date` REGEXP '^[0-9]{2}-[0-9]{2}-[0-9]{4}$' THEN
                        CASE
                            WHEN CAST(SUBSTRING_INDEX(`Date`, '-', 1) AS UNSIGNED) > 12
                            THEN STR_TO_DATE(`Date`, '%d-%m-%Y')
                            ELSE STR_TO_DATE(`Date`, '%m-%d-%Y')
                        END
                    ELSE NULL
                END AS date_norm,
                CASE
                    WHEN creation_date REGEXP '^[0-9]{2}/[0-9]{2}/[0-9]{4}$' THEN
                        CASE
                            WHEN CAST(SUBSTRING_INDEX(creation_date, '/', 1) AS UNSIGNED) > 12
                            THEN STR_TO_DATE(creation_date, '%d/%m/%Y')
                            ELSE STR_TO_DATE(creation_date, '%m/%d/%Y')
                        END
                    WHEN creation_date REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' THEN
                        STR_TO_DATE(creation_date, '%Y-%m-%d')
                    WHEN creation_date REGEXP '^[0-9]{2}-[0-9]{2}-[0-9]{4}$' THEN
                        CASE
                            WHEN CAST(SUBSTRING_INDEX(creation_date, '-', 1) AS UNSIGNED) > 12
                            THEN STR_TO_DATE(creation_date, '%d-%m-%Y')
                            ELSE STR_TO_DATE(creation_date, '%m-%d-%Y')
                        END
                    ELSE NULL
                END AS creation_date_norm,
                CASE
                    WHEN startdate REGEXP '^[0-9]{2}/[0-9]{2}/[0-9]{4}$' THEN
                        CASE
                            WHEN CAST(SUBSTRING_INDEX(startdate, '/', 1) AS UNSIGNED) > 12
                            THEN STR_TO_DATE(startdate, '%d/%m/%Y')
                            ELSE STR_TO_DATE(startdate, '%m/%d/%Y')
                        END
                    WHEN startdate REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' THEN
                        STR_TO_DATE(startdate, '%Y-%m-%d')
                    WHEN startdate REGEXP '^[0-9]{2}-[0-9]{2}-[0-9]{4}$' THEN
                        CASE
                            WHEN CAST(SUBSTRING_INDEX(startdate, '-', 1) AS UNSIGNED) > 12
                            THEN STR_TO_DATE(startdate, '%d-%m-%Y')
                            ELSE STR_TO_DATE(startdate, '%m-%d-%Y')
                        END
                    ELSE NULL
                END AS startdate_norm,
                CASE
                    WHEN enddate REGEXP '^[0-9]{2}/[0-9]{2}/[0-9]{4}$' THEN
                        CASE
                            WHEN CAST(SUBSTRING_INDEX(enddate, '/', 1) AS UNSIGNED) > 12
                            THEN STR_TO_DATE(enddate, '%d/%m/%Y')
                            ELSE STR_TO_DATE(enddate, '%m/%d/%Y')
                        END
                    WHEN enddate REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' THEN
                        STR_TO_DATE(enddate, '%Y-%m-%d')
                    WHEN enddate REGEXP '^[0-9]{2}-[0-9]{2}-[0-9]{4}$' THEN
                        CASE
                            WHEN CAST(SUBSTRING_INDEX(enddate, '-', 1) AS UNSIGNED) > 12
                            THEN STR_TO_DATE(enddate, '%d-%m-%Y')
                            ELSE STR_TO_DATE(enddate, '%m-%d-%Y')
                        END
                    ELSE NULL
                END AS enddate_norm,
                CASE
                    WHEN last_updated REGEXP '^[0-9]{2}/[0-9]{2}/[0-9]{4}$' THEN
                        CASE
                            WHEN CAST(SUBSTRING_INDEX(last_updated, '/', 1) AS UNSIGNED) > 12
                            THEN STR_TO_DATE(last_updated, '%d/%m/%Y')
                            ELSE STR_TO_DATE(last_updated, '%m/%d/%Y')
                        END
                    WHEN last_updated REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' THEN
                        STR_TO_DATE(last_updated, '%Y-%m-%d')
                    WHEN last_updated REGEXP '^[0-9]{2}-[0-9]{2}-[0-9]{4}$' THEN
                        CASE
                            WHEN CAST(SUBSTRING_INDEX(last_updated, '-', 1) AS UNSIGNED) > 12
                            THEN STR_TO_DATE(last_updated, '%d-%m-%Y')
                            ELSE STR_TO_DATE(last_updated, '%m-%d-%Y')
                        END
                    ELSE NULL
                END AS last_updated_norm
            FROM tb_reservas_por_data
        ) AS B
            ON A.id = B.unit_id
           AND A.Administradora = B.Administradora_x
           AND A.date_norm IS NOT NULL
           AND B.date_norm IS NOT NULL
           AND DATE_FORMAT(A.date_norm, '%Y%m%d') = DATE_FORMAT(B.date_norm, '%Y%m%d')
        LEFT JOIN (
            SELECT
                id_unit,
                Administradora,
                `rate_type.season`,
                `rate_type.rate`,
                `rate_type.minStay`,
                CASE
                    WHEN `date` REGEXP '^[0-9]{2}/[0-9]{2}/[0-9]{4}$' THEN
                        CASE
                            WHEN CAST(SUBSTRING_INDEX(`date`, '/', 1) AS UNSIGNED) > 12
                            THEN STR_TO_DATE(`date`, '%d/%m/%Y')
                            ELSE STR_TO_DATE(`date`, '%m/%d/%Y')
                        END
                    WHEN `date` REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' THEN
                        STR_TO_DATE(`date`, '%Y-%m-%d')
                    WHEN `date` REGEXP '^[0-9]{2}-[0-9]{2}-[0-9]{4}$' THEN
                        CASE
                            WHEN CAST(SUBSTRING_INDEX(`date`, '-', 1) AS UNSIGNED) > 12
                            THEN STR_TO_DATE(`date`, '%d-%m-%Y')
                            ELSE STR_TO_DATE(`date`, '%m-%d-%Y')
                        END
                    ELSE NULL
                END AS date_norm
            FROM tb_metas
        ) AS C
            ON A.id = C.id_unit
           AND A.Administradora = C.Administradora
           AND A.date_norm IS NOT NULL
           AND C.date_norm IS NOT NULL
           AND DATE_FORMAT(A.date_norm, '%Y%m%d') = DATE_FORMAT(C.date_norm, '%Y%m%d')
        LEFT JOIN tb_property_list_wordpress AS D
            ON A.id = D.id
           AND A.Administradora = D.Administradora
        LEFT JOIN (
            SELECT
                `Data`,
                Administradora,
                Ocupação AS Ocupacao,
                Season,
                CASE
                    WHEN `Data` REGEXP '^[0-9]{2}/[0-9]{2}/[0-9]{4}$' THEN
                        CASE
                            WHEN CAST(SUBSTRING_INDEX(`Data`, '/', 1) AS UNSIGNED) > 12
                            THEN STR_TO_DATE(`Data`, '%d/%m/%Y')
                            ELSE STR_TO_DATE(`Data`, '%m/%d/%Y')
                        END
                    WHEN `Data` REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' THEN
                        STR_TO_DATE(`Data`, '%Y-%m-%d')
                    WHEN `Data` REGEXP '^[0-9]{2}-[0-9]{2}-[0-9]{4}$' THEN
                        CASE
                            WHEN CAST(SUBSTRING_INDEX(`Data`, '-', 1) AS UNSIGNED) > 12
                            THEN STR_TO_DATE(`Data`, '%d-%m-%Y')
                            ELSE STR_TO_DATE(`Data`, '%m-%d-%Y')
                        END
                    ELSE NULL
                END AS data_norm
            FROM tb_ocupacao_meta
        ) AS F
            ON A.Administradora = F.Administradora
           AND A.date_norm IS NOT NULL
           AND F.data_norm IS NOT NULL
           AND DATE_FORMAT(A.date_norm, '%Y%m%d') = DATE_FORMAT(F.data_norm, '%Y%m%d')
        LEFT JOIN (
            SELECT
                id_unit,
                Administradora,
                minStay,
                rate,
                CASE
                    WHEN `date` REGEXP '^[0-9]{2}/[0-9]{2}/[0-9]{4}$' THEN
                        CASE
                            WHEN CAST(SUBSTRING_INDEX(`date`, '/', 1) AS UNSIGNED) > 12
                            THEN STR_TO_DATE(`date`, '%d/%m/%Y')
                            ELSE STR_TO_DATE(`date`, '%m/%d/%Y')
                        END
                    WHEN `date` REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' THEN
                        STR_TO_DATE(`date`, '%Y-%m-%d')
                    WHEN `date` REGEXP '^[0-9]{2}-[0-9]{2}-[0-9]{4}$' THEN
                        CASE
                            WHEN CAST(SUBSTRING_INDEX(`date`, '-', 1) AS UNSIGNED) > 12
                            THEN STR_TO_DATE(`date`, '%d-%m-%Y')
                            ELSE STR_TO_DATE(`date`, '%m-%d-%Y')
                        END
                    ELSE NULL
                END AS date_norm
            FROM tb_price
        ) AS G
            ON A.id = G.id_unit
           AND A.Administradora = G.Administradora
           AND A.date_norm IS NOT NULL
           AND G.date_norm IS NOT NULL
           AND DATE_FORMAT(A.date_norm, '%Y%m%d') = DATE_FORMAT(G.date_norm, '%Y%m%d');
    """

    # ---------------------------------------------------------
    # 2) tb_houses_geral
    # ---------------------------------------------------------
    sql_drop_tb_houses_geral = """
        DROP TABLE IF EXISTS tb_houses_geral;
    """

    sql_create_tb_houses_geral = """
        CREATE TABLE tb_houses_geral AS
        WITH tb_houses_parcial AS (
            SELECT
                A.id AS unit_id,
                A.Administradora,
                A.date_norm AS Date_House,
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
                B.creation_date_norm AS creation_date,
                B.startdate_norm AS startdate,
                B.enddate_norm AS enddate,
                B.last_updated_norm AS last_updated,
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
                F.Ocupacao AS Ocupação,
                F.`Holiday for Year` AS Holiday_for_Year,
                F.`Name of Holiday` AS Name_of_Holiday,
                F.`Number Of Day Holidays` AS Number_Of_Day_Holidays,
                F.Season AS temporada,
                F.Temporada AS Temporada_Intervalo,
                G.minStay AS minstay_actual,
                G.rate AS rate_actual,
                COALESCE(H.Status, 'Disponivel') AS Status_Vendas,
                YEAR(A.date_norm) AS Ano,
                MONTH(A.date_norm) AS Mes,
                CASE
                    WHEN A.date_norm >= CURDATE() THEN 1
                    ELSE 0
                END AS Considerar,
                COALESCE(I.value, 'NÃO') AS valido_para_meta,
                J.Canal,
                DATEDIFF(A.date_norm, B.creation_date_norm) AS Advp_Dias,
                CASE
                    WHEN DATEDIFF(A.date_norm, B.creation_date_norm) IS NULL THEN '-'
                    WHEN DATEDIFF(A.date_norm, B.creation_date_norm) <= 30 THEN '<= 30 days'
                    WHEN DATEDIFF(A.date_norm, B.creation_date_norm) <= 60 THEN '<= 60 days'
                    WHEN DATEDIFF(A.date_norm, B.creation_date_norm) <= 90 THEN '<= 90 days'
                    WHEN DATEDIFF(A.date_norm, B.creation_date_norm) <= 120 THEN '<= 120 days'
                    WHEN DATEDIFF(A.date_norm, B.creation_date_norm) <= 180 THEN '<= 180 days'
                    ELSE 'Acima de 180 days'
                END AS Advp_Faixas,
                CASE
                    WHEN DATEDIFF(A.date_norm, B.creation_date_norm) IS NULL THEN '-'
                    WHEN DATEDIFF(A.date_norm, B.creation_date_norm) <= 30 THEN 1
                    WHEN DATEDIFF(A.date_norm, B.creation_date_norm) <= 60 THEN 2
                    WHEN DATEDIFF(A.date_norm, B.creation_date_norm) <= 90 THEN 3
                    WHEN DATEDIFF(A.date_norm, B.creation_date_norm) <= 120 THEN 4
                    WHEN DATEDIFF(A.date_norm, B.creation_date_norm) <= 180 THEN 5
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
                DATE_FORMAT(A.date_norm, '%m. %b') AS Mes_Classificado_Check_In,
                L.Dia_Ajustado AS Dia_Ajustado_Check_in,
                NOW() AS atualizacao_
            FROM (
                SELECT
                    id,
                    Administradora,
                    CASE
                        WHEN `date` REGEXP '^[0-9]{2}/[0-9]{2}/[0-9]{4}$' THEN
                            CASE
                                WHEN CAST(SUBSTRING_INDEX(`date`, '/', 1) AS UNSIGNED) > 12
                                THEN STR_TO_DATE(`date`, '%d/%m/%Y')
                                ELSE STR_TO_DATE(`date`, '%m/%d/%Y')
                            END
                        WHEN `date` REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' THEN
                            STR_TO_DATE(`date`, '%Y-%m-%d')
                        WHEN `date` REGEXP '^[0-9]{2}-[0-9]{2}-[0-9]{4}$' THEN
                            CASE
                                WHEN CAST(SUBSTRING_INDEX(`date`, '-', 1) AS UNSIGNED) > 12
                                THEN STR_TO_DATE(`date`, '%d-%m-%Y')
                                ELSE STR_TO_DATE(`date`, '%m-%d-%Y')
                            END
                        ELSE NULL
                    END AS date_norm
                FROM tb_unit_dates
            ) AS A
            LEFT JOIN (
                SELECT
                    id,
                    unit_id,
                    Administradora_x,
                    confirmation_id,
                    client_id,
                    occupants,
                    occupants_small,
                    email,
                    first_name,
                    last_name,
                    days_number,
                    maketype_description,
                    type_name,
                    status_code,
                    price_nightly,
                    price_total,
                    status_id,
                    hear_about_name,
                    travelagent_name,
                    season,
                    price,
                    extra,
                    discount,
                    original_cost,
                    reservation_id,
                    location_name,
                    CASE
                        WHEN `Date` REGEXP '^[0-9]{2}/[0-9]{2}/[0-9]{4}$' THEN
                            CASE
                                WHEN CAST(SUBSTRING_INDEX(`Date`, '/', 1) AS UNSIGNED) > 12
                                THEN STR_TO_DATE(`Date`, '%d/%m/%Y')
                                ELSE STR_TO_DATE(`Date`, '%m/%d/%Y')
                            END
                        WHEN `Date` REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' THEN
                            STR_TO_DATE(`Date`, '%Y-%m-%d')
                        WHEN `Date` REGEXP '^[0-9]{2}-[0-9]{2}-[0-9]{4}$' THEN
                            CASE
                                WHEN CAST(SUBSTRING_INDEX(`Date`, '-', 1) AS UNSIGNED) > 12
                                THEN STR_TO_DATE(`Date`, '%d-%m-%Y')
                                ELSE STR_TO_DATE(`Date`, '%m-%d-%Y')
                            END
                        ELSE NULL
                    END AS date_norm,
                    CASE
                        WHEN creation_date REGEXP '^[0-9]{2}/[0-9]{2}/[0-9]{4}$' THEN
                            CASE
                                WHEN CAST(SUBSTRING_INDEX(creation_date, '/', 1) AS UNSIGNED) > 12
                                THEN STR_TO_DATE(creation_date, '%d/%m/%Y')
                                ELSE STR_TO_DATE(creation_date, '%m/%d/%Y')
                            END
                        WHEN creation_date REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' THEN
                            STR_TO_DATE(creation_date, '%Y-%m-%d')
                        WHEN creation_date REGEXP '^[0-9]{2}-[0-9]{2}-[0-9]{4}$' THEN
                            CASE
                                WHEN CAST(SUBSTRING_INDEX(creation_date, '-', 1) AS UNSIGNED) > 12
                                THEN STR_TO_DATE(creation_date, '%d-%m-%Y')
                                ELSE STR_TO_DATE(creation_date, '%m-%d-%Y')
                            END
                        ELSE NULL
                    END AS creation_date_norm,
                    CASE
                        WHEN startdate REGEXP '^[0-9]{2}/[0-9]{2}/[0-9]{4}$' THEN
                            CASE
                                WHEN CAST(SUBSTRING_INDEX(startdate, '/', 1) AS UNSIGNED) > 12
                                THEN STR_TO_DATE(startdate, '%d/%m/%Y')
                                ELSE STR_TO_DATE(startdate, '%m/%d/%Y')
                            END
                        WHEN startdate REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' THEN
                            STR_TO_DATE(startdate, '%Y-%m-%d')
                        WHEN startdate REGEXP '^[0-9]{2}-[0-9]{2}-[0-9]{4}$' THEN
                            CASE
                                WHEN CAST(SUBSTRING_INDEX(startdate, '-', 1) AS UNSIGNED) > 12
                                THEN STR_TO_DATE(startdate, '%d-%m-%Y')
                                ELSE STR_TO_DATE(startdate, '%m-%d-%Y')
                            END
                        ELSE NULL
                    END AS startdate_norm,
                    CASE
                        WHEN enddate REGEXP '^[0-9]{2}/[0-9]{2}/[0-9]{4}$' THEN
                            CASE
                                WHEN CAST(SUBSTRING_INDEX(enddate, '/', 1) AS UNSIGNED) > 12
                                THEN STR_TO_DATE(enddate, '%d/%m/%Y')
                                ELSE STR_TO_DATE(enddate, '%m/%d/%Y')
                            END
                        WHEN enddate REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' THEN
                            STR_TO_DATE(enddate, '%Y-%m-%d')
                        WHEN enddate REGEXP '^[0-9]{2}-[0-9]{2}-[0-9]{4}$' THEN
                            CASE
                                WHEN CAST(SUBSTRING_INDEX(enddate, '-', 1) AS UNSIGNED) > 12
                                THEN STR_TO_DATE(enddate, '%d-%m-%Y')
                                ELSE STR_TO_DATE(enddate, '%m-%d-%Y')
                            END
                        ELSE NULL
                    END AS enddate_norm,
                    CASE
                        WHEN last_updated REGEXP '^[0-9]{2}/[0-9]{2}/[0-9]{4}$' THEN
                            CASE
                                WHEN CAST(SUBSTRING_INDEX(last_updated, '/', 1) AS UNSIGNED) > 12
                                THEN STR_TO_DATE(last_updated, '%d/%m/%Y')
                                ELSE STR_TO_DATE(last_updated, '%m/%d/%Y')
                            END
                        WHEN last_updated REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' THEN
                            STR_TO_DATE(last_updated, '%Y-%m-%d')
                        WHEN last_updated REGEXP '^[0-9]{2}-[0-9]{2}-[0-9]{4}$' THEN
                            CASE
                                WHEN CAST(SUBSTRING_INDEX(last_updated, '-', 1) AS UNSIGNED) > 12
                                THEN STR_TO_DATE(last_updated, '%d-%m-%Y')
                                ELSE STR_TO_DATE(last_updated, '%m-%d-%Y')
                            END
                        ELSE NULL
                    END AS last_updated_norm
                FROM tb_reservas_por_data
            ) AS B
                ON A.id = B.unit_id
               AND A.Administradora = B.Administradora_x
               AND A.date_norm IS NOT NULL
               AND B.date_norm IS NOT NULL
               AND DATE_FORMAT(A.date_norm, '%Y%m%d') = DATE_FORMAT(B.date_norm, '%Y%m%d')
            LEFT JOIN (
                SELECT
                    id_unit,
                    Administradora,
                    `rate_type.season`,
                    `rate_type.rate`,
                    `rate_type.minStay`,
                    CASE
                        WHEN `date` REGEXP '^[0-9]{2}/[0-9]{2}/[0-9]{4}$' THEN
                            CASE
                                WHEN CAST(SUBSTRING_INDEX(`date`, '/', 1) AS UNSIGNED) > 12
                                THEN STR_TO_DATE(`date`, '%d/%m/%Y')
                                ELSE STR_TO_DATE(`date`, '%m/%d/%Y')
                            END
                        WHEN `date` REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' THEN
                            STR_TO_DATE(`date`, '%Y-%m-%d')
                        WHEN `date` REGEXP '^[0-9]{2}-[0-9]{2}-[0-9]{4}$' THEN
                            CASE
                                WHEN CAST(SUBSTRING_INDEX(`date`, '-', 1) AS UNSIGNED) > 12
                                THEN STR_TO_DATE(`date`, '%d-%m-%Y')
                                ELSE STR_TO_DATE(`date`, '%m-%d-%Y')
                            END
                        ELSE NULL
                    END AS date_norm
                FROM tb_metas
            ) AS C
                ON A.id = C.id_unit
               AND A.Administradora = C.Administradora
               AND A.date_norm IS NOT NULL
               AND C.date_norm IS NOT NULL
               AND DATE_FORMAT(A.date_norm, '%Y%m%d') = DATE_FORMAT(C.date_norm, '%Y%m%d')
            LEFT JOIN tb_depara_casas AS D
                ON A.id = D.id
               AND A.Administradora = D.Administradora
            LEFT JOIN (
                SELECT
                    `Data`,
                    Administradora,
                    Ocupação AS Ocupacao,
                    `Holiday for Year`,
                    `Name of Holiday`,
                    `Number Of Day Holidays`,
                    Season,
                    Temporada,
                    CASE
                        WHEN `Data` REGEXP '^[0-9]{2}/[0-9]{2}/[0-9]{4}$' THEN
                            CASE
                                WHEN CAST(SUBSTRING_INDEX(`Data`, '/', 1) AS UNSIGNED) > 12
                                THEN STR_TO_DATE(`Data`, '%d/%m/%Y')
                                ELSE STR_TO_DATE(`Data`, '%m/%d/%Y')
                            END
                        WHEN `Data` REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' THEN
                            STR_TO_DATE(`Data`, '%Y-%m-%d')
                        WHEN `Data` REGEXP '^[0-9]{2}-[0-9]{2}-[0-9]{4}$' THEN
                            CASE
                                WHEN CAST(SUBSTRING_INDEX(`Data`, '-', 1) AS UNSIGNED) > 12
                                THEN STR_TO_DATE(`Data`, '%d-%m-%Y')
                                ELSE STR_TO_DATE(`Data`, '%m-%d-%Y')
                            END
                        ELSE NULL
                    END AS data_norm
                FROM tb_ocupacao_meta
            ) AS F
                ON A.Administradora = F.Administradora
               AND A.date_norm IS NOT NULL
               AND F.data_norm IS NOT NULL
               AND DATE_FORMAT(A.date_norm, '%Y%m%d') = DATE_FORMAT(F.data_norm, '%Y%m%d')
            LEFT JOIN (
                SELECT
                    id_unit,
                    Administradora,
                    minStay,
                    rate,
                    CASE
                        WHEN `date` REGEXP '^[0-9]{2}/[0-9]{2}/[0-9]{4}$' THEN
                            CASE
                                WHEN CAST(SUBSTRING_INDEX(`date`, '/', 1) AS UNSIGNED) > 12
                                THEN STR_TO_DATE(`date`, '%d/%m/%Y')
                                ELSE STR_TO_DATE(`date`, '%m/%d/%Y')
                            END
                        WHEN `date` REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' THEN
                            STR_TO_DATE(`date`, '%Y-%m-%d')
                        WHEN `date` REGEXP '^[0-9]{2}-[0-9]{2}-[0-9]{4}$' THEN
                            CASE
                                WHEN CAST(SUBSTRING_INDEX(`date`, '-', 1) AS UNSIGNED) > 12
                                THEN STR_TO_DATE(`date`, '%d-%m-%Y')
                                ELSE STR_TO_DATE(`date`, '%m-%d-%Y')
                            END
                        ELSE NULL
                    END AS date_norm
                FROM tb_price
            ) AS G
                ON A.id = G.id_unit
               AND A.Administradora = G.Administradora
               AND A.date_norm IS NOT NULL
               AND G.date_norm IS NOT NULL
               AND DATE_FORMAT(A.date_norm, '%Y%m%d') = DATE_FORMAT(G.date_norm, '%Y%m%d')
            LEFT JOIN tb_status_vendas AS H
                ON B.type_name = H.type_name
            LEFT JOIN tb_filtro_metas AS I
                ON A.id = I.id_unit
            LEFT JOIN tb_depara_canais AS J
                ON B.type_name = J.Status
            LEFT JOIN (
                SELECT
                    AnoMes,
                    Dia_Ajustado,
                    YTD,
                    MTD,
                    L7D,
                    SEMANA,
                    `Ano Comparação`,
                    CASE
                        WHEN `data` REGEXP '^[0-9]{2}/[0-9]{2}/[0-9]{4}$' THEN
                            CASE
                                WHEN CAST(SUBSTRING_INDEX(`data`, '/', 1) AS UNSIGNED) > 12
                                THEN STR_TO_DATE(`data`, '%d/%m/%Y')
                                ELSE STR_TO_DATE(`data`, '%m/%d/%Y')
                            END
                        WHEN `data` REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' THEN
                            STR_TO_DATE(`data`, '%Y-%m-%d')
                        WHEN `data` REGEXP '^[0-9]{2}-[0-9]{2}-[0-9]{4}$' THEN
                            CASE
                                WHEN CAST(SUBSTRING_INDEX(`data`, '-', 1) AS UNSIGNED) > 12
                                THEN STR_TO_DATE(`data`, '%d-%m-%Y')
                                ELSE STR_TO_DATE(`data`, '%m-%d-%Y')
                            END
                        ELSE NULL
                    END AS data_norm
                FROM tb_calendario
            ) AS K
                ON B.creation_date_norm IS NOT NULL
               AND K.data_norm IS NOT NULL
               AND DATE_FORMAT(B.creation_date_norm, '%Y%m%d') = DATE_FORMAT(K.data_norm, '%Y%m%d')
            LEFT JOIN (
                SELECT
                    Dia_Ajustado,
                    `Ano Comparação`,
                    CASE
                        WHEN `data` REGEXP '^[0-9]{2}/[0-9]{2}/[0-9]{4}$' THEN
                            CASE
                                WHEN CAST(SUBSTRING_INDEX(`data`, '/', 1) AS UNSIGNED) > 12
                                THEN STR_TO_DATE(`data`, '%d/%m/%Y')
                                ELSE STR_TO_DATE(`data`, '%m/%d/%Y')
                            END
                        WHEN `data` REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' THEN
                            STR_TO_DATE(`data`, '%Y-%m-%d')
                        WHEN `data` REGEXP '^[0-9]{2}-[0-9]{2}-[0-9]{4}$' THEN
                            CASE
                                WHEN CAST(SUBSTRING_INDEX(`data`, '-', 1) AS UNSIGNED) > 12
                                THEN STR_TO_DATE(`data`, '%d-%m-%Y')
                                ELSE STR_TO_DATE(`data`, '%m-%d-%Y')
                            END
                        ELSE NULL
                    END AS data_norm
                FROM tb_calendario
            ) AS L
                ON A.date_norm IS NOT NULL
               AND L.data_norm IS NOT NULL
               AND DATE_FORMAT(A.date_norm, '%Y%m%d') = DATE_FORMAT(L.data_norm, '%Y%m%d')
        ),
        primeira_venda AS (
            SELECT
                unit_id,
                Administradora,
                MIN(creation_date) AS earliest_creation_date,
                MAX(Date_House) AS latest_date_house_with_reserva,
                MIN(Date_House) AS earliest_date_house_with_reserva,
                NOW() AS atualizacao
            FROM tb_houses_parcial
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
          ON A.unit_id = B.unit_id
         AND A.Administradora = B.Administradora
        WHERE A.unit_id NOT IN (
            747169, 567515, 588400, 588406, 441403, 614452, 747735, 747169, 
            747171, 379240, 747215, 747220, 614526, 747747, 614527, 575254, 
            747224, 747230, 563466, 747235, 614955, 747754, 747249, 747092, 
            747254, 746111, 735680
        );
    """

    try:
        # Transação única para toda a recriação
        with engine.begin() as connection:
            # ⚠️ Desabilita STRICT na sessão pra não quebrar em datas zoada
            connection.execute(
                text(
                    """
                    SET SESSION sql_mode = REPLACE(
                        REPLACE(@@sql_mode, 'STRICT_TRANS_TABLES', ''),
                        'STRICT_ALL_TABLES',
                        ''
                    );
                    """
                )
            )

            # tb_houses (tabela de apoio)
            connection.execute(text(sql_drop_tb_houses))
            connection.execute(text(sql_create_tb_houses))
            print("tb_houses criada/atualizada com sucesso (MySQL).")

            # tb_houses_geral
            connection.execute(text(sql_drop_tb_houses_geral))
            connection.execute(text(sql_create_tb_houses_geral))
            print("tb_houses_geral criada/atualizada com sucesso (MySQL).")

    finally:
        engine.dispose()
        print("Conexão MySQL encerrada.")


# ---------------------------------------------------------
# AIRFLOW DAG
# ---------------------------------------------------------
SP_TZ = pendulum.timezone("America/Sao_Paulo")

with DAG(
    dag_id="OVH-tabelas_houses",
    start_date=pendulum.datetime(2025, 9, 23, 8, 0, tz=SP_TZ),
    schedule="30 7 * * *",  # todo dia 06:30
    catchup=False,
    tags=["Tabelas - OVH", "tb_houses"],
):

    @task()
    def atualizar_tb_houses_e_geral():
        main()

    atualizar_tb_houses_e_geral()


if __name__ == "__main__":
    main()
