{{
    config(
        materialized='table',
        database='NZ_EV_ANALYSIS',
        schema='PRESENTATION'
    )
}}

WITH date_range AS (
    SELECT
        DATEADD(DAY, SEQ4(), TO_DATE('2010-01-01')) AS date_day
    FROM TABLE(GENERATOR(ROWCOUNT => 10950))  -- ~30 years of dates
)

SELECT
    date_day,
    YEAR(date_day) AS year,
    MONTH(date_day) AS month,
    DAY(date_day) AS day,
    TO_CHAR(date_day, 'Day') AS day_name,
    TO_CHAR(date_day, 'Month') AS month_name,
    DAYOFWEEK(date_day) AS day_of_week,
    QUARTER(date_day) AS quarter,
    CASE
        WHEN DAYOFWEEK(date_day) IN (1, 7) THEN TRUE
        ELSE FALSE
    END AS is_weekend,
    CASE
        WHEN MONTH(date_day) BETWEEN 4 AND 9 THEN 'NZST'
        ELSE 'NZDT'
    END AS nz_timezone
FROM
    date_range