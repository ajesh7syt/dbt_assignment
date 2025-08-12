{{
    config(
        materialized='table',
        database='NZ_EV_ANALYSIS',
        schema='PRESENTATION'
    )
}}

WITH DIM_EV_VEHICLES AS (

select *
FROM {{ ref('EV_VEHICLES') }} ev
where  ev.TLA is not null
)

SELECT *
from DIM_EV_VEHICLES