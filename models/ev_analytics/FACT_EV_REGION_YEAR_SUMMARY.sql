{{
    config(
        materialized='table',
        database='NZ_EV_ANALYSIS',
        schema='PRESENTATION'
    )
}}

with cs_count as (
--cumilative sum of connectors by region over years.
SELECT distinct
year(t1.date_first_operational) AS year,
t1.territorial_authority as region,
SUM(t1.number_of_connectors) OVER (partition by t1.territorial_authority ORDER BY year(t1.date_first_operational)) AS charging_CONNECTOR_cumulative_sum,
COUNT(t1.OBJECTID) OVER (partition by t1.territorial_authority ORDER BY year(t1.date_first_operational)) AS charging_station_cumulative_sum,
FROM {{ ref('DIM_CHARGING_STATIONS') }} t1
--where t1.territorial_authority='auckland'
--order by 1,2
),
ev_count as (
--cumilative sum of EV cars by region over years.
SELECT distinct
TO_NUMBER(t1.first_nz_registration_year) AS year,
t1.tla  as region,
count(t1.OBJECTID) OVER (partition by t1.tla ORDER BY TO_NUMBER(t1.first_nz_registration_year)) AS ev_cumulative_sum
FROM {{ ref('DIM_EV_VEHICLES') }} t1
--where t1.territorial_authority='auckland'
order by 1,2 desc
)

select e.YEAR as VEHICLE_YEAR,
e.REGION as VEHICLE_REGION,
e.ev_cumulative_sum as EV_VEH_CUM_SUM,
c.YEar as CHARGING_STATION_YEAR,
c.REGION as CHARGING_STATION_REGION,
c.charging_CONNECTOR_cumulative_sum as CHARGING_CONN_CUM_SUM,
c.charging_station_cumulative_sum as CHARGING_STATION_CUM_SUM
from ev_count e
left outer join cs_count c on e.year=c.year and e.region = c.region
