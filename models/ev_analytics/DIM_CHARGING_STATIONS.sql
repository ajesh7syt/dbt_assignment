{{
    config(
        materialized='table',
        database='NZ_EV_ANALYSIS',
        schema='PRESENTATION'
    )
}}

WITH dim_cs AS (

select 
CS.OBJECTID,
CS.NAME,
CS.OPERATOR,
CS.OWNER,
replace(upper(nz.territorial_authority),'ō','o') as territorial_authority, 
CS.ADDRESS,
nz.full_address as NZ_full_address ,
st_distance(ST_POINT(cs.longitude, cs.latitude),ST_GEOGRAPHYFROMWKT(nz.wkt)) Closest_distance,
CS.IS_24HOURS,
CS.CARPARK_COUNT,
CS.HAS_CARPARK_COST,
CS.MAX_TIME_LIMIT,
CS.HAS_TOURIST_ATTRACTION,
CS.LATITUDE,
CS.LONGITUDE,
CS.CURRENT_TYPE,
CS.DATE_FIRST_OPERATIONAL,
CS.NUMBER_OF_CONNECTORS,
CS.CONNECTORS_LIST,
CS.HAS_CHARGING_COST,
CS.GLOBAL_ID,
CS.SYSTEM_FILE_NAME,
CS.SYSTEM_FILE_ROW_NUMBER,
CURRENT_TIMESTAMP,
CURRENT_USER
from {{ source('NZ_EV_ANALYSIS', 'NZ_ADDRESSES') }}  nz
inner join (select distinct tla from  {{ ref('EV_VEHICLES') }}) veh on replace(lower(nz.territorial_authority),'ō','o')=lower(veh.tla)
right join  {{ ref('EV_CHARING_STATIONS') }} cs on st_distance(ST_POINT(cs.longitude, cs.latitude),ST_GEOGRAPHYFROMWKT(nz.wkt)) <1000

where nz.territorial_authority is not null
qualify row_number() over (partition by cs.objectid order by st_distance(ST_POINT(cs.longitude, cs.latitude),ST_GEOGRAPHYFROMWKT(nz.wkt)) ) =1


)

SELECT *
from dim_cs