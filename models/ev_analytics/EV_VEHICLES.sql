{{
    config(
        materialized='table',
        database='NZ_EV_ANALYSIS',
        schema='INTEGRATION'
    )
}}
with ev_vehicles as (
SELECT 
objectid,
to_number(first_nz_registration_year) as first_nz_registration_year,
to_number(first_nz_registration_month) as first_nz_registration_month,
to_date(to_number(first_nz_registration_year)||'-'||to_number(first_nz_registration_month)||'-01') as registration_date,
VIN11,
ENGINE_NUMBER,
MVMA_MODEL_CODE,
upper(TLA) AS TLA,
make,
model,
submodel,
motive_power,
body_type,
cc_rating,
chassis7,
basic_colour,
class,
vehicle_type,
industry_class,
industry_model_code,
import_status,
nz_assembled,
original_country,
previous_country,
vehicle_year
FROM {{ source('NZ_EV_ANALYSIS', 'MOTOR_VEHICLE_REGISTER') }}
where motive_power in ('PLUGIN PETROL HYBRID','ELECTRIC','ELECTRIC [PETROL EXTENDED]')
and NVL(vin11, '') <> ''
)

select *
from ev_vehicles