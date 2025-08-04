select * from {{ source('NZ_EV_ANALYSIS', 'EV_ROAM_CHARING_STATIONS_JSON') }}
