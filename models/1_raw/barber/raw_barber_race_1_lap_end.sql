{{
    config(
        materialized='view'
    )
}}

-- Raw lap end timestamps from Barber Race 1
-- Source: R1_barber_lap_end.csv
SELECT
    expire_at,
    lap,
    meta_event,
    meta_session,
    meta_source,
    meta_time,
    original_vehicle_id,
    outing,
    timestamp,
    vehicle_id,
    vehicle_number
FROM {{ source('barber', 'barber_race_1_lap_end') }}

