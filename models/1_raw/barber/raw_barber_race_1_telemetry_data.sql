{{
    config(
        materialized='view'
    )
}}

-- Raw telemetry data from Barber Race 1
-- Source: R1_barber_telemetry_data.csv
SELECT
    expire_at,
    lap,
    meta_event,
    meta_session,
    meta_source,
    meta_time,
    original_vehicle_id,
    outing,
    telemetry_name,
    telemetry_value,
    timestamp,
    vehicle_id,
    vehicle_number
FROM {{ source('barber', 'barber_race_1_telemetry_data') }}
