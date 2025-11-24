{{
    config(
        materialized='table'
    )
}}

-- Speed telemetry data from Barber Race 1
SELECT
    expire_at,
    lap,
    meta_event,
    meta_session,
    meta_source,
    meta_time,
    original_vehicle_id,
    outing,
    telemetry_value AS speed,
    timestamp,
    vehicle_id,
    vehicle_number,
    'barber' AS circuit,
    1 AS race_number
FROM {{ ref('raw_barber_race_1_telemetry_data') }}
WHERE telemetry_name = 'speed'

