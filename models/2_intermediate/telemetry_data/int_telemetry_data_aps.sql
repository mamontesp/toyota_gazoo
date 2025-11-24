{{
    config(
        materialized='table'
    )
}}

-- Accelerator Pedal Sensor (APS) telemetry data from Barber Race 1
SELECT
    expire_at,
    lap,
    meta_event,
    meta_session,
    meta_source,
    meta_time,
    original_vehicle_id,
    outing,
    telemetry_value AS aps,
    timestamp,
    vehicle_id,
    vehicle_number,
    'barber' AS circuit,
    1 AS race_number
FROM {{ ref('raw_barber_race_1_telemetry_data') }}
WHERE telemetry_name = 'aps'

