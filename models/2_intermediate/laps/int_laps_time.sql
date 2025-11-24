{{
    config(
        materialized='table'
    )
}}

-- Lap time telemetry events for Barber Race 1
WITH laps_time AS (
    -- Extract lap time event timestamps for Barber Race 1
    SELECT
        expire_at,
        lap,
        meta_event,
        meta_session,
        meta_source,
        meta_time,
        original_vehicle_id,
        outing,
        timestamp AS lap_time_timestamp,
        vehicle_id,
        vehicle_number,
        'barber' AS circuit,
        1 AS race_number
    FROM {{ ref('raw_barber_race_1_lap_time') }}
)

SELECT
    expire_at,
    lap,
    meta_event,
    meta_session,
    meta_source,
    meta_time,
    original_vehicle_id,
    outing,
    lap_time_timestamp,
    vehicle_id,
    vehicle_number,
    circuit,
    race_number
FROM laps_time
WHERE lap_time_timestamp IS NOT NULL

