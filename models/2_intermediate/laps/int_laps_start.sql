{{
    config(
        materialized='table'
    )
}}

WITH lap_start_source AS (
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
        vehicle_number,
        'barber' AS circuit,
        1 AS race_number
    FROM {{ ref('raw_barber_race_1_lap_start') }}
    WHERE timestamp IS NOT NULL
),

-- Deduplicate lap start records per lap and vehicle keeping the latest metadata
deduplicated_lap_start AS (
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
        vehicle_number,
        circuit,
        race_number,
        ROW_NUMBER() OVER (
            PARTITION BY lap, vehicle_id
            ORDER BY timestamp
        ) AS record_rank
    FROM lap_start_source
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
    (timestamp AT TIME ZONE 'EST') AS lap_start_timestamp,
    (timestamp AT TIME ZONE 'UTC') AS lap_start_timestamp_utc,
    vehicle_id,
    vehicle_number,
    circuit,
    race_number
FROM deduplicated_lap_start
WHERE record_rank = 1
