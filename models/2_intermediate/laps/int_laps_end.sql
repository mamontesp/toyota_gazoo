{{
    config(
        materialized='table'
    )
}}

WITH laps_end_source AS (
    -- Extract lap completion timestamps for Barber Race 1
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
    FROM {{ ref('raw_barber_race_1_lap_end') }}
),

-- Deduplicate lap start records per lap and vehicle keeping the latest metadata
deduplicated_lap_end AS (
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
            ORDER BY timestamp DESC
        ) AS record_rank
    FROM laps_end_source
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
    (timestamp AT TIME ZONE 'EST') AS lap_end_timestamp,
    (timestamp AT TIME ZONE 'UTC') AS lap_end_timestamp_utc,
    vehicle_id,
    vehicle_number,
    circuit,
    race_number
FROM deduplicated_lap_end
WHERE record_rank = 1

