{{
    config(
        materialized='table'
    )
}}

-- Fact table for vehicle position data
-- Deduplicates and combines longitude and latitude coordinates
-- Each row represents a unique position timestamp with complete coordinates

WITH 
-- Deduplicate longitude data - keep most recent by meta_time
vbox_long_ranked AS (
    SELECT
        circuit,
        race_number,
        vehicle_id,
        vehicle_number,
        timestamp,
        meta_time,
        vbox_long_minutes,
        lap,
        outing,
        meta_event,
        meta_session,
        ROW_NUMBER() OVER (
            PARTITION BY circuit, race_number, vehicle_id, timestamp
            ORDER BY meta_time DESC
        ) AS rn
    FROM {{ ref('int_telemetry_data_vbox_long_minutes') }}
),

vbox_long_deduped AS (
    SELECT
        circuit,
        race_number,
        vehicle_id,
        vehicle_number,
        timestamp,
        meta_time,
        vbox_long_minutes,
        lap,
        outing,
        meta_event,
        meta_session
    FROM vbox_long_ranked
    WHERE rn = 1
),

-- Deduplicate latitude data - keep most recent by meta_time
vbox_lat_ranked AS (
    SELECT
        circuit,
        race_number,
        vehicle_id,
        timestamp,
        meta_time,
        vbox_lat_minutes,
        ROW_NUMBER() OVER (
            PARTITION BY circuit, race_number, vehicle_id, timestamp
            ORDER BY meta_time DESC
        ) AS rn
    FROM {{ ref('int_telemetry_data_vbox_lat_minutes') }}
),

vbox_lat_deduped AS (
    SELECT
        circuit,
        race_number,
        vehicle_id,
        timestamp,
        meta_time,
        vbox_lat_minutes
    FROM vbox_lat_ranked
    WHERE rn = 1
),

-- Combine coordinates - FULL OUTER JOIN to get all timestamps from both streams
position_combined AS (
    SELECT
        COALESCE(long.circuit, lat.circuit) AS circuit,
        COALESCE(long.race_number, lat.race_number) AS race_number,
        COALESCE(long.vehicle_id, lat.vehicle_id) AS vehicle_id,
        COALESCE(long.timestamp, lat.timestamp) AS timestamp,
        long.vehicle_number,
        long.vbox_long_minutes,
        lat.vbox_lat_minutes,
        long.lap,
        long.outing,
        long.meta_event,
        long.meta_session,
        long.meta_time AS long_meta_time,
        lat.meta_time AS lat_meta_time
    FROM vbox_long_deduped long
    FULL OUTER JOIN vbox_lat_deduped lat
        ON long.circuit = lat.circuit
        AND long.race_number = lat.race_number
        AND long.vehicle_id = lat.vehicle_id
        AND long.timestamp = lat.timestamp
),

-- Forward fill coordinates for all timestamps
position_filled AS (
    SELECT
        circuit,
        race_number,
        vehicle_id,
        vehicle_number,
        timestamp,
        -- Forward fill coordinates
        LAST_VALUE(vbox_long_minutes IGNORE NULLS) OVER (
            PARTITION BY circuit, race_number, vehicle_id
            ORDER BY timestamp
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS vbox_long_minutes,
        LAST_VALUE(vbox_lat_minutes IGNORE NULLS) OVER (
            PARTITION BY circuit, race_number, vehicle_id
            ORDER BY timestamp
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS vbox_lat_minutes,
        -- Forward fill context fields
        LAST_VALUE(lap IGNORE NULLS) OVER (
            PARTITION BY circuit, race_number, vehicle_id
            ORDER BY timestamp
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS lap,
        LAST_VALUE(outing IGNORE NULLS) OVER (
            PARTITION BY circuit, race_number, vehicle_id
            ORDER BY timestamp
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS outing,
        LAST_VALUE(meta_event IGNORE NULLS) OVER (
            PARTITION BY circuit, race_number, vehicle_id
            ORDER BY timestamp
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS meta_event,
        LAST_VALUE(meta_session IGNORE NULLS) OVER (
            PARTITION BY circuit, race_number, vehicle_id
            ORDER BY timestamp
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS meta_session,
        long_meta_time,
        lat_meta_time
    FROM position_combined
),

-- Compare current position with previous to detect changes
position_with_lag AS (
    SELECT
        *,
        LAG(vbox_long_minutes) OVER (PARTITION BY circuit, race_number, vehicle_id ORDER BY timestamp) AS prev_vbox_long_minutes,
        LAG(vbox_lat_minutes) OVER (PARTITION BY circuit, race_number, vehicle_id ORDER BY timestamp) AS prev_vbox_lat_minutes
    FROM position_filled
),

-- Mark position changes and create groups
position_groups AS (
    SELECT
        circuit,
        race_number,
        vehicle_id,
        vehicle_number,
        timestamp,
        vbox_long_minutes,
        vbox_lat_minutes,
        lap,
        outing,
        meta_event,
        meta_session,
        long_meta_time,
        lat_meta_time,
        SUM(CASE 
            WHEN prev_vbox_long_minutes IS DISTINCT FROM vbox_long_minutes
                OR prev_vbox_lat_minutes IS DISTINCT FROM vbox_lat_minutes
            THEN 1 
            ELSE 0 
        END) OVER (
            PARTITION BY circuit, race_number, vehicle_id 
            ORDER BY timestamp
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS position_change_group
    FROM position_with_lag
)

SELECT
    circuit,
    race_number,
    vehicle_id,
    MAX(vehicle_number) AS vehicle_number,
    MIN(timestamp) AS position_start_timestamp,
    LEAD(MIN(timestamp)) OVER (
        PARTITION BY circuit, race_number, vehicle_id
        ORDER BY MIN(timestamp)
    ) AS position_end_timestamp,
    MAX(vbox_long_minutes) AS vbox_long_minutes,
    MAX(vbox_lat_minutes) AS vbox_lat_minutes,
    MAX(lap) AS lap,
    MAX(outing) AS outing,
    MAX(meta_event) AS meta_event,
    MAX(meta_session) AS meta_session,
    MAX(long_meta_time) AS long_meta_time,
    MAX(lat_meta_time) AS lat_meta_time
FROM position_groups
GROUP BY circuit, race_number, vehicle_id, position_change_group

