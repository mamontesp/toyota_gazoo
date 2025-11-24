{{
    config(
        materialized='table'
    )
}}

-- Fact table capturing each change in speed with validity periods
-- Each row represents a unique change in the speed metric
-- The grain is one record per value change per vehicle
-- speed_start_timestamp and speed_end_timestamp define when this value was active
-- This allows multiple position records to reference the same speed value during its validity period

WITH 
-- Source telemetry data for speed
source_data AS (
    SELECT
        circuit,
        race_number,
        vehicle_id,
        vehicle_number,
        lap,
        outing,
        meta_event,
        meta_session,
        meta_source,
        meta_time,
        timestamp,
        speed,
        original_vehicle_id,
        expire_at
    FROM {{ ref('int_telemetry_data_speed') }}
),

-- Identify value changes
value_changes AS (
    SELECT
        *,
        LAG(speed) OVER (
            PARTITION BY circuit, race_number, vehicle_id 
            ORDER BY timestamp
        ) AS previous_speed
    FROM source_data
),

-- Create groups for consecutive speed values
speed_groups AS (
    SELECT
        *,
        -- Create a group identifier: increment when speed changes
        SUM(CASE 
            WHEN previous_speed IS NULL OR speed != previous_speed THEN 1 
            ELSE 0 
        END) OVER (
            PARTITION BY circuit, race_number, vehicle_id 
            ORDER BY timestamp
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS speed_group_id
    FROM value_changes
),

-- Aggregate each group to get start timestamp
speed_periods AS (
    SELECT
        -- Grain columns
        circuit,
        race_number,
        vehicle_id,
        MIN(vehicle_number) AS vehicle_number,
        
        -- Start timestamp
        MIN(timestamp) AS speed_start_timestamp,
        
        -- Context columns (taking first values from the group)
        MIN(lap) AS lap,
        MIN(outing) AS outing,
        MIN(meta_event) AS meta_event,
        MIN(meta_session) AS meta_session,
        MIN(meta_source) AS meta_source,
        MIN(meta_time) AS meta_time,
        
        -- Metric columns
        speed,
        
        -- Additional columns
        MIN(original_vehicle_id) AS original_vehicle_id,
        MIN(expire_at) AS expire_at,
        
        -- Keep group id for ordering
        speed_group_id
        
    FROM speed_groups
    GROUP BY 
        circuit,
        race_number,
        vehicle_id,
        speed_group_id,
        speed
)

-- Calculate end timestamp and previous speed from the previous group
SELECT
    -- Grain columns
    circuit,
    race_number,
    vehicle_id,
    vehicle_number,
    
    -- Validity period
    speed_start_timestamp,
    LEAD(speed_start_timestamp) OVER (
        PARTITION BY circuit, race_number, vehicle_id 
        ORDER BY speed_start_timestamp
    ) AS speed_end_timestamp,
    
    -- Context columns
    lap,
    outing,
    meta_event,
    meta_session,
    meta_source,
    meta_time,
    
    -- Metric columns
    speed,
    LAG(speed) OVER (
        PARTITION BY circuit, race_number, vehicle_id 
        ORDER BY speed_start_timestamp
    ) AS previous_speed,
    speed - COALESCE(LAG(speed) OVER (
        PARTITION BY circuit, race_number, vehicle_id 
        ORDER BY speed_start_timestamp
    ), 0) AS speed_change,
    
    -- Additional columns
    original_vehicle_id,
    expire_at
    
FROM speed_periods




