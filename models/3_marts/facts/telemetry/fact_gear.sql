{{
    config(
        materialized='table'
    )
}}

-- Fact table capturing each change in gear position with validity periods
-- Each row represents a unique change in the gear metric
-- The grain is one record per value change per vehicle
-- gear_start_timestamp and gear_end_timestamp define when this value was active
-- This allows multiple position records to reference the same gear value during its validity period

WITH 
-- Source telemetry data for gear position
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
        gear,
        original_vehicle_id,
        expire_at
    FROM {{ ref('int_telemetry_data_gear') }}
),

-- Identify value changes
value_changes AS (
    SELECT
        *,
        LAG(gear) OVER (
            PARTITION BY circuit, race_number, vehicle_id 
            ORDER BY timestamp
        ) AS previous_gear
    FROM source_data
),

-- Create groups for consecutive gear values
gear_groups AS (
    SELECT
        *,
        -- Create a group identifier: increment when gear changes
        SUM(CASE 
            WHEN previous_gear IS NULL OR gear != previous_gear THEN 1 
            ELSE 0 
        END) OVER (
            PARTITION BY circuit, race_number, vehicle_id 
            ORDER BY timestamp
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS gear_group_id
    FROM value_changes
),

-- Aggregate each group to get start timestamp
gear_periods AS (
    SELECT
        -- Grain columns
        circuit,
        race_number,
        vehicle_id,
        MIN(vehicle_number) AS vehicle_number,
        
        -- Start timestamp
        MIN(timestamp) AS gear_start_timestamp,
        
        -- Context columns (taking first values from the group)
        MIN(lap) AS lap,
        MIN(outing) AS outing,
        MIN(meta_event) AS meta_event,
        MIN(meta_session) AS meta_session,
        MIN(meta_source) AS meta_source,
        MIN(meta_time) AS meta_time,
        
        -- Metric columns
        gear,
        
        -- Additional columns
        MIN(original_vehicle_id) AS original_vehicle_id,
        MIN(expire_at) AS expire_at,
        
        -- Keep group id for ordering
        gear_group_id
        
    FROM gear_groups
    GROUP BY 
        circuit,
        race_number,
        vehicle_id,
        gear_group_id,
        gear
)

-- Calculate end timestamp and previous gear from the previous group
SELECT
    -- Grain columns
    circuit,
    race_number,
    vehicle_id,
    vehicle_number,
    
    -- Validity period
    gear_start_timestamp,
    LEAD(gear_start_timestamp) OVER (
        PARTITION BY circuit, race_number, vehicle_id 
        ORDER BY gear_start_timestamp
    ) AS gear_end_timestamp,
    
    -- Context columns
    lap,
    outing,
    meta_event,
    meta_session,
    meta_source,
    meta_time,
    
    -- Metric columns
    gear,
    LAG(gear) OVER (
        PARTITION BY circuit, race_number, vehicle_id 
        ORDER BY gear_start_timestamp
    ) AS previous_gear,
    gear - COALESCE(LAG(gear) OVER (
        PARTITION BY circuit, race_number, vehicle_id 
        ORDER BY gear_start_timestamp
    ), 0) AS gear_change,
    
    -- Additional columns
    original_vehicle_id,
    expire_at
    
FROM gear_periods



