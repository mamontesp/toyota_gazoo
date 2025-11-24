{{
    config(
        materialized='table'
    )
}}

-- Fact table capturing each change in engine RPM (nmot) with validity periods
-- Each row represents a unique change in the nmot metric
-- The grain is one record per value change per vehicle
-- nmot_start_timestamp and nmot_end_timestamp define when this value was active
-- This allows multiple position records to reference the same nmot value during its validity period

WITH 
-- Source telemetry data for engine RPM
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
        nmot,
        original_vehicle_id,
        expire_at
    FROM {{ ref('int_telemetry_data_nmot') }}
),

-- Identify value changes
value_changes AS (
    SELECT
        *,
        LAG(nmot) OVER (
            PARTITION BY circuit, race_number, vehicle_id 
            ORDER BY timestamp
        ) AS previous_nmot
    FROM source_data
),

-- Create groups for consecutive nmot values
nmot_groups AS (
    SELECT
        *,
        -- Create a group identifier: increment when nmot changes
        SUM(CASE 
            WHEN previous_nmot IS NULL OR nmot != previous_nmot THEN 1 
            ELSE 0 
        END) OVER (
            PARTITION BY circuit, race_number, vehicle_id 
            ORDER BY timestamp
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS nmot_group_id
    FROM value_changes
),

-- Aggregate each group to get start timestamp
nmot_periods AS (
    SELECT
        -- Grain columns
        circuit,
        race_number,
        vehicle_id,
        MIN(vehicle_number) AS vehicle_number,
        
        -- Start timestamp
        MIN(timestamp) AS nmot_start_timestamp,
        
        -- Context columns (taking first values from the group)
        MIN(lap) AS lap,
        MIN(outing) AS outing,
        MIN(meta_event) AS meta_event,
        MIN(meta_session) AS meta_session,
        MIN(meta_source) AS meta_source,
        MIN(meta_time) AS meta_time,
        
        -- Metric columns
        nmot,
        
        -- Additional columns
        MIN(original_vehicle_id) AS original_vehicle_id,
        MIN(expire_at) AS expire_at,
        
        -- Keep group id for ordering
        nmot_group_id
        
    FROM nmot_groups
    GROUP BY 
        circuit,
        race_number,
        vehicle_id,
        nmot_group_id,
        nmot
)

-- Calculate end timestamp and previous nmot from the previous group
SELECT
    -- Grain columns
    circuit,
    race_number,
    vehicle_id,
    vehicle_number,
    
    -- Validity period
    nmot_start_timestamp,
    LEAD(nmot_start_timestamp) OVER (
        PARTITION BY circuit, race_number, vehicle_id 
        ORDER BY nmot_start_timestamp
    ) AS nmot_end_timestamp,
    
    -- Context columns
    lap,
    outing,
    meta_event,
    meta_session,
    meta_source,
    meta_time,
    
    -- Metric columns
    nmot,
    LAG(nmot) OVER (
        PARTITION BY circuit, race_number, vehicle_id 
        ORDER BY nmot_start_timestamp
    ) AS previous_nmot,
    nmot - COALESCE(LAG(nmot) OVER (
        PARTITION BY circuit, race_number, vehicle_id 
        ORDER BY nmot_start_timestamp
    ), 0) AS nmot_change,
    
    -- Additional columns
    original_vehicle_id,
    expire_at
    
FROM nmot_periods




