{{
    config(
        materialized='table'
    )
}}

-- Fact table capturing each change in aps (Accelerator Pedal Sensor) value with validity periods
-- Each row represents a unique change in the APS metric
-- The grain is one record per value change per vehicle
-- aps_start_timestamp and aps_end_timestamp define when this value was active
-- This allows multiple position records to reference the same APS value during its validity period

WITH 
-- Source telemetry data for Accelerator Pedal Sensor
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
        aps,
        original_vehicle_id,
        expire_at
    FROM {{ ref('int_telemetry_data_aps') }}
),

-- Identify value changes
value_changes AS (
    SELECT
        *,
        LAG(aps) OVER (
            PARTITION BY circuit, race_number, vehicle_id 
            ORDER BY timestamp
        ) AS previous_aps
    FROM source_data
),

-- Create groups for consecutive aps values
aps_groups AS (
    SELECT
        *,
        -- Create a group identifier: increment when aps changes
        SUM(CASE 
            WHEN previous_aps IS NULL OR aps != previous_aps THEN 1 
            ELSE 0 
        END) OVER (
            PARTITION BY circuit, race_number, vehicle_id 
            ORDER BY timestamp
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS aps_group_id
    FROM value_changes
),

-- Aggregate each group to get start timestamp
aps_periods AS (
    SELECT
        -- Grain columns
        circuit,
        race_number,
        vehicle_id,
        MIN(vehicle_number) AS vehicle_number,
        
        -- Start timestamp
        MIN(timestamp) AS aps_start_timestamp,
        
        -- Context columns (taking first values from the group)
        MIN(lap) AS lap,
        MIN(outing) AS outing,
        MIN(meta_event) AS meta_event,
        MIN(meta_session) AS meta_session,
        MIN(meta_source) AS meta_source,
        MIN(meta_time) AS meta_time,
        
        -- Metric columns
        aps,
        
        -- Additional columns
        MIN(original_vehicle_id) AS original_vehicle_id,
        MIN(expire_at) AS expire_at,
        
        -- Keep group id for ordering
        aps_group_id
        
    FROM aps_groups
    GROUP BY 
        circuit,
        race_number,
        vehicle_id,
        aps_group_id,
        aps
)

-- Calculate end timestamp and previous aps from the previous group
SELECT
    -- Grain columns
    circuit,
    race_number,
    vehicle_id,
    vehicle_number,
    
    -- Validity period
    aps_start_timestamp,
    LEAD(aps_start_timestamp) OVER (
        PARTITION BY circuit, race_number, vehicle_id 
        ORDER BY aps_start_timestamp
    ) AS aps_end_timestamp,
    
    -- Context columns
    lap,
    outing,
    meta_event,
    meta_session,
    meta_source,
    meta_time,
    
    -- Metric columns
    aps,
    LAG(aps) OVER (
        PARTITION BY circuit, race_number, vehicle_id 
        ORDER BY aps_start_timestamp
    ) AS previous_aps,
    aps - COALESCE(LAG(aps) OVER (
        PARTITION BY circuit, race_number, vehicle_id 
        ORDER BY aps_start_timestamp
    ), 0) AS aps_change,
    
    -- Additional columns
    original_vehicle_id,
    expire_at
    
FROM aps_periods


