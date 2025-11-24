{{
    config(
        materialized='table'
    )
}}

-- Fact table capturing each change in front brake pressure with validity periods
-- Each row represents a unique change in the front brake pressure metric
-- The grain is one record per value change per vehicle
-- front_brake_pressure_start_timestamp and front_brake_pressure_end_timestamp define when this value was active
-- This allows multiple position records to reference the same front brake pressure value during its validity period

WITH 
-- Source telemetry data for front brake pressure
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
        pbrake_f AS front_brake_pressure,
        original_vehicle_id,
        expire_at
    FROM {{ ref('int_telemetry_data_pbrake_f') }}
),

-- Identify value changes
value_changes AS (
    SELECT
        *,
        LAG(front_brake_pressure) OVER (
            PARTITION BY circuit, race_number, vehicle_id 
            ORDER BY timestamp
        ) AS previous_front_brake_pressure
    FROM source_data
),

-- Create groups for consecutive front brake pressure values
front_brake_pressure_groups AS (
    SELECT
        *,
        -- Create a group identifier: increment when front brake pressure changes
        SUM(CASE 
            WHEN previous_front_brake_pressure IS NULL OR front_brake_pressure != previous_front_brake_pressure THEN 1 
            ELSE 0 
        END) OVER (
            PARTITION BY circuit, race_number, vehicle_id 
            ORDER BY timestamp
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS front_brake_pressure_group_id
    FROM value_changes
),

-- Aggregate each group to get start timestamp
front_brake_pressure_periods AS (
    SELECT
        -- Grain columns
        circuit,
        race_number,
        vehicle_id,
        MIN(vehicle_number) AS vehicle_number,
        
        -- Start timestamp
        MIN(timestamp) AS front_brake_pressure_start_timestamp,
        
        -- Context columns (taking first values from the group)
        MIN(lap) AS lap,
        MIN(outing) AS outing,
        MIN(meta_event) AS meta_event,
        MIN(meta_session) AS meta_session,
        MIN(meta_source) AS meta_source,
        MIN(meta_time) AS meta_time,
        
        -- Metric columns
        front_brake_pressure,
        
        -- Additional columns
        MIN(original_vehicle_id) AS original_vehicle_id,
        MIN(expire_at) AS expire_at,
        
        -- Keep group id for ordering
        front_brake_pressure_group_id
        
    FROM front_brake_pressure_groups
    GROUP BY 
        circuit,
        race_number,
        vehicle_id,
        front_brake_pressure_group_id,
        front_brake_pressure
)

-- Calculate end timestamp and previous front brake pressure from the previous group
SELECT
    -- Grain columns
    circuit,
    race_number,
    vehicle_id,
    vehicle_number,
    
    -- Validity period
    front_brake_pressure_start_timestamp,
    LEAD(front_brake_pressure_start_timestamp) OVER (
        PARTITION BY circuit, race_number, vehicle_id 
        ORDER BY front_brake_pressure_start_timestamp
    ) AS front_brake_pressure_end_timestamp,
    
    -- Context columns
    lap,
    outing,
    meta_event,
    meta_session,
    meta_source,
    meta_time,
    
    -- Metric columns
    front_brake_pressure,
    LAG(front_brake_pressure) OVER (
        PARTITION BY circuit, race_number, vehicle_id 
        ORDER BY front_brake_pressure_start_timestamp
    ) AS previous_front_brake_pressure,
    front_brake_pressure - COALESCE(LAG(front_brake_pressure) OVER (
        PARTITION BY circuit, race_number, vehicle_id 
        ORDER BY front_brake_pressure_start_timestamp
    ), 0) AS front_brake_pressure_change,
    
    -- Additional columns
    original_vehicle_id,
    expire_at
    
FROM front_brake_pressure_periods

