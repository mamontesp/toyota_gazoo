{{
    config(
        materialized='table'
    )
}}

-- Fact table capturing each change in rear brake pressure with validity periods
-- Each row represents a unique change in the rear brake pressure metric
-- The grain is one record per value change per vehicle
-- rear_brake_pressure_start_timestamp and rear_brake_pressure_end_timestamp define when this value was active
-- This allows multiple position records to reference the same rear brake pressure value during its validity period

WITH 
-- Source telemetry data for rear brake pressure
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
        pbrake_r AS rear_brake_pressure,
        original_vehicle_id,
        expire_at
    FROM {{ ref('int_telemetry_data_pbrake_r') }}
),

-- Identify value changes
value_changes AS (
    SELECT
        *,
        LAG(rear_brake_pressure) OVER (
            PARTITION BY circuit, race_number, vehicle_id 
            ORDER BY timestamp
        ) AS previous_rear_brake_pressure
    FROM source_data
),

-- Create groups for consecutive rear brake pressure values
rear_brake_pressure_groups AS (
    SELECT
        *,
        -- Create a group identifier: increment when rear brake pressure changes
        SUM(CASE 
            WHEN previous_rear_brake_pressure IS NULL OR rear_brake_pressure != previous_rear_brake_pressure THEN 1 
            ELSE 0 
        END) OVER (
            PARTITION BY circuit, race_number, vehicle_id 
            ORDER BY timestamp
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS rear_brake_pressure_group_id
    FROM value_changes
),

-- Aggregate each group to get start timestamp
rear_brake_pressure_periods AS (
    SELECT
        -- Grain columns
        circuit,
        race_number,
        vehicle_id,
        MIN(vehicle_number) AS vehicle_number,
        
        -- Start timestamp
        MIN(timestamp) AS rear_brake_pressure_start_timestamp,
        
        -- Context columns (taking first values from the group)
        MIN(lap) AS lap,
        MIN(outing) AS outing,
        MIN(meta_event) AS meta_event,
        MIN(meta_session) AS meta_session,
        MIN(meta_source) AS meta_source,
        MIN(meta_time) AS meta_time,
        
        -- Metric columns
        rear_brake_pressure,
        
        -- Additional columns
        MIN(original_vehicle_id) AS original_vehicle_id,
        MIN(expire_at) AS expire_at,
        
        -- Keep group id for ordering
        rear_brake_pressure_group_id
        
    FROM rear_brake_pressure_groups
    GROUP BY 
        circuit,
        race_number,
        vehicle_id,
        rear_brake_pressure_group_id,
        rear_brake_pressure
)

-- Calculate end timestamp and previous rear brake pressure from the previous group
SELECT
    -- Grain columns
    circuit,
    race_number,
    vehicle_id,
    vehicle_number,
    
    -- Validity period
    rear_brake_pressure_start_timestamp,
    LEAD(rear_brake_pressure_start_timestamp) OVER (
        PARTITION BY circuit, race_number, vehicle_id 
        ORDER BY rear_brake_pressure_start_timestamp
    ) AS rear_brake_pressure_end_timestamp,
    
    -- Context columns
    lap,
    outing,
    meta_event,
    meta_session,
    meta_source,
    meta_time,
    
    -- Metric columns
    rear_brake_pressure,
    LAG(rear_brake_pressure) OVER (
        PARTITION BY circuit, race_number, vehicle_id 
        ORDER BY rear_brake_pressure_start_timestamp
    ) AS previous_rear_brake_pressure,
    rear_brake_pressure - COALESCE(LAG(rear_brake_pressure) OVER (
        PARTITION BY circuit, race_number, vehicle_id 
        ORDER BY rear_brake_pressure_start_timestamp
    ), 0) AS rear_brake_pressure_change,
    
    -- Additional columns
    original_vehicle_id,
    expire_at
    
FROM rear_brake_pressure_periods

