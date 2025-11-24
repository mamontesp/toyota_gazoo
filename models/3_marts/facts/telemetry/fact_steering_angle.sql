{{
    config(
        materialized='table'
    )
}}

-- Fact table capturing each change in steering angle with validity periods
-- Each row represents a unique change in the steering_angle metric
-- The grain is one record per value change per vehicle
-- steering_angle_start_timestamp and steering_angle_end_timestamp define when this value was active
-- This allows multiple position records to reference the same steering_angle value during its validity period

WITH 
-- Source telemetry data for steering angle
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
        steering_angle,
        original_vehicle_id,
        expire_at
    FROM {{ ref('int_telemetry_data_steering_angle') }}
),

-- Identify value changes
value_changes AS (
    SELECT
        *,
        LAG(steering_angle) OVER (
            PARTITION BY circuit, race_number, vehicle_id 
            ORDER BY timestamp
        ) AS previous_steering_angle
    FROM source_data
),

-- Create groups for consecutive steering_angle values
steering_angle_groups AS (
    SELECT
        *,
        -- Create a group identifier: increment when steering_angle changes
        SUM(CASE 
            WHEN previous_steering_angle IS NULL OR steering_angle != previous_steering_angle THEN 1 
            ELSE 0 
        END) OVER (
            PARTITION BY circuit, race_number, vehicle_id 
            ORDER BY timestamp
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS steering_angle_group_id
    FROM value_changes
),

-- Aggregate each group to get start timestamp
steering_angle_periods AS (
    SELECT
        -- Grain columns
        circuit,
        race_number,
        vehicle_id,
        MIN(vehicle_number) AS vehicle_number,
        
        -- Start timestamp
        MIN(timestamp) AS steering_angle_start_timestamp,
        
        -- Context columns (taking first values from the group)
        MIN(lap) AS lap,
        MIN(outing) AS outing,
        MIN(meta_event) AS meta_event,
        MIN(meta_session) AS meta_session,
        MIN(meta_source) AS meta_source,
        MIN(meta_time) AS meta_time,
        
        -- Metric columns
        steering_angle,
        
        -- Additional columns
        MIN(original_vehicle_id) AS original_vehicle_id,
        MIN(expire_at) AS expire_at,
        
        -- Keep group id for ordering
        steering_angle_group_id
        
    FROM steering_angle_groups
    GROUP BY 
        circuit,
        race_number,
        vehicle_id,
        steering_angle_group_id,
        steering_angle
)

-- Calculate end timestamp and previous steering_angle from the previous group
SELECT
    -- Grain columns
    circuit,
    race_number,
    vehicle_id,
    vehicle_number,
    
    -- Validity period
    steering_angle_start_timestamp,
    LEAD(steering_angle_start_timestamp) OVER (
        PARTITION BY circuit, race_number, vehicle_id 
        ORDER BY steering_angle_start_timestamp
    ) AS steering_angle_end_timestamp,
    
    -- Context columns
    lap,
    outing,
    meta_event,
    meta_session,
    meta_source,
    meta_time,
    
    -- Metric columns
    steering_angle,
    LAG(steering_angle) OVER (
        PARTITION BY circuit, race_number, vehicle_id 
        ORDER BY steering_angle_start_timestamp
    ) AS previous_steering_angle,
    steering_angle - COALESCE(LAG(steering_angle) OVER (
        PARTITION BY circuit, race_number, vehicle_id 
        ORDER BY steering_angle_start_timestamp
    ), 0) AS steering_angle_change,
    
    -- Additional columns
    original_vehicle_id,
    expire_at
    
FROM steering_angle_periods




