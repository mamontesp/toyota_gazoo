{{
    config(
        materialized='table'
    )
}}

-- Fact table capturing each change in laptrigger_lapdist_dls value with validity periods
-- Each row represents a unique change in the laptrigger_lapdist_dls metric
-- The grain is one record per value change per vehicle
-- record_start_time and record_end_time define when this value was active
-- This allows multiple position records to reference the same laptrigger value during its validity period

WITH 
-- Source telemetry data for lap trigger distance
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
        laptrigger_lapdist_dls,
        original_vehicle_id,
        expire_at
    FROM {{ ref('int_telemetry_data_laptrigger_lapdist_dls') }}
),

-- Identify value changes
value_changes AS (
    SELECT
        *,
        LAG(laptrigger_lapdist_dls) OVER (
            PARTITION BY circuit, race_number, vehicle_id 
            ORDER BY timestamp
        ) AS previous_laptrigger_lapdist_dls
    FROM source_data
),

-- Create groups for consecutive laptrigger_lapdist_dls values
laptrigger_groups AS (
    SELECT
        *,
        -- Create a group identifier: increment when laptrigger_lapdist_dls changes
        SUM(CASE 
            WHEN previous_laptrigger_lapdist_dls IS NULL OR laptrigger_lapdist_dls != previous_laptrigger_lapdist_dls THEN 1 
            ELSE 0 
        END) OVER (
            PARTITION BY circuit, race_number, vehicle_id 
            ORDER BY timestamp
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS laptrigger_group_id
    FROM value_changes
),

-- Aggregate each group to get start timestamp
laptrigger_periods AS (
    SELECT
        -- Grain columns
        circuit,
        race_number,
        vehicle_id,
        MIN(vehicle_number) AS vehicle_number,
        
        -- Start timestamp
        MIN(timestamp) AS laptrigger_lapdist_dls_start_timestamp,
        
        -- Context columns (taking first values from the group)
        MIN(lap) AS lap,
        MIN(outing) AS outing,
        MIN(meta_event) AS meta_event,
        MIN(meta_session) AS meta_session,
        MIN(meta_source) AS meta_source,
        MIN(meta_time) AS meta_time,
        
        -- Metric columns
        laptrigger_lapdist_dls,
        
        -- Additional columns
        MIN(original_vehicle_id) AS original_vehicle_id,
        MIN(expire_at) AS expire_at,
        
        -- Keep group id for ordering
        laptrigger_group_id
        
    FROM laptrigger_groups
    GROUP BY 
        circuit,
        race_number,
        vehicle_id,
        laptrigger_group_id,
        laptrigger_lapdist_dls
)

-- Calculate end timestamp and previous laptrigger_lapdist_dls from the previous group
SELECT
    -- Grain columns
    circuit,
    race_number,
    vehicle_id,
    vehicle_number,
    
    -- Validity period
    laptrigger_lapdist_dls_start_timestamp,
    LEAD(laptrigger_lapdist_dls_start_timestamp) OVER (
        PARTITION BY circuit, race_number, vehicle_id 
        ORDER BY laptrigger_lapdist_dls_start_timestamp
    ) AS laptrigger_lapdist_dls_end_timestamp,
    
    -- Context columns
    lap,
    outing,
    meta_event,
    meta_session,
    meta_source,
    meta_time,
    
    -- Metric columns
    laptrigger_lapdist_dls,
    LAG(laptrigger_lapdist_dls) OVER (
        PARTITION BY circuit, race_number, vehicle_id 
        ORDER BY laptrigger_lapdist_dls_start_timestamp
    ) AS previous_laptrigger_lapdist_dls,
    laptrigger_lapdist_dls - COALESCE(LAG(laptrigger_lapdist_dls) OVER (
        PARTITION BY circuit, race_number, vehicle_id 
        ORDER BY laptrigger_lapdist_dls_start_timestamp
    ), 0) AS laptrigger_lapdist_dls_change,
    
    -- Additional columns
    original_vehicle_id,
    expire_at
    
FROM laptrigger_periods

