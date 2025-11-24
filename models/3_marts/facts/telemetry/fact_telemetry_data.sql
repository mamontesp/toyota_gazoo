{{
    config(
        materialized='table'
    )
}}

-- Fact table with telemetry data capturing each change in any metric
-- Each row represents a change in position, laptrigger_lapdist_dls, aps, gear, nmot, acc, speed, steering_angle, front_brake_pressure, or rear_brake_pressure
-- The grain is one record per change in any of these metrics per vehicle
-- This allows tracking all telemetry changes regardless of which metric changed
-- Optimized to avoid expensive range joins by using window functions

WITH 
-- Position changes with metric values
position_events AS (
    SELECT
        circuit,
        race_number,
        vehicle_id,
        vehicle_number,
        position_start_timestamp AS event_timestamp,
        'position' AS event_source,
        lap,
        outing,
        meta_event,
        meta_session,
        -- Position values
        vbox_long_minutes,
        vbox_lat_minutes,
        long_meta_time,
        lat_meta_time,
        position_start_timestamp,
        position_end_timestamp,
        -- Null for other metrics
        CAST(NULL AS DOUBLE) AS laptrigger_lapdist_dls,
        CAST(NULL AS TIMESTAMP) AS laptrigger_lapdist_dls_start_timestamp,
        CAST(NULL AS TIMESTAMP) AS laptrigger_lapdist_dls_end_timestamp,
        CAST(NULL AS DOUBLE) AS aps,
        CAST(NULL AS TIMESTAMP) AS aps_start_timestamp,
        CAST(NULL AS TIMESTAMP) AS aps_end_timestamp,
        CAST(NULL AS DOUBLE) AS gear,
        CAST(NULL AS TIMESTAMP) AS gear_start_timestamp,
        CAST(NULL AS TIMESTAMP) AS gear_end_timestamp,
        CAST(NULL AS DOUBLE) AS nmot,
        CAST(NULL AS TIMESTAMP) AS nmot_start_timestamp,
        CAST(NULL AS TIMESTAMP) AS nmot_end_timestamp,
        CAST(NULL AS DOUBLE) AS accx_can,
        CAST(NULL AS DOUBLE) AS accy_can,
        CAST(NULL AS TIMESTAMP) AS acc_start_timestamp,
        CAST(NULL AS TIMESTAMP) AS acc_end_timestamp,
        CAST(NULL AS DOUBLE) AS speed,
        CAST(NULL AS TIMESTAMP) AS speed_start_timestamp,
        CAST(NULL AS TIMESTAMP) AS speed_end_timestamp,
        CAST(NULL AS DOUBLE) AS steering_angle,
        CAST(NULL AS TIMESTAMP) AS steering_angle_start_timestamp,
        CAST(NULL AS TIMESTAMP) AS steering_angle_end_timestamp,
        CAST(NULL AS DOUBLE) AS front_brake_pressure,
        CAST(NULL AS TIMESTAMP) AS front_brake_pressure_start_timestamp,
        CAST(NULL AS TIMESTAMP) AS front_brake_pressure_end_timestamp,
        CAST(NULL AS DOUBLE) AS rear_brake_pressure,
        CAST(NULL AS TIMESTAMP) AS rear_brake_pressure_start_timestamp,
        CAST(NULL AS TIMESTAMP) AS rear_brake_pressure_end_timestamp
    FROM {{ ref('fact_position') }}
),

-- Laptrigger changes with metric values
laptrigger_events AS (
    SELECT
        circuit,
        race_number,
        vehicle_id,
        vehicle_number,
        laptrigger_lapdist_dls_start_timestamp AS event_timestamp,
        'laptrigger' AS event_source,
        lap,
        outing,
        meta_event,
        meta_session,
        -- Null for position
        CAST(NULL AS DOUBLE) AS vbox_long_minutes,
        CAST(NULL AS DOUBLE) AS vbox_lat_minutes,
        CAST(NULL AS TIMESTAMP) AS long_meta_time,
        CAST(NULL AS TIMESTAMP) AS lat_meta_time,
        CAST(NULL AS TIMESTAMP) AS position_start_timestamp,
        CAST(NULL AS TIMESTAMP) AS position_end_timestamp,
        -- Laptrigger values
        laptrigger_lapdist_dls,
        laptrigger_lapdist_dls_start_timestamp,
        laptrigger_lapdist_dls_end_timestamp,
        -- Null for APS
        CAST(NULL AS DOUBLE) AS aps,
        CAST(NULL AS TIMESTAMP) AS aps_start_timestamp,
        CAST(NULL AS TIMESTAMP) AS aps_end_timestamp,
        -- Null for gear
        CAST(NULL AS DOUBLE) AS gear,
        CAST(NULL AS TIMESTAMP) AS gear_start_timestamp,
        CAST(NULL AS TIMESTAMP) AS gear_end_timestamp,
        -- Null for nmot
        CAST(NULL AS DOUBLE) AS nmot,
        CAST(NULL AS TIMESTAMP) AS nmot_start_timestamp,
        CAST(NULL AS TIMESTAMP) AS nmot_end_timestamp,
        -- Null for acc
        CAST(NULL AS DOUBLE) AS accx_can,
        CAST(NULL AS DOUBLE) AS accy_can,
        CAST(NULL AS TIMESTAMP) AS acc_start_timestamp,
        CAST(NULL AS TIMESTAMP) AS acc_end_timestamp,
        -- Null for speed
        CAST(NULL AS DOUBLE) AS speed,
        CAST(NULL AS TIMESTAMP) AS speed_start_timestamp,
        CAST(NULL AS TIMESTAMP) AS speed_end_timestamp,
        -- Null for steering_angle
        CAST(NULL AS DOUBLE) AS steering_angle,
        CAST(NULL AS TIMESTAMP) AS steering_angle_start_timestamp,
        CAST(NULL AS TIMESTAMP) AS steering_angle_end_timestamp,
        -- Null for brake pressure
        CAST(NULL AS DOUBLE) AS front_brake_pressure,
        CAST(NULL AS TIMESTAMP) AS front_brake_pressure_start_timestamp,
        CAST(NULL AS TIMESTAMP) AS front_brake_pressure_end_timestamp,
        CAST(NULL AS DOUBLE) AS rear_brake_pressure,
        CAST(NULL AS TIMESTAMP) AS rear_brake_pressure_start_timestamp,
        CAST(NULL AS TIMESTAMP) AS rear_brake_pressure_end_timestamp
    FROM {{ ref('fact_laptrigger_lapdist_dls') }}
),

-- APS changes with metric values
aps_events AS (
    SELECT
        circuit,
        race_number,
        vehicle_id,
        vehicle_number,
        aps_start_timestamp AS event_timestamp,
        'aps' AS event_source,
        lap,
        outing,
        meta_event,
        meta_session,
        -- Null for position
        CAST(NULL AS DOUBLE) AS vbox_long_minutes,
        CAST(NULL AS DOUBLE) AS vbox_lat_minutes,
        CAST(NULL AS TIMESTAMP) AS long_meta_time,
        CAST(NULL AS TIMESTAMP) AS lat_meta_time,
        CAST(NULL AS TIMESTAMP) AS position_start_timestamp,
        CAST(NULL AS TIMESTAMP) AS position_end_timestamp,
        -- Null for laptrigger
        CAST(NULL AS DOUBLE) AS laptrigger_lapdist_dls,
        CAST(NULL AS TIMESTAMP) AS laptrigger_lapdist_dls_start_timestamp,
        CAST(NULL AS TIMESTAMP) AS laptrigger_lapdist_dls_end_timestamp,
        -- APS values
        aps,
        aps_start_timestamp,
        aps_end_timestamp,
        -- Null for gear
        CAST(NULL AS DOUBLE) AS gear,
        CAST(NULL AS TIMESTAMP) AS gear_start_timestamp,
        CAST(NULL AS TIMESTAMP) AS gear_end_timestamp,
        -- Null for nmot
        CAST(NULL AS DOUBLE) AS nmot,
        CAST(NULL AS TIMESTAMP) AS nmot_start_timestamp,
        CAST(NULL AS TIMESTAMP) AS nmot_end_timestamp,
        -- Null for acc
        CAST(NULL AS DOUBLE) AS accx_can,
        CAST(NULL AS DOUBLE) AS accy_can,
        CAST(NULL AS TIMESTAMP) AS acc_start_timestamp,
        CAST(NULL AS TIMESTAMP) AS acc_end_timestamp,
        -- Null for speed
        CAST(NULL AS DOUBLE) AS speed,
        CAST(NULL AS TIMESTAMP) AS speed_start_timestamp,
        CAST(NULL AS TIMESTAMP) AS speed_end_timestamp,
        -- Null for steering_angle
        CAST(NULL AS DOUBLE) AS steering_angle,
        CAST(NULL AS TIMESTAMP) AS steering_angle_start_timestamp,
        CAST(NULL AS TIMESTAMP) AS steering_angle_end_timestamp,
        -- Null for brake pressure
        CAST(NULL AS DOUBLE) AS front_brake_pressure,
        CAST(NULL AS TIMESTAMP) AS front_brake_pressure_start_timestamp,
        CAST(NULL AS TIMESTAMP) AS front_brake_pressure_end_timestamp,
        CAST(NULL AS DOUBLE) AS rear_brake_pressure,
        CAST(NULL AS TIMESTAMP) AS rear_brake_pressure_start_timestamp,
        CAST(NULL AS TIMESTAMP) AS rear_brake_pressure_end_timestamp
    FROM {{ ref('fact_aps') }}
),

-- Gear changes with metric values
gear_events AS (
    SELECT
        circuit,
        race_number,
        vehicle_id,
        vehicle_number,
        gear_start_timestamp AS event_timestamp,
        'gear' AS event_source,
        lap,
        outing,
        meta_event,
        meta_session,
        -- Null for position
        CAST(NULL AS DOUBLE) AS vbox_long_minutes,
        CAST(NULL AS DOUBLE) AS vbox_lat_minutes,
        CAST(NULL AS TIMESTAMP) AS long_meta_time,
        CAST(NULL AS TIMESTAMP) AS lat_meta_time,
        CAST(NULL AS TIMESTAMP) AS position_start_timestamp,
        CAST(NULL AS TIMESTAMP) AS position_end_timestamp,
        -- Null for laptrigger
        CAST(NULL AS DOUBLE) AS laptrigger_lapdist_dls,
        CAST(NULL AS TIMESTAMP) AS laptrigger_lapdist_dls_start_timestamp,
        CAST(NULL AS TIMESTAMP) AS laptrigger_lapdist_dls_end_timestamp,
        -- Null for APS
        CAST(NULL AS DOUBLE) AS aps,
        CAST(NULL AS TIMESTAMP) AS aps_start_timestamp,
        CAST(NULL AS TIMESTAMP) AS aps_end_timestamp,
        -- Gear values
        gear,
        gear_start_timestamp,
        gear_end_timestamp,
        -- Null for nmot
        CAST(NULL AS DOUBLE) AS nmot,
        CAST(NULL AS TIMESTAMP) AS nmot_start_timestamp,
        CAST(NULL AS TIMESTAMP) AS nmot_end_timestamp,
        -- Null for acc
        CAST(NULL AS DOUBLE) AS accx_can,
        CAST(NULL AS DOUBLE) AS accy_can,
        CAST(NULL AS TIMESTAMP) AS acc_start_timestamp,
        CAST(NULL AS TIMESTAMP) AS acc_end_timestamp,
        -- Null for speed
        CAST(NULL AS DOUBLE) AS speed,
        CAST(NULL AS TIMESTAMP) AS speed_start_timestamp,
        CAST(NULL AS TIMESTAMP) AS speed_end_timestamp,
        -- Null for steering_angle
        CAST(NULL AS DOUBLE) AS steering_angle,
        CAST(NULL AS TIMESTAMP) AS steering_angle_start_timestamp,
        CAST(NULL AS TIMESTAMP) AS steering_angle_end_timestamp,
        -- Null for brake pressure
        CAST(NULL AS DOUBLE) AS front_brake_pressure,
        CAST(NULL AS TIMESTAMP) AS front_brake_pressure_start_timestamp,
        CAST(NULL AS TIMESTAMP) AS front_brake_pressure_end_timestamp,
        CAST(NULL AS DOUBLE) AS rear_brake_pressure,
        CAST(NULL AS TIMESTAMP) AS rear_brake_pressure_start_timestamp,
        CAST(NULL AS TIMESTAMP) AS rear_brake_pressure_end_timestamp
    FROM {{ ref('fact_gear') }}
),

-- Nmot (engine RPM) changes with metric values
nmot_events AS (
    SELECT
        circuit,
        race_number,
        vehicle_id,
        vehicle_number,
        nmot_start_timestamp AS event_timestamp,
        'nmot' AS event_source,
        lap,
        outing,
        meta_event,
        meta_session,
        -- Null for position
        CAST(NULL AS DOUBLE) AS vbox_long_minutes,
        CAST(NULL AS DOUBLE) AS vbox_lat_minutes,
        CAST(NULL AS TIMESTAMP) AS long_meta_time,
        CAST(NULL AS TIMESTAMP) AS lat_meta_time,
        CAST(NULL AS TIMESTAMP) AS position_start_timestamp,
        CAST(NULL AS TIMESTAMP) AS position_end_timestamp,
        -- Null for laptrigger
        CAST(NULL AS DOUBLE) AS laptrigger_lapdist_dls,
        CAST(NULL AS TIMESTAMP) AS laptrigger_lapdist_dls_start_timestamp,
        CAST(NULL AS TIMESTAMP) AS laptrigger_lapdist_dls_end_timestamp,
        -- Null for APS
        CAST(NULL AS DOUBLE) AS aps,
        CAST(NULL AS TIMESTAMP) AS aps_start_timestamp,
        CAST(NULL AS TIMESTAMP) AS aps_end_timestamp,
        -- Null for gear
        CAST(NULL AS DOUBLE) AS gear,
        CAST(NULL AS TIMESTAMP) AS gear_start_timestamp,
        CAST(NULL AS TIMESTAMP) AS gear_end_timestamp,
        -- Nmot values
        nmot,
        nmot_start_timestamp,
        nmot_end_timestamp,
        -- Null for acc
        CAST(NULL AS DOUBLE) AS accx_can,
        CAST(NULL AS DOUBLE) AS accy_can,
        CAST(NULL AS TIMESTAMP) AS acc_start_timestamp,
        CAST(NULL AS TIMESTAMP) AS acc_end_timestamp,
        -- Null for speed
        CAST(NULL AS DOUBLE) AS speed,
        CAST(NULL AS TIMESTAMP) AS speed_start_timestamp,
        CAST(NULL AS TIMESTAMP) AS speed_end_timestamp,
        -- Null for steering_angle
        CAST(NULL AS DOUBLE) AS steering_angle,
        CAST(NULL AS TIMESTAMP) AS steering_angle_start_timestamp,
        CAST(NULL AS TIMESTAMP) AS steering_angle_end_timestamp,
        -- Null for brake pressure
        CAST(NULL AS DOUBLE) AS front_brake_pressure,
        CAST(NULL AS TIMESTAMP) AS front_brake_pressure_start_timestamp,
        CAST(NULL AS TIMESTAMP) AS front_brake_pressure_end_timestamp,
        CAST(NULL AS DOUBLE) AS rear_brake_pressure,
        CAST(NULL AS TIMESTAMP) AS rear_brake_pressure_start_timestamp,
        CAST(NULL AS TIMESTAMP) AS rear_brake_pressure_end_timestamp
    FROM {{ ref('fact_nmot') }}
),

-- Acceleration changes with metric values
acc_events AS (
    SELECT
        circuit,
        race_number,
        vehicle_id,
        vehicle_number,
        acc_start_timestamp AS event_timestamp,
        'acc' AS event_source,
        lap,
        outing,
        meta_event,
        meta_session,
        -- Null for position
        CAST(NULL AS DOUBLE) AS vbox_long_minutes,
        CAST(NULL AS DOUBLE) AS vbox_lat_minutes,
        CAST(NULL AS TIMESTAMP) AS long_meta_time,
        CAST(NULL AS TIMESTAMP) AS lat_meta_time,
        CAST(NULL AS TIMESTAMP) AS position_start_timestamp,
        CAST(NULL AS TIMESTAMP) AS position_end_timestamp,
        -- Null for laptrigger
        CAST(NULL AS DOUBLE) AS laptrigger_lapdist_dls,
        CAST(NULL AS TIMESTAMP) AS laptrigger_lapdist_dls_start_timestamp,
        CAST(NULL AS TIMESTAMP) AS laptrigger_lapdist_dls_end_timestamp,
        -- Null for APS
        CAST(NULL AS DOUBLE) AS aps,
        CAST(NULL AS TIMESTAMP) AS aps_start_timestamp,
        CAST(NULL AS TIMESTAMP) AS aps_end_timestamp,
        -- Null for gear
        CAST(NULL AS DOUBLE) AS gear,
        CAST(NULL AS TIMESTAMP) AS gear_start_timestamp,
        CAST(NULL AS TIMESTAMP) AS gear_end_timestamp,
        -- Null for nmot
        CAST(NULL AS DOUBLE) AS nmot,
        CAST(NULL AS TIMESTAMP) AS nmot_start_timestamp,
        CAST(NULL AS TIMESTAMP) AS nmot_end_timestamp,
        -- Acceleration values
        accx_can,
        accy_can,
        acc_start_timestamp,
        acc_end_timestamp,
        -- Null for speed
        CAST(NULL AS DOUBLE) AS speed,
        CAST(NULL AS TIMESTAMP) AS speed_start_timestamp,
        CAST(NULL AS TIMESTAMP) AS speed_end_timestamp,
        -- Null for steering_angle
        CAST(NULL AS DOUBLE) AS steering_angle,
        CAST(NULL AS TIMESTAMP) AS steering_angle_start_timestamp,
        CAST(NULL AS TIMESTAMP) AS steering_angle_end_timestamp,
        -- Null for brake pressure
        CAST(NULL AS DOUBLE) AS front_brake_pressure,
        CAST(NULL AS TIMESTAMP) AS front_brake_pressure_start_timestamp,
        CAST(NULL AS TIMESTAMP) AS front_brake_pressure_end_timestamp,
        CAST(NULL AS DOUBLE) AS rear_brake_pressure,
        CAST(NULL AS TIMESTAMP) AS rear_brake_pressure_start_timestamp,
        CAST(NULL AS TIMESTAMP) AS rear_brake_pressure_end_timestamp
    FROM {{ ref('fact_acc') }}
),

-- Speed changes with metric values
speed_events AS (
    SELECT
        circuit,
        race_number,
        vehicle_id,
        vehicle_number,
        speed_start_timestamp AS event_timestamp,
        'speed' AS event_source,
        lap,
        outing,
        meta_event,
        meta_session,
        -- Null for position
        CAST(NULL AS DOUBLE) AS vbox_long_minutes,
        CAST(NULL AS DOUBLE) AS vbox_lat_minutes,
        CAST(NULL AS TIMESTAMP) AS long_meta_time,
        CAST(NULL AS TIMESTAMP) AS lat_meta_time,
        CAST(NULL AS TIMESTAMP) AS position_start_timestamp,
        CAST(NULL AS TIMESTAMP) AS position_end_timestamp,
        -- Null for laptrigger
        CAST(NULL AS DOUBLE) AS laptrigger_lapdist_dls,
        CAST(NULL AS TIMESTAMP) AS laptrigger_lapdist_dls_start_timestamp,
        CAST(NULL AS TIMESTAMP) AS laptrigger_lapdist_dls_end_timestamp,
        -- Null for APS
        CAST(NULL AS DOUBLE) AS aps,
        CAST(NULL AS TIMESTAMP) AS aps_start_timestamp,
        CAST(NULL AS TIMESTAMP) AS aps_end_timestamp,
        -- Null for gear
        CAST(NULL AS DOUBLE) AS gear,
        CAST(NULL AS TIMESTAMP) AS gear_start_timestamp,
        CAST(NULL AS TIMESTAMP) AS gear_end_timestamp,
        -- Null for nmot
        CAST(NULL AS DOUBLE) AS nmot,
        CAST(NULL AS TIMESTAMP) AS nmot_start_timestamp,
        CAST(NULL AS TIMESTAMP) AS nmot_end_timestamp,
        -- Null for acc
        CAST(NULL AS DOUBLE) AS accx_can,
        CAST(NULL AS DOUBLE) AS accy_can,
        CAST(NULL AS TIMESTAMP) AS acc_start_timestamp,
        CAST(NULL AS TIMESTAMP) AS acc_end_timestamp,
        -- Speed values
        speed,
        speed_start_timestamp,
        speed_end_timestamp,
        -- Null for steering_angle
        CAST(NULL AS DOUBLE) AS steering_angle,
        CAST(NULL AS TIMESTAMP) AS steering_angle_start_timestamp,
        CAST(NULL AS TIMESTAMP) AS steering_angle_end_timestamp,
        -- Null for brake pressure
        CAST(NULL AS DOUBLE) AS front_brake_pressure,
        CAST(NULL AS TIMESTAMP) AS front_brake_pressure_start_timestamp,
        CAST(NULL AS TIMESTAMP) AS front_brake_pressure_end_timestamp,
        CAST(NULL AS DOUBLE) AS rear_brake_pressure,
        CAST(NULL AS TIMESTAMP) AS rear_brake_pressure_start_timestamp,
        CAST(NULL AS TIMESTAMP) AS rear_brake_pressure_end_timestamp
    FROM {{ ref('fact_speed') }}
),

-- Steering angle changes with metric values
steering_angle_events AS (
    SELECT
        circuit,
        race_number,
        vehicle_id,
        vehicle_number,
        steering_angle_start_timestamp AS event_timestamp,
        'steering_angle' AS event_source,
        lap,
        outing,
        meta_event,
        meta_session,
        -- Null for position
        CAST(NULL AS DOUBLE) AS vbox_long_minutes,
        CAST(NULL AS DOUBLE) AS vbox_lat_minutes,
        CAST(NULL AS TIMESTAMP) AS long_meta_time,
        CAST(NULL AS TIMESTAMP) AS lat_meta_time,
        CAST(NULL AS TIMESTAMP) AS position_start_timestamp,
        CAST(NULL AS TIMESTAMP) AS position_end_timestamp,
        -- Null for laptrigger
        CAST(NULL AS DOUBLE) AS laptrigger_lapdist_dls,
        CAST(NULL AS TIMESTAMP) AS laptrigger_lapdist_dls_start_timestamp,
        CAST(NULL AS TIMESTAMP) AS laptrigger_lapdist_dls_end_timestamp,
        -- Null for APS
        CAST(NULL AS DOUBLE) AS aps,
        CAST(NULL AS TIMESTAMP) AS aps_start_timestamp,
        CAST(NULL AS TIMESTAMP) AS aps_end_timestamp,
        -- Null for gear
        CAST(NULL AS DOUBLE) AS gear,
        CAST(NULL AS TIMESTAMP) AS gear_start_timestamp,
        CAST(NULL AS TIMESTAMP) AS gear_end_timestamp,
        -- Null for nmot
        CAST(NULL AS DOUBLE) AS nmot,
        CAST(NULL AS TIMESTAMP) AS nmot_start_timestamp,
        CAST(NULL AS TIMESTAMP) AS nmot_end_timestamp,
        -- Null for acc
        CAST(NULL AS DOUBLE) AS accx_can,
        CAST(NULL AS DOUBLE) AS accy_can,
        CAST(NULL AS TIMESTAMP) AS acc_start_timestamp,
        CAST(NULL AS TIMESTAMP) AS acc_end_timestamp,
        -- Null for speed
        CAST(NULL AS DOUBLE) AS speed,
        CAST(NULL AS TIMESTAMP) AS speed_start_timestamp,
        CAST(NULL AS TIMESTAMP) AS speed_end_timestamp,
        -- Steering angle values
        steering_angle,
        steering_angle_start_timestamp,
        steering_angle_end_timestamp,
        -- Null for brake pressure
        CAST(NULL AS DOUBLE) AS front_brake_pressure,
        CAST(NULL AS TIMESTAMP) AS front_brake_pressure_start_timestamp,
        CAST(NULL AS TIMESTAMP) AS front_brake_pressure_end_timestamp,
        CAST(NULL AS DOUBLE) AS rear_brake_pressure,
        CAST(NULL AS TIMESTAMP) AS rear_brake_pressure_start_timestamp,
        CAST(NULL AS TIMESTAMP) AS rear_brake_pressure_end_timestamp
    FROM {{ ref('fact_steering_angle') }}
),

-- Front brake pressure changes with metric values
front_brake_pressure_events AS (
    SELECT
        circuit,
        race_number,
        vehicle_id,
        vehicle_number,
        front_brake_pressure_start_timestamp AS event_timestamp,
        'front_brake_pressure' AS event_source,
        lap,
        outing,
        meta_event,
        meta_session,
        -- Null for position
        CAST(NULL AS DOUBLE) AS vbox_long_minutes,
        CAST(NULL AS DOUBLE) AS vbox_lat_minutes,
        CAST(NULL AS TIMESTAMP) AS long_meta_time,
        CAST(NULL AS TIMESTAMP) AS lat_meta_time,
        CAST(NULL AS TIMESTAMP) AS position_start_timestamp,
        CAST(NULL AS TIMESTAMP) AS position_end_timestamp,
        -- Null for laptrigger
        CAST(NULL AS DOUBLE) AS laptrigger_lapdist_dls,
        CAST(NULL AS TIMESTAMP) AS laptrigger_lapdist_dls_start_timestamp,
        CAST(NULL AS TIMESTAMP) AS laptrigger_lapdist_dls_end_timestamp,
        -- Null for APS
        CAST(NULL AS DOUBLE) AS aps,
        CAST(NULL AS TIMESTAMP) AS aps_start_timestamp,
        CAST(NULL AS TIMESTAMP) AS aps_end_timestamp,
        -- Null for gear
        CAST(NULL AS DOUBLE) AS gear,
        CAST(NULL AS TIMESTAMP) AS gear_start_timestamp,
        CAST(NULL AS TIMESTAMP) AS gear_end_timestamp,
        -- Null for nmot
        CAST(NULL AS DOUBLE) AS nmot,
        CAST(NULL AS TIMESTAMP) AS nmot_start_timestamp,
        CAST(NULL AS TIMESTAMP) AS nmot_end_timestamp,
        -- Null for acc
        CAST(NULL AS DOUBLE) AS accx_can,
        CAST(NULL AS DOUBLE) AS accy_can,
        CAST(NULL AS TIMESTAMP) AS acc_start_timestamp,
        CAST(NULL AS TIMESTAMP) AS acc_end_timestamp,
        -- Null for speed
        CAST(NULL AS DOUBLE) AS speed,
        CAST(NULL AS TIMESTAMP) AS speed_start_timestamp,
        CAST(NULL AS TIMESTAMP) AS speed_end_timestamp,
        -- Null for steering_angle
        CAST(NULL AS DOUBLE) AS steering_angle,
        CAST(NULL AS TIMESTAMP) AS steering_angle_start_timestamp,
        CAST(NULL AS TIMESTAMP) AS steering_angle_end_timestamp,
        -- Front brake pressure values
        front_brake_pressure,
        front_brake_pressure_start_timestamp,
        front_brake_pressure_end_timestamp,
        -- Null for rear brake pressure
        CAST(NULL AS DOUBLE) AS rear_brake_pressure,
        CAST(NULL AS TIMESTAMP) AS rear_brake_pressure_start_timestamp,
        CAST(NULL AS TIMESTAMP) AS rear_brake_pressure_end_timestamp
    FROM {{ ref('fact_front_brake_pressure') }}
),

-- Rear brake pressure changes with metric values
rear_brake_pressure_events AS (
    SELECT
        circuit,
        race_number,
        vehicle_id,
        vehicle_number,
        rear_brake_pressure_start_timestamp AS event_timestamp,
        'rear_brake_pressure' AS event_source,
        lap,
        outing,
        meta_event,
        meta_session,
        -- Null for position
        CAST(NULL AS DOUBLE) AS vbox_long_minutes,
        CAST(NULL AS DOUBLE) AS vbox_lat_minutes,
        CAST(NULL AS TIMESTAMP) AS long_meta_time,
        CAST(NULL AS TIMESTAMP) AS lat_meta_time,
        CAST(NULL AS TIMESTAMP) AS position_start_timestamp,
        CAST(NULL AS TIMESTAMP) AS position_end_timestamp,
        -- Null for laptrigger
        CAST(NULL AS DOUBLE) AS laptrigger_lapdist_dls,
        CAST(NULL AS TIMESTAMP) AS laptrigger_lapdist_dls_start_timestamp,
        CAST(NULL AS TIMESTAMP) AS laptrigger_lapdist_dls_end_timestamp,
        -- Null for APS
        CAST(NULL AS DOUBLE) AS aps,
        CAST(NULL AS TIMESTAMP) AS aps_start_timestamp,
        CAST(NULL AS TIMESTAMP) AS aps_end_timestamp,
        -- Null for gear
        CAST(NULL AS DOUBLE) AS gear,
        CAST(NULL AS TIMESTAMP) AS gear_start_timestamp,
        CAST(NULL AS TIMESTAMP) AS gear_end_timestamp,
        -- Null for nmot
        CAST(NULL AS DOUBLE) AS nmot,
        CAST(NULL AS TIMESTAMP) AS nmot_start_timestamp,
        CAST(NULL AS TIMESTAMP) AS nmot_end_timestamp,
        -- Null for acc
        CAST(NULL AS DOUBLE) AS accx_can,
        CAST(NULL AS DOUBLE) AS accy_can,
        CAST(NULL AS TIMESTAMP) AS acc_start_timestamp,
        CAST(NULL AS TIMESTAMP) AS acc_end_timestamp,
        -- Null for speed
        CAST(NULL AS DOUBLE) AS speed,
        CAST(NULL AS TIMESTAMP) AS speed_start_timestamp,
        CAST(NULL AS TIMESTAMP) AS speed_end_timestamp,
        -- Null for steering_angle
        CAST(NULL AS DOUBLE) AS steering_angle,
        CAST(NULL AS TIMESTAMP) AS steering_angle_start_timestamp,
        CAST(NULL AS TIMESTAMP) AS steering_angle_end_timestamp,
        -- Null for front brake pressure
        CAST(NULL AS DOUBLE) AS front_brake_pressure,
        CAST(NULL AS TIMESTAMP) AS front_brake_pressure_start_timestamp,
        CAST(NULL AS TIMESTAMP) AS front_brake_pressure_end_timestamp,
        -- Rear brake pressure values
        rear_brake_pressure,
        rear_brake_pressure_start_timestamp,
        rear_brake_pressure_end_timestamp
    FROM {{ ref('fact_rear_brake_pressure') }}
),

-- Combine all events into one timeline
all_events AS (
    SELECT * FROM position_events
    UNION ALL
    SELECT * FROM laptrigger_events
    UNION ALL
    SELECT * FROM aps_events
    UNION ALL
    SELECT * FROM gear_events
    UNION ALL
    SELECT * FROM nmot_events
    UNION ALL
    SELECT * FROM acc_events
    UNION ALL
    SELECT * FROM speed_events
    UNION ALL
    SELECT * FROM steering_angle_events
    UNION ALL
    SELECT * FROM front_brake_pressure_events
    UNION ALL
    SELECT * FROM rear_brake_pressure_events
),

-- Use window functions to carry forward the last known value for each metric
-- This avoids expensive range joins
events_with_carried_values AS (
    SELECT
        circuit,
        race_number,
        vehicle_id,
        vehicle_number,
        event_timestamp,
        event_source,
        lap,
        outing,
        meta_event,
        meta_session,
        
        -- Carry forward position values using LAST_VALUE with IGNORE NULLS
        LAST_VALUE(vbox_long_minutes IGNORE NULLS) OVER (
            PARTITION BY circuit, race_number, vehicle_id 
            ORDER BY event_timestamp
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS vbox_long_minutes,
        
        LAST_VALUE(vbox_lat_minutes IGNORE NULLS) OVER (
            PARTITION BY circuit, race_number, vehicle_id 
            ORDER BY event_timestamp
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS vbox_lat_minutes,
        
        LAST_VALUE(long_meta_time IGNORE NULLS) OVER (
            PARTITION BY circuit, race_number, vehicle_id 
            ORDER BY event_timestamp
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS long_meta_time,
        
        LAST_VALUE(lat_meta_time IGNORE NULLS) OVER (
            PARTITION BY circuit, race_number, vehicle_id 
            ORDER BY event_timestamp
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS lat_meta_time,
        
        LAST_VALUE(position_start_timestamp IGNORE NULLS) OVER (
            PARTITION BY circuit, race_number, vehicle_id 
            ORDER BY event_timestamp
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS position_start_timestamp,
        
        LAST_VALUE(position_end_timestamp IGNORE NULLS) OVER (
            PARTITION BY circuit, race_number, vehicle_id 
            ORDER BY event_timestamp
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS position_end_timestamp,
        
        -- Carry forward laptrigger values
        LAST_VALUE(laptrigger_lapdist_dls IGNORE NULLS) OVER (
            PARTITION BY circuit, race_number, vehicle_id 
            ORDER BY event_timestamp
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS laptrigger_lapdist_dls,
        
        LAST_VALUE(laptrigger_lapdist_dls_start_timestamp IGNORE NULLS) OVER (
            PARTITION BY circuit, race_number, vehicle_id 
            ORDER BY event_timestamp
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS laptrigger_lapdist_dls_start_timestamp,
        
        LAST_VALUE(laptrigger_lapdist_dls_end_timestamp IGNORE NULLS) OVER (
            PARTITION BY circuit, race_number, vehicle_id 
            ORDER BY event_timestamp
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS laptrigger_lapdist_dls_end_timestamp,
        
        -- Carry forward APS values
        LAST_VALUE(aps IGNORE NULLS) OVER (
            PARTITION BY circuit, race_number, vehicle_id 
            ORDER BY event_timestamp
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS aps,
        
        LAST_VALUE(aps_start_timestamp IGNORE NULLS) OVER (
            PARTITION BY circuit, race_number, vehicle_id 
            ORDER BY event_timestamp
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS aps_start_timestamp,
        
        LAST_VALUE(aps_end_timestamp IGNORE NULLS) OVER (
            PARTITION BY circuit, race_number, vehicle_id 
            ORDER BY event_timestamp
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS aps_end_timestamp,
        
        -- Carry forward gear values
        LAST_VALUE(gear IGNORE NULLS) OVER (
            PARTITION BY circuit, race_number, vehicle_id 
            ORDER BY event_timestamp
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS gear,
        
        LAST_VALUE(gear_start_timestamp IGNORE NULLS) OVER (
            PARTITION BY circuit, race_number, vehicle_id 
            ORDER BY event_timestamp
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS gear_start_timestamp,
        
        LAST_VALUE(gear_end_timestamp IGNORE NULLS) OVER (
            PARTITION BY circuit, race_number, vehicle_id 
            ORDER BY event_timestamp
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS gear_end_timestamp,
        
        -- Carry forward nmot values
        LAST_VALUE(nmot IGNORE NULLS) OVER (
            PARTITION BY circuit, race_number, vehicle_id 
            ORDER BY event_timestamp
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS nmot,
        
        LAST_VALUE(nmot_start_timestamp IGNORE NULLS) OVER (
            PARTITION BY circuit, race_number, vehicle_id 
            ORDER BY event_timestamp
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS nmot_start_timestamp,
        
        LAST_VALUE(nmot_end_timestamp IGNORE NULLS) OVER (
            PARTITION BY circuit, race_number, vehicle_id 
            ORDER BY event_timestamp
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS nmot_end_timestamp,
        
        -- Carry forward acceleration values
        LAST_VALUE(accx_can IGNORE NULLS) OVER (
            PARTITION BY circuit, race_number, vehicle_id 
            ORDER BY event_timestamp
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS accx_can,
        
        LAST_VALUE(accy_can IGNORE NULLS) OVER (
            PARTITION BY circuit, race_number, vehicle_id 
            ORDER BY event_timestamp
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS accy_can,
        
        LAST_VALUE(acc_start_timestamp IGNORE NULLS) OVER (
            PARTITION BY circuit, race_number, vehicle_id 
            ORDER BY event_timestamp
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS acc_start_timestamp,
        
        LAST_VALUE(acc_end_timestamp IGNORE NULLS) OVER (
            PARTITION BY circuit, race_number, vehicle_id 
            ORDER BY event_timestamp
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS acc_end_timestamp,
        
        -- Carry forward speed values
        LAST_VALUE(speed IGNORE NULLS) OVER (
            PARTITION BY circuit, race_number, vehicle_id 
            ORDER BY event_timestamp
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS speed,
        
        LAST_VALUE(speed_start_timestamp IGNORE NULLS) OVER (
            PARTITION BY circuit, race_number, vehicle_id 
            ORDER BY event_timestamp
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS speed_start_timestamp,
        
        LAST_VALUE(speed_end_timestamp IGNORE NULLS) OVER (
            PARTITION BY circuit, race_number, vehicle_id 
            ORDER BY event_timestamp
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS speed_end_timestamp,
        
        -- Carry forward steering_angle values
        LAST_VALUE(steering_angle IGNORE NULLS) OVER (
            PARTITION BY circuit, race_number, vehicle_id 
            ORDER BY event_timestamp
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS steering_angle,
        
        LAST_VALUE(steering_angle_start_timestamp IGNORE NULLS) OVER (
            PARTITION BY circuit, race_number, vehicle_id 
            ORDER BY event_timestamp
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS steering_angle_start_timestamp,
        
        LAST_VALUE(steering_angle_end_timestamp IGNORE NULLS) OVER (
            PARTITION BY circuit, race_number, vehicle_id 
            ORDER BY event_timestamp
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS steering_angle_end_timestamp,
        
        -- Carry forward front brake pressure values
        LAST_VALUE(front_brake_pressure IGNORE NULLS) OVER (
            PARTITION BY circuit, race_number, vehicle_id 
            ORDER BY event_timestamp
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS front_brake_pressure,
        
        LAST_VALUE(front_brake_pressure_start_timestamp IGNORE NULLS) OVER (
            PARTITION BY circuit, race_number, vehicle_id 
            ORDER BY event_timestamp
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS front_brake_pressure_start_timestamp,
        
        LAST_VALUE(front_brake_pressure_end_timestamp IGNORE NULLS) OVER (
            PARTITION BY circuit, race_number, vehicle_id 
            ORDER BY event_timestamp
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS front_brake_pressure_end_timestamp,
        
        -- Carry forward rear brake pressure values
        LAST_VALUE(rear_brake_pressure IGNORE NULLS) OVER (
            PARTITION BY circuit, race_number, vehicle_id 
            ORDER BY event_timestamp
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS rear_brake_pressure,
        
        LAST_VALUE(rear_brake_pressure_start_timestamp IGNORE NULLS) OVER (
            PARTITION BY circuit, race_number, vehicle_id 
            ORDER BY event_timestamp
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS rear_brake_pressure_start_timestamp,
        
        LAST_VALUE(rear_brake_pressure_end_timestamp IGNORE NULLS) OVER (
            PARTITION BY circuit, race_number, vehicle_id 
            ORDER BY event_timestamp
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS rear_brake_pressure_end_timestamp
        
    FROM all_events
)

SELECT
    -- Grain columns
    circuit,
    race_number,
    vehicle_id,
    vehicle_number,
    event_timestamp,
    event_source,
    
    -- Context columns
    lap,
    outing,
    meta_event,
    meta_session,
    
    -- Position data (carried forward)
    position_start_timestamp,
    position_end_timestamp,
    vbox_long_minutes,
    vbox_lat_minutes,
    long_meta_time,
    lat_meta_time,
    
    -- Laptrigger data (carried forward)
    laptrigger_lapdist_dls_start_timestamp,
    laptrigger_lapdist_dls_end_timestamp,
    laptrigger_lapdist_dls,
    
    -- APS data (carried forward)
    aps_start_timestamp,
    aps_end_timestamp,
    aps,
    
    -- Gear data (carried forward)
    gear_start_timestamp,
    gear_end_timestamp,
    gear,
    
    -- Nmot data (carried forward)
    nmot_start_timestamp,
    nmot_end_timestamp,
    nmot,
    
    -- Acceleration data (carried forward)
    acc_start_timestamp,
    acc_end_timestamp,
    accx_can,
    accy_can,
    
    -- Speed data (carried forward)
    speed_start_timestamp,
    speed_end_timestamp,
    speed,
    
    -- Steering angle data (carried forward)
    steering_angle_start_timestamp,
    steering_angle_end_timestamp,
    steering_angle,
    
    -- Front brake pressure data (carried forward)
    front_brake_pressure_start_timestamp,
    front_brake_pressure_end_timestamp,
    front_brake_pressure,
    
    -- Rear brake pressure data (carried forward)
    rear_brake_pressure_start_timestamp,
    rear_brake_pressure_end_timestamp,
    rear_brake_pressure
    
FROM events_with_carried_values
