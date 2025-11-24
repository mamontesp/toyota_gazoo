{{
    config(
        materialized='table'
    )
}}

-- Fact table capturing each change in acceleration with validity periods
-- Each row represents a unique change in the accx_can or accy_can metrics
-- The grain is one record per value change per vehicle
-- acc_start_timestamp and acc_end_timestamp define when this value was active
-- This allows multiple telemetry records to reference the same acceleration value during its validity period

WITH 
-- Deduplicate longitudinal acceleration data - keep most recent by meta_time
accx_ranked AS (
    SELECT
        circuit,
        race_number,
        vehicle_id,
        vehicle_number,
        timestamp,
        meta_time,
        meta_source,
        original_vehicle_id,
        expire_at,
        accx_can,
        lap,
        outing,
        meta_event,
        meta_session,
        ROW_NUMBER() OVER (
            PARTITION BY circuit, race_number, vehicle_id, timestamp
            ORDER BY meta_time DESC
        ) AS rn
    FROM {{ ref('int_telemetry_data_accx_can') }}
),

accx_deduped AS (
    SELECT
        circuit,
        race_number,
        vehicle_id,
        vehicle_number,
        timestamp,
        meta_time,
        meta_source,
        original_vehicle_id,
        expire_at,
        accx_can,
        lap,
        outing,
        meta_event,
        meta_session
    FROM accx_ranked
    WHERE rn = 1
),

-- Deduplicate lateral acceleration data - keep most recent by meta_time
accy_ranked AS (
    SELECT
        circuit,
        race_number,
        vehicle_id,
        timestamp,
        meta_time,
        accy_can,
        ROW_NUMBER() OVER (
            PARTITION BY circuit, race_number, vehicle_id, timestamp
            ORDER BY meta_time DESC
        ) AS rn
    FROM {{ ref('int_telemetry_data_accy_can') }}
),

accy_deduped AS (
    SELECT
        circuit,
        race_number,
        vehicle_id,
        timestamp,
        meta_time,
        accy_can
    FROM accy_ranked
    WHERE rn = 1
),

-- Combine acceleration data - FULL OUTER JOIN to get all timestamps from both streams
acc_combined AS (
    SELECT
        COALESCE(accx.circuit, accy.circuit) AS circuit,
        COALESCE(accx.race_number, accy.race_number) AS race_number,
        COALESCE(accx.vehicle_id, accy.vehicle_id) AS vehicle_id,
        COALESCE(accx.timestamp, accy.timestamp) AS timestamp,
        accx.vehicle_number,
        accx.accx_can,
        accy.accy_can,
        accx.lap,
        accx.outing,
        accx.meta_event,
        accx.meta_session,
        accx.meta_source,
        accx.meta_time,
        accx.original_vehicle_id,
        accx.expire_at
    FROM accx_deduped accx
    FULL OUTER JOIN accy_deduped accy
        ON accx.circuit = accy.circuit
        AND accx.race_number = accy.race_number
        AND accx.vehicle_id = accy.vehicle_id
        AND accx.timestamp = accy.timestamp
),

-- Forward fill acceleration values for all timestamps
acc_filled AS (
    SELECT
        circuit,
        race_number,
        vehicle_id,
        vehicle_number,
        timestamp,
        -- Forward fill acceleration values
        LAST_VALUE(accx_can IGNORE NULLS) OVER (
            PARTITION BY circuit, race_number, vehicle_id
            ORDER BY timestamp
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS accx_can,
        LAST_VALUE(accy_can IGNORE NULLS) OVER (
            PARTITION BY circuit, race_number, vehicle_id
            ORDER BY timestamp
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS accy_can,
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
        LAST_VALUE(meta_source IGNORE NULLS) OVER (
            PARTITION BY circuit, race_number, vehicle_id
            ORDER BY timestamp
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS meta_source,
        LAST_VALUE(meta_time IGNORE NULLS) OVER (
            PARTITION BY circuit, race_number, vehicle_id
            ORDER BY timestamp
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS meta_time,
        LAST_VALUE(original_vehicle_id IGNORE NULLS) OVER (
            PARTITION BY circuit, race_number, vehicle_id
            ORDER BY timestamp
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS original_vehicle_id,
        LAST_VALUE(expire_at IGNORE NULLS) OVER (
            PARTITION BY circuit, race_number, vehicle_id
            ORDER BY timestamp
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS expire_at
    FROM acc_combined
),

-- Compare current acceleration with previous to detect changes
acc_with_lag AS (
    SELECT
        *,
        LAG(accx_can) OVER (PARTITION BY circuit, race_number, vehicle_id ORDER BY timestamp) AS prev_accx_can,
        LAG(accy_can) OVER (PARTITION BY circuit, race_number, vehicle_id ORDER BY timestamp) AS prev_accy_can
    FROM acc_filled
),

-- Mark acceleration changes and create groups
acc_groups AS (
    SELECT
        circuit,
        race_number,
        vehicle_id,
        vehicle_number,
        timestamp,
        accx_can,
        accy_can,
        lap,
        outing,
        meta_event,
        meta_session,
        meta_source,
        meta_time,
        original_vehicle_id,
        expire_at,
        SUM(CASE 
            WHEN prev_accx_can IS DISTINCT FROM accx_can
                OR prev_accy_can IS DISTINCT FROM accy_can
            THEN 1 
            ELSE 0 
        END) OVER (
            PARTITION BY circuit, race_number, vehicle_id 
            ORDER BY timestamp
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS acc_change_group
    FROM acc_with_lag
),

-- Aggregate each group to get start timestamp
acc_periods AS (
    SELECT
        -- Grain columns
        circuit,
        race_number,
        vehicle_id,
        MIN(vehicle_number) AS vehicle_number,
        
        -- Start timestamp
        MIN(timestamp) AS acc_start_timestamp,
        
        -- Context columns (taking first values from the group)
        MIN(lap) AS lap,
        MIN(outing) AS outing,
        MIN(meta_event) AS meta_event,
        MIN(meta_session) AS meta_session,
        MIN(meta_source) AS meta_source,
        MIN(meta_time) AS meta_time,
        
        -- Metric columns
        accx_can,
        accy_can,
        
        -- Additional columns
        MIN(original_vehicle_id) AS original_vehicle_id,
        MIN(expire_at) AS expire_at,
        
        -- Keep group id for ordering
        acc_change_group
        
    FROM acc_groups
    GROUP BY 
        circuit,
        race_number,
        vehicle_id,
        acc_change_group,
        accx_can,
        accy_can
)

-- Calculate end timestamp and previous acceleration values from the previous group
SELECT
    -- Grain columns
    circuit,
    race_number,
    vehicle_id,
    vehicle_number,
    
    -- Validity period
    acc_start_timestamp,
    LEAD(acc_start_timestamp) OVER (
        PARTITION BY circuit, race_number, vehicle_id 
        ORDER BY acc_start_timestamp
    ) AS acc_end_timestamp,
    
    -- Context columns
    lap,
    outing,
    meta_event,
    meta_session,
    meta_source,
    meta_time,
    
    -- Metric columns
    accx_can,
    accy_can,
    LAG(accx_can) OVER (
        PARTITION BY circuit, race_number, vehicle_id 
        ORDER BY acc_start_timestamp
    ) AS previous_accx_can,
    LAG(accy_can) OVER (
        PARTITION BY circuit, race_number, vehicle_id 
        ORDER BY acc_start_timestamp
    ) AS previous_accy_can,
    accx_can - COALESCE(LAG(accx_can) OVER (
        PARTITION BY circuit, race_number, vehicle_id 
        ORDER BY acc_start_timestamp
    ), 0) AS accx_change,
    accy_can - COALESCE(LAG(accy_can) OVER (
        PARTITION BY circuit, race_number, vehicle_id 
        ORDER BY acc_start_timestamp
    ), 0) AS accy_change,
    
    -- Additional columns
    original_vehicle_id,
    expire_at
    
FROM acc_periods



