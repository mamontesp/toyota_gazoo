{{
    config(
        materialized='table'
    )
}}

-- Lap start timestamps enriched with lap context
WITH lap_start AS (
    SELECT
        circuit,
        race_number,
        vehicle_id,
        vehicle_number,
        lap,
        outing,
        lap_start_timestamp_utc,
        LEAD(lap_start_timestamp_utc) OVER (
            PARTITION BY circuit, race_number, vehicle_id, outing
            ORDER BY lap
        ) AS next_lap_start_timestamp_utc,
        meta_event,
        meta_session,
        meta_source,
        meta_time,
        original_vehicle_id,
        expire_at
    FROM {{ ref('int_laps_start') }}
),

-- Lap end timestamps enriched with lap context
lap_end AS (
    SELECT
        circuit,
        race_number,
        vehicle_id,
        vehicle_number,
        lap,
        outing,
        lap_end_timestamp_utc,
        LAG(lap_end_timestamp_utc) OVER (
            PARTITION BY circuit, race_number, vehicle_id, outing
            ORDER BY lap
        ) AS previous_lap_end_timestamp_utc,
        meta_event,
        meta_session,
        meta_source,
        meta_time,
        original_vehicle_id,
        expire_at
    FROM {{ ref('int_laps_end') }}
),

-- Combine lap start and end timestamps per lap occurrence
combined_laps AS (
    SELECT
        COALESCE(start_data.circuit, end_data.circuit) AS circuit,
        COALESCE(start_data.race_number, end_data.race_number) AS race_number,
        COALESCE(start_data.vehicle_id, end_data.vehicle_id) AS vehicle_id,
        COALESCE(start_data.vehicle_number, end_data.vehicle_number) AS vehicle_number,
        COALESCE(start_data.lap, end_data.lap) AS lap,
        COALESCE(start_data.outing, end_data.outing) AS outing,
        COALESCE(start_data.lap_start_timestamp_utc, previous_lap_end_timestamp_utc) AS lap_start_timestamp_utc,
        COALESCE(end_data.lap_end_timestamp_utc, start_data.next_lap_start_timestamp_utc) AS lap_end_timestamp_utc,
        CASE
            WHEN lap_start_timestamp_utc IS NOT NULL
                AND lap_end_timestamp_utc IS NOT NULL
            THEN DATEDIFF('second', lap_start_timestamp_utc, lap_end_timestamp_utc)
            ELSE NULL
        END AS lap_duration_seconds,
        CASE
            WHEN lap_start_timestamp_utc IS NOT NULL
                AND lap_end_timestamp_utc IS NOT NULL
            THEN lap_end_timestamp_utc - lap_start_timestamp_utc
            ELSE NULL
        END AS lap_duration_minutes,
        COALESCE(start_data.meta_event, end_data.meta_event) AS meta_event,
        COALESCE(start_data.meta_session, end_data.meta_session) AS meta_session,
        COALESCE(start_data.meta_source, end_data.meta_source) AS meta_source,
        COALESCE(start_data.meta_time, end_data.meta_time) AS meta_time,
        COALESCE(start_data.original_vehicle_id, end_data.original_vehicle_id) AS original_vehicle_id,
        COALESCE(start_data.expire_at, end_data.expire_at) AS expire_at
    FROM lap_start AS start_data
    FULL OUTER JOIN lap_end AS end_data
        ON start_data.circuit = end_data.circuit
        AND start_data.race_number = end_data.race_number
        AND start_data.vehicle_id = end_data.vehicle_id
        AND start_data.outing = end_data.outing
        AND start_data.lap = end_data.lap
),

-- Humidity observations with normalized timestamp
weather_humidity AS (
    SELECT
        circuit,
        race_number,
        time_utc,
        humidity_percentage
    FROM {{ ref('int_weather_humidity') }}
    WHERE humidity_percentage IS NOT NULL
),

-- Atmospheric pressure observations with normalized timestamp
weather_pressure AS (
    SELECT
        circuit,
        race_number,
        time_utc,
        pressure_inches,
        pressure_mbars
    FROM {{ ref('int_weather_pressure') }}
    WHERE pressure_inches IS NOT NULL
),

-- Air temperature observations with normalized timestamp
weather_air_temp AS (
    SELECT
        circuit,
        race_number,
        time_utc,
        air_temp_fahrenheit,
        air_temp_celsius
    FROM {{ ref('int_weather_air_temp') }}
    WHERE air_temp_fahrenheit IS NOT NULL
),

-- Track temperature observations with normalized timestamp
weather_track_temp AS (
    SELECT
        circuit,
        race_number,
        time_utc,
        track_temp_fahrenheit,
        track_temp_celsius
    FROM {{ ref('int_weather_track_temp') }}
    WHERE track_temp_fahrenheit IS NOT NULL
),

-- Lap-level humidity statistics derived from weather observations
lap_humidity_stats AS (
    SELECT
        laps.circuit,
        laps.race_number,
        laps.vehicle_id,
        laps.vehicle_number,
        laps.lap,
        laps.outing,
        MAX(humidity.humidity_percentage) OVER (
            PARTITION BY laps.circuit,
                laps.race_number,
                laps.vehicle_id,
                laps.vehicle_number,
                laps.lap,
                laps.outing
        ) AS lap_max_humidity_percentage,
        AVG(humidity.humidity_percentage) OVER (
            PARTITION BY laps.circuit,
                laps.race_number,
                laps.vehicle_id,
                laps.vehicle_number,
                laps.lap,
                laps.outing
        ) AS lap_mean_humidity_percentage,
        MEDIAN(humidity.humidity_percentage) OVER (
            PARTITION BY laps.circuit,
                laps.race_number,
                laps.vehicle_id,
                laps.vehicle_number,
                laps.lap,
                laps.outing
        ) AS lap_median_humidity_percentage
    FROM combined_laps AS laps
    LEFT JOIN weather_humidity AS humidity
        ON humidity.circuit = laps.circuit
        AND humidity.race_number = laps.race_number
        AND humidity.time_utc >= laps.lap_start_timestamp_utc
        AND humidity.time_utc <= laps.lap_end_timestamp_utc
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY laps.circuit,
            laps.race_number,
            laps.vehicle_id,
            laps.vehicle_number,
            laps.lap,
            laps.outing
        ORDER BY laps.lap_start_timestamp_utc
    ) = 1
),

-- Lap-level pressure statistics derived from weather observations
lap_pressure_stats AS (
    SELECT
        laps.circuit,
        laps.race_number,
        laps.vehicle_id,
        laps.vehicle_number,
        laps.lap,
        laps.outing,
        MAX(pressure.pressure_inches) OVER (
            PARTITION BY laps.circuit,
                laps.race_number,
                laps.vehicle_id,
                laps.vehicle_number,
                laps.lap,
                laps.outing
        ) AS lap_max_pressure_inches,
        AVG(pressure.pressure_inches) OVER (
            PARTITION BY laps.circuit,
                laps.race_number,
                laps.vehicle_id,
                laps.vehicle_number,
                laps.lap,
                laps.outing
        ) AS lap_mean_pressure_inches,
        MEDIAN(pressure.pressure_inches) OVER (
            PARTITION BY laps.circuit,
                laps.race_number,
                laps.vehicle_id,
                laps.vehicle_number,
                laps.lap,
                laps.outing
        ) AS lap_median_pressure_inches,
        MAX(pressure.pressure_mbars) OVER (
            PARTITION BY laps.circuit,
                laps.race_number,
                laps.vehicle_id,
                laps.vehicle_number,
                laps.lap,
                laps.outing
        ) AS lap_max_pressure_mbars,
        AVG(pressure.pressure_mbars) OVER (
            PARTITION BY laps.circuit,
                laps.race_number,
                laps.vehicle_id,
                laps.vehicle_number,
                laps.lap,
                laps.outing
        ) AS lap_mean_pressure_mbars,
        MEDIAN(pressure.pressure_mbars) OVER (
            PARTITION BY laps.circuit,
                laps.race_number,
                laps.vehicle_id,
                laps.vehicle_number,
                laps.lap,
                laps.outing
        ) AS lap_median_pressure_mbars
    FROM combined_laps AS laps
    LEFT JOIN weather_pressure AS pressure
        ON pressure.circuit = laps.circuit
        AND pressure.race_number = laps.race_number
        AND pressure.time_utc >= laps.lap_start_timestamp_utc
        AND pressure.time_utc <= laps.lap_end_timestamp_utc
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY laps.circuit,
            laps.race_number,
            laps.vehicle_id,
            laps.vehicle_number,
            laps.lap,
            laps.outing
        ORDER BY laps.lap_start_timestamp_utc
    ) = 1
),

-- Lap-level air temperature statistics derived from weather observations
lap_air_temp_stats AS (
    SELECT
        laps.circuit,
        laps.race_number,
        laps.vehicle_id,
        laps.vehicle_number,
        laps.lap,
        laps.outing,
        MAX(air_temp.air_temp_fahrenheit) OVER (
            PARTITION BY laps.circuit,
                laps.race_number,
                laps.vehicle_id,
                laps.vehicle_number,
                laps.lap,
                laps.outing
        ) AS lap_max_air_temp_fahrenheit,
        AVG(air_temp.air_temp_fahrenheit) OVER (
            PARTITION BY laps.circuit,
                laps.race_number,
                laps.vehicle_id,
                laps.vehicle_number,
                laps.lap,
                laps.outing
        ) AS lap_mean_air_temp_fahrenheit,
        MEDIAN(air_temp.air_temp_fahrenheit) OVER (
            PARTITION BY laps.circuit,
                laps.race_number,
                laps.vehicle_id,
                laps.vehicle_number,
                laps.lap,
                laps.outing
        ) AS lap_median_air_temp_fahrenheit,
        MAX(air_temp.air_temp_celsius) OVER (
            PARTITION BY laps.circuit,
                laps.race_number,
                laps.vehicle_id,
                laps.vehicle_number,
                laps.lap,
                laps.outing
        ) AS lap_max_air_temp_celsius,
        AVG(air_temp.air_temp_celsius) OVER (
            PARTITION BY laps.circuit,
                laps.race_number,
                laps.vehicle_id,
                laps.vehicle_number,
                laps.lap,
                laps.outing
        ) AS lap_mean_air_temp_celsius,
        MEDIAN(air_temp.air_temp_celsius) OVER (
            PARTITION BY laps.circuit,
                laps.race_number,
                laps.vehicle_id,
                laps.vehicle_number,
                laps.lap,
                laps.outing
        ) AS lap_median_air_temp_celsius
    FROM combined_laps AS laps
    LEFT JOIN weather_air_temp AS air_temp
        ON air_temp.circuit = laps.circuit
        AND air_temp.race_number = laps.race_number
        AND air_temp.time_utc >= laps.lap_start_timestamp_utc
        AND air_temp.time_utc <= laps.lap_end_timestamp_utc
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY laps.circuit,
            laps.race_number,
            laps.vehicle_id,
            laps.vehicle_number,
            laps.lap,
            laps.outing
        ORDER BY laps.lap_start_timestamp_utc
    ) = 1
),

-- Lap-level track temperature statistics derived from weather observations
lap_track_temp_stats AS (
    SELECT
        laps.circuit,
        laps.race_number,
        laps.vehicle_id,
        laps.vehicle_number,
        laps.lap,
        laps.outing,
        MAX(track_temp.track_temp_fahrenheit) OVER (
            PARTITION BY laps.circuit,
                laps.race_number,
                laps.vehicle_id,
                laps.vehicle_number,
                laps.lap,
                laps.outing
        ) AS lap_max_track_temp_fahrenheit,
        AVG(track_temp.track_temp_fahrenheit) OVER (
            PARTITION BY laps.circuit,
                laps.race_number,
                laps.vehicle_id,
                laps.vehicle_number,
                laps.lap,
                laps.outing
        ) AS lap_mean_track_temp_fahrenheit,
        MEDIAN(track_temp.track_temp_fahrenheit) OVER (
            PARTITION BY laps.circuit,
                laps.race_number,
                laps.vehicle_id,
                laps.vehicle_number,
                laps.lap,
                laps.outing
        ) AS lap_median_track_temp_fahrenheit,
        MAX(track_temp.track_temp_celsius) OVER (
            PARTITION BY laps.circuit,
                laps.race_number,
                laps.vehicle_id,
                laps.vehicle_number,
                laps.lap,
                laps.outing
        ) AS lap_max_track_temp_celsius,
        AVG(track_temp.track_temp_celsius) OVER (
            PARTITION BY laps.circuit,
                laps.race_number,
                laps.vehicle_id,
                laps.vehicle_number,
                laps.lap,
                laps.outing
        ) AS lap_mean_track_temp_celsius,
        MEDIAN(track_temp.track_temp_celsius) OVER (
            PARTITION BY laps.circuit,
                laps.race_number,
                laps.vehicle_id,
                laps.vehicle_number,
                laps.lap,
                laps.outing
        ) AS lap_median_track_temp_celsius
    FROM combined_laps AS laps
    LEFT JOIN weather_track_temp AS track_temp
        ON track_temp.circuit = laps.circuit
        AND track_temp.race_number = laps.race_number
        AND track_temp.time_utc >= laps.lap_start_timestamp_utc
        AND track_temp.time_utc <= laps.lap_end_timestamp_utc
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY laps.circuit,
            laps.race_number,
            laps.vehicle_id,
            laps.vehicle_number,
            laps.lap,
            laps.outing
        ORDER BY laps.lap_start_timestamp_utc
    ) = 1
)

SELECT
    laps.circuit,
    laps.race_number,
    laps.vehicle_id,
    laps.vehicle_number,
    laps.lap,
    laps.outing,
    laps.lap_start_timestamp_utc,
    laps.lap_end_timestamp_utc,
    laps.lap_duration_seconds,
    laps.lap_duration_minutes,
    laps.meta_event,
    laps.meta_session,
    laps.meta_source,
    laps.meta_time,
    laps.original_vehicle_id,
    laps.expire_at,
    ROUND(humidity.lap_max_humidity_percentage, 3) AS lap_max_humidity_percentage,
    ROUND(humidity.lap_mean_humidity_percentage, 3) AS lap_mean_humidity_percentage,
    ROUND(humidity.lap_median_humidity_percentage, 3) AS lap_median_humidity_percentage,
    ROUND(pressure.lap_max_pressure_inches, 3) AS lap_max_pressure_inches,
    ROUND(pressure.lap_mean_pressure_inches, 3) AS lap_mean_pressure_inches,
    ROUND(pressure.lap_median_pressure_inches, 3) AS lap_median_pressure_inches,
    ROUND(pressure.lap_max_pressure_mbars, 3) AS lap_max_pressure_mbars,
    ROUND(pressure.lap_mean_pressure_mbars, 3) AS lap_mean_pressure_mbars,
    ROUND(pressure.lap_median_pressure_mbars, 3) AS lap_median_pressure_mbars,
    ROUND(air_temp.lap_max_air_temp_fahrenheit, 3) AS lap_max_air_temp_fahrenheit,
    ROUND(air_temp.lap_mean_air_temp_fahrenheit, 3) AS lap_mean_air_temp_fahrenheit,
    ROUND(air_temp.lap_median_air_temp_fahrenheit, 3) AS lap_median_air_temp_fahrenheit,
    ROUND(air_temp.lap_max_air_temp_celsius, 3) AS lap_max_air_temp_celsius,
    ROUND(air_temp.lap_mean_air_temp_celsius, 3) AS lap_mean_air_temp_celsius,
    ROUND(air_temp.lap_median_air_temp_celsius, 3) AS lap_median_air_temp_celsius,
    ROUND(track_temp.lap_max_track_temp_fahrenheit, 3) AS lap_max_track_temp_fahrenheit,
    ROUND(track_temp.lap_mean_track_temp_fahrenheit, 3) AS lap_mean_track_temp_fahrenheit,
    ROUND(track_temp.lap_median_track_temp_fahrenheit, 3) AS lap_median_track_temp_fahrenheit,
    ROUND(track_temp.lap_max_track_temp_celsius, 3) AS lap_max_track_temp_celsius,
    ROUND(track_temp.lap_mean_track_temp_celsius, 3) AS lap_mean_track_temp_celsius,
    ROUND(track_temp.lap_median_track_temp_celsius, 3) AS lap_median_track_temp_celsius
FROM combined_laps AS laps
LEFT JOIN lap_humidity_stats AS humidity
    ON humidity.circuit = laps.circuit
    AND humidity.race_number = laps.race_number
    AND humidity.vehicle_id = laps.vehicle_id
    AND humidity.vehicle_number = laps.vehicle_number
    AND humidity.lap = laps.lap
    AND humidity.outing = laps.outing
LEFT JOIN lap_pressure_stats AS pressure
    ON pressure.circuit = laps.circuit
    AND pressure.race_number = laps.race_number
    AND pressure.vehicle_id = laps.vehicle_id
    AND pressure.vehicle_number = laps.vehicle_number
    AND pressure.lap = laps.lap
    AND pressure.outing = laps.outing
LEFT JOIN lap_air_temp_stats AS air_temp
    ON air_temp.circuit = laps.circuit
    AND air_temp.race_number = laps.race_number
    AND air_temp.vehicle_id = laps.vehicle_id
    AND air_temp.vehicle_number = laps.vehicle_number
    AND air_temp.lap = laps.lap
    AND air_temp.outing = laps.outing
LEFT JOIN lap_track_temp_stats AS track_temp
    ON track_temp.circuit = laps.circuit
    AND track_temp.race_number = laps.race_number
    AND track_temp.vehicle_id = laps.vehicle_id
    AND track_temp.vehicle_number = laps.vehicle_number
    AND track_temp.lap = laps.lap
    AND track_temp.outing = laps.outing
WHERE laps.lap_start_timestamp_utc IS NOT NULL
    OR laps.lap_end_timestamp_utc IS NOT NULL
