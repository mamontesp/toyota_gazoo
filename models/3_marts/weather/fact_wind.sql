{{
    config(
        materialized='table'
    )
}}

-- Wind direction readings enriched with normalized values
WITH wind_direction AS (
    SELECT
        time_utc,
        time_utc_seconds,
        wind_direction_degrees,
        CASE
            WHEN wind_direction_degrees = 360 THEN 0
            ELSE wind_direction_degrees
        END AS wind_direction_degrees_normalized,
        circuit,
        race_number
    FROM {{ ref('int_weather_wind_direction') }}
),

-- Wind speed readings per timestamp
wind_speed AS (
    SELECT
        time_utc,
        time_utc_seconds,
        wind_speed_mph,
        wind_speed_kph,
        circuit,
        race_number
    FROM {{ ref('int_weather_wind_speed') }}
),

-- Combine wind speed and direction into a single fact table
wind_fact AS (
    SELECT
        COALESCE(direction.time_utc, speed.time_utc) AS time_utc,
        COALESCE(direction.time_utc_seconds, speed.time_utc_seconds) AS time_utc_seconds,
        direction.wind_direction_degrees,
        direction.wind_direction_degrees_normalized,
        speed.wind_speed_mph,
        speed.wind_speed_kph,
        CASE
            WHEN speed.wind_speed_mph IS NULL THEN NULL
            WHEN speed.wind_speed_mph < 5 THEN '00-05 mph'
            WHEN speed.wind_speed_mph < 10 THEN '05-10 mph'
            WHEN speed.wind_speed_mph < 15 THEN '10-15 mph'
            WHEN speed.wind_speed_mph < 20 THEN '15-20 mph'
            ELSE '20+ mph'
        END AS wind_speed_band_mph,
        COALESCE(direction.circuit, speed.circuit) AS circuit,
        COALESCE(direction.race_number, speed.race_number) AS race_number
    FROM wind_direction AS direction
    FULL OUTER JOIN wind_speed AS speed
        ON direction.circuit = speed.circuit
        AND direction.race_number = speed.race_number
        AND direction.time_utc_seconds = speed.time_utc_seconds
)

SELECT
    time_utc,
    time_utc_seconds,
    wind_speed_mph,
    wind_speed_kph,
    wind_direction_degrees,
    CASE
        WHEN wind_direction_degrees_normalized IS NULL THEN NULL
        WHEN wind_direction_degrees_normalized >= 337.5
            OR wind_direction_degrees_normalized < 22.5 THEN 'North'
        WHEN wind_direction_degrees_normalized >= 22.5
            AND wind_direction_degrees_normalized < 67.5 THEN 'North East'
        WHEN wind_direction_degrees_normalized >= 67.5
            AND wind_direction_degrees_normalized < 112.5 THEN 'East'
        WHEN wind_direction_degrees_normalized >= 112.5
            AND wind_direction_degrees_normalized < 157.5 THEN 'South East'
        WHEN wind_direction_degrees_normalized >= 157.5
            AND wind_direction_degrees_normalized < 202.5 THEN 'South'
        WHEN wind_direction_degrees_normalized >= 202.5
            AND wind_direction_degrees_normalized < 247.5 THEN 'South West'
        WHEN wind_direction_degrees_normalized >= 247.5
            AND wind_direction_degrees_normalized < 292.5 THEN 'West'
        WHEN wind_direction_degrees_normalized >= 292.5
            AND wind_direction_degrees_normalized < 337.5 THEN 'North West'
        ELSE NULL
    END AS wind_direction_label,
    wind_speed_band_mph,
    circuit,
    race_number
FROM wind_fact
WHERE wind_speed_mph IS NOT NULL
    OR wind_direction_degrees IS NOT NULL
