{{
    config(
        materialized='table'
    )
}}

-- Wind direction observations captured during Barber Race 1
WITH weather_wind_direction AS (
    -- Extract the raw wind direction readings for Barber Race 1
    SELECT
        time_utc_str,
        time_utc_seconds,
        wind_direction AS wind_direction_degrees,
        'barber' AS circuit,
        1 AS race_number
    FROM {{ ref('raw_barber_race_1_weather') }}
)

SELECT
    time_utc_str AS time_utc,
    time_utc_seconds,
    wind_direction_degrees,
    circuit,
    race_number
FROM weather_wind_direction
WHERE wind_direction_degrees IS NOT NULL

