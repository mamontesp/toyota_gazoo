{{
    config(
        materialized='table'
    )
}}

-- Track surface temperature records captured during Barber Race 1
WITH weather_track_temp AS (
    -- Extract the raw track temperature readings for Barber Race 1
    SELECT
        time_utc_str,
        time_utc_seconds,
        track_temp AS track_temp_fahrenheit,
        (track_temp -32) * 5/9 AS track_temp_celsius,
        'barber' AS circuit,
        1 AS race_number
    FROM {{ ref('raw_barber_race_1_weather') }}
)

SELECT
    time_utc_str AS time_utc,
    time_utc_seconds,
    track_temp_fahrenheit,
    track_temp_celsius,
    circuit,
    race_number
FROM weather_track_temp
WHERE track_temp_fahrenheit IS NOT NULL

