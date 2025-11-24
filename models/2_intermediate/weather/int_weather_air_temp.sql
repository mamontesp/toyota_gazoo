{{
    config(
        materialized='table'
    )
}}

-- Air temperature records captured during Barber Race 1
WITH weather_air_temp AS (
    -- Extract the raw weather readings for Barber Race 1
    SELECT
        time_utc_str,
        time_utc_seconds,
        air_temp AS air_temp_fahrenheit,
        (air_temp -32) * 5/9 AS air_temp_celsius,
        'barber' AS circuit,
        1 AS race_number
    FROM {{ ref('raw_barber_race_1_weather') }}
)

SELECT
    time_utc_str AS time_utc,
    time_utc_seconds,
    air_temp_fahrenheit,
    air_temp_celsius,
    circuit,
    race_number
FROM weather_air_temp
WHERE air_temp_fahrenheit IS NOT NULL

