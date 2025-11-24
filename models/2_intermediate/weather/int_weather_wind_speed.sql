{{
    config(
        materialized='table'
    )
}}

-- Wind speed observations captured during Barber Race 1
WITH weather_wind_speed AS (
    -- Extract the raw wind speed readings for Barber Race 1
    SELECT
        time_utc_str,
        time_utc_seconds,
        wind_speed AS wind_speed_mph,
        wind_speed * 1.609 AS wind_speed_kph,
        'barber' AS circuit,
        1 AS race_number
    FROM {{ ref('raw_barber_race_1_weather') }}
)

SELECT
    time_utc_str AS time_utc,
    time_utc_seconds,
    wind_speed_mph,
    wind_speed_kph,
    circuit,
    race_number
FROM weather_wind_speed
WHERE wind_speed_mph IS NOT NULL

