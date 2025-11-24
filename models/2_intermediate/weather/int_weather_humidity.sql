{{
    config(
        materialized='table'
    )
}}

-- Humidity observations captured during Barber Race 1
WITH weather_humidity AS (
    -- Extract the raw humidity readings for Barber Race 1
    SELECT
        time_utc_str,
        time_utc_seconds,
        humidity AS humidity_percentage,
        'barber' AS circuit,
        1 AS race_number
    FROM {{ ref('raw_barber_race_1_weather') }}
)

SELECT
    time_utc_str AS time_utc,
    time_utc_seconds,
    humidity_percentage,
    circuit,
    race_number
FROM weather_humidity
WHERE humidity_percentage IS NOT NULL

