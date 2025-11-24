{{
    config(
        materialized='table'
    )
}}

-- Atmospheric pressure observations captured during Barber Race 1
WITH weather_pressure AS (
    -- Extract the raw pressure readings for Barber Race 1
    SELECT
        time_utc_str,
        time_utc_seconds,
        pressure AS pressure_inches,
        pressure * 36.86 AS pressure_mbars,
        'barber' AS circuit,
        1 AS race_number
    FROM {{ ref('raw_barber_race_1_weather') }}
)

SELECT
    time_utc_str AS time_utc,
    time_utc_seconds,
    pressure_inches,
    pressure_mbars,
    circuit,
    race_number
FROM weather_pressure
WHERE pressure_inches IS NOT NULL

