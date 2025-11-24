{{
    config(
        materialized='view'
    )
}}

-- Raw weather conditions from Barber Race 1
-- Source: 26_Weather_Race 1_Anonymized.CSV
SELECT
    time_utc_seconds,
    time_utc_str,
    air_temp,
    track_temp,
    humidity,
    pressure,
    wind_speed,
    wind_direction,
    rain
FROM {{ source('barber', 'barber_race_1_weather') }}

