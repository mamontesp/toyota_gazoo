{{
    config(
        materialized='view'
    )
}}

-- Raw provisional results by class from Barber Race 1
-- Source: 05_Provisional Results by Class_Race 1_Anonymized.CSV
SELECT
    class_type,
    pos,
    pic,
    number,
    vehicle,
    laps,
    elapsed,
    gap_first,
    gap_previous,
    best_lap_num,
    best_lap_time,
    best_lap_kph
FROM {{ source('barber', 'barber_race_1_results_by_class') }}

