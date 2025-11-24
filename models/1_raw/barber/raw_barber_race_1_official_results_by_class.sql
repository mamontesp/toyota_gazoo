{{
    config(
        materialized='view'
    )
}}

-- Raw official results by class from Barber Race 1 (GR Cup)
-- Source: 05_Results by Class GR Cup Race 1 Official_Anonymized.CSV
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
FROM {{ source('barber', 'barber_race_1_official_results_by_class') }}

