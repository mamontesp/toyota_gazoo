{{
    config(
        materialized='view'
    )
}}

-- Raw best 10 laps by driver from Barber Race 1
-- Source: 99_Best 10 Laps By Driver_Race 1_Anonymized.CSV
SELECT
    number,
    vehicle,
    class,
    total_driver_laps,
    bestlap_1,
    bestlap_1_lapnum,
    bestlap_2,
    bestlap_2_lapnum,
    bestlap_3,
    bestlap_3_lapnum,
    bestlap_4,
    bestlap_4_lapnum,
    bestlap_5,
    bestlap_5_lapnum,
    bestlap_6,
    bestlap_6_lapnum,
    bestlap_7,
    bestlap_7_lapnum,
    bestlap_8,
    bestlap_8_lapnum,
    bestlap_9,
    bestlap_9_lapnum,
    bestlap_10,
    bestlap_10_lapnum,
    average
FROM {{ source('barber', 'barber_race_1_best_laps') }}

