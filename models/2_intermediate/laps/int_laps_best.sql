{{
    config(
        materialized='table'
    )
}}

-- Best lap aggregates per vehicle for Barber Race 1
WITH laps_best AS (
    -- Extract top lap performance metrics for Barber Race 1
    SELECT
        number AS vehicle_number,
        vehicle,
        class,
        total_driver_laps,
        bestlap_1 AS bestlap_1_elapsed_time,
        bestlap_1_lapnum,
        bestlap_2 AS bestlap_2_elapsed_time,
        bestlap_2_lapnum,
        bestlap_3 AS bestlap_3_elapsed_time,
        bestlap_3_lapnum,
        bestlap_4 AS bestlap_4_elapsed_time,
        bestlap_4_lapnum,
        bestlap_5 AS bestlap_5_elapsed_time,
        bestlap_5_lapnum,
        bestlap_6 AS bestlap_6_elapsed_time,
        bestlap_6_lapnum,
        bestlap_7 AS bestlap_7_elapsed_time,
        bestlap_7_lapnum,
        bestlap_8 AS bestlap_8_elapsed_time,
        bestlap_8_lapnum,
        bestlap_9 AS bestlap_9_elapsed_time,
        bestlap_9_lapnum,
        bestlap_10 AS bestlap_10_elapsed_time,
        bestlap_10_lapnum,
        average AS average_best_lap,
        'barber' AS circuit,
        1 AS race_number
    FROM {{ ref('raw_barber_race_1_best_laps') }}
)

SELECT
    vehicle_number,
    vehicle,
    class,
    total_driver_laps,
    bestlap_1_elapsed_time,
    bestlap_1_lapnum,
    bestlap_2_elapsed_time,
    bestlap_2_lapnum,
    bestlap_3_elapsed_time,
    bestlap_3_lapnum,
    bestlap_4_elapsed_time,
    bestlap_4_lapnum,
    bestlap_5_elapsed_time,
    bestlap_5_lapnum,
    bestlap_6_elapsed_time,
    bestlap_6_lapnum,
    bestlap_7_elapsed_time,
    bestlap_7_lapnum,
    bestlap_8_elapsed_time,
    bestlap_8_lapnum,
    bestlap_9_elapsed_time,
    bestlap_9_lapnum,
    bestlap_10_elapsed_time,
    bestlap_10_lapnum,
    average_best_lap,
    circuit,
    race_number
FROM laps_best
WHERE vehicle_number IS NOT NULL

