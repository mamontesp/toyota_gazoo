{{
    config(
        materialized='view'
    )
}}

-- Raw provisional results from Barber Race 1
-- Source: 03_Provisional Results_Race 1_Anonymized.CSV
SELECT
    position,
    number,
    status,
    laps,
    total_time,
    gap_first,
    gap_previous,
    fl_lapnum,
    fl_time,
    fl_kph,
    class,
    "group",
    division,
    vehicle,
    tires,
    "ECM Participant Id" AS ecm_participant_id,
    "ECM Team Id" AS ecm_team_id,
    "ECM Category Id" AS ecm_category_id,
    "ECM Car Id" AS ecm_car_id,
    "ECM Brand Id" AS ecm_brand_id,
    "ECM Country Id" AS ecm_country_id,
    "*Extra 7" AS extra_7,
    "*Extra 8" AS extra_8,
    "*Extra 9" AS extra_9,
    "Sort Key" AS sort_key,
    "DRIVER_*Extra 3" AS driver_extra_3,
    "DRIVER_*Extra 4" AS driver_extra_4,
    "DRIVER_*Extra 5" AS driver_extra_5
FROM {{ source('barber', 'barber_race_1_provisional_results') }}

