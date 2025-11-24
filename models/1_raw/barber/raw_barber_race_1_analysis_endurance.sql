{{
    config(
        materialized='view'
    )
}}

-- Raw endurance analysis with sector data from Barber Race 1
-- Source: 23_AnalysisEnduranceWithSections_Race 1_Anonymized.CSV
SELECT
    number,
    driver_number,
    lap_number,
    lap_time,
    lap_improvement,
    crossing_finish_line_in_pit,
    s1,
    s1_improvement,
    s2,
    s2_improvement,
    s3,
    s3_improvement,
    kph,
    elapsed,
    hour,
    s1_large,
    s2_large,
    s3_large,
    top_speed,
    pit_time,
    class,
    "group",
    manufacturer,
    flag_at_fl,
    s1_seconds,
    s2_seconds,
    s3_seconds,
    im1a_time,
    im1a_elapsed,
    im1_time,
    im1_elapsed,
    im2a_time,
    im2a_elapsed,
    im2_time,
    im2_elapsed,
    im3a_time,
    im3a_elapsed,
    fl_time,
    fl_elapsed
FROM {{ source('barber', 'barber_race_1_analysis_endurance') }}

