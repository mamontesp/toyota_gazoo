{{
    config(
        materialized='table'
    )
}}

SELECT
    fact.circuit,
    fact.race_number,
    fact.vehicle_id,
    fact.vehicle_number,
    fact.outing,
    fact.lap,
    fact.event_timestamp,
    fact.laptrigger_lapdist_dls,
    sector.meter_position_m,
    fact.event_source,
    fact.speed,
    fact.gear,
    fact.aps,
    fact.steering_angle,
    fact.front_brake_pressure,
    fact.rear_brake_pressure,
    sector.sector_number,
    sector.sector_name,
    sector.sector_length_m
FROM  {{ref('fact_telemetry_data')}} AS fact
LEFT JOIN  {{ref('dim_track_sectors')}} AS sector
    ON FLOOR(fact.laptrigger_lapdist_dls) = sector.meter_position_m
WHERE fact.lap IS NOT NULL
    AND fact.laptrigger_lapdist_dls IS NOT NULL