-- Audit: Identify drivers with consistent lap maneuvers based on telemetry signature alignment
-- This analysis compares per-lap event sequences against each driver's modal behavior at each lap position

WITH
-- Base telemetry events scoped to laps with lap distance metadata
base_events AS (
    SELECT
        circuit,
        race_number,
        vehicle_id,
        vehicle_number,
        outing,
        lap,
        event_timestamp,
        laptrigger_lapdist_dls,
        event_source,
        speed,
        gear,
        aps,
        steering_angle,
        front_brake_pressure,
        rear_brake_pressure
    FROM dev.main_marts.fact_telemetry_data
    WHERE lap IS NOT NULL
      AND laptrigger_lapdist_dls IS NOT NULL
),
-- Rank events within each driver lap to compare ordered maneuvers
ranked_events AS (
    SELECT
        circuit,
        race_number,
        vehicle_id,
        vehicle_number,
        outing,
        lap,
        event_timestamp,
        laptrigger_lapdist_dls,
        event_source,
        speed,
        gear,
        aps,
        steering_angle,
        front_brake_pressure,
        rear_brake_pressure,
        ROW_NUMBER() OVER (
            PARTITION BY circuit, race_number, vehicle_id, outing, lap
            ORDER BY laptrigger_lapdist_dls, event_timestamp, event_source
        ) AS event_order,
        COUNT(*) OVER (
            PARTITION BY circuit, race_number, vehicle_id, outing, lap
        ) AS events_per_lap
    FROM base_events
),
-- Count event sources per driver, circuit, and sequence step
driver_event_source_counts AS (
    SELECT
        circuit,
        vehicle_id,
        vehicle_number,
        event_order,
        event_source,
        COUNT(*) AS event_instances,
        COUNT(DISTINCT lap) AS laps_with_event
    FROM ranked_events
    GROUP BY 1, 2, 3, 4, 5
),
-- Capture the modal event source per driver at each sequence step
driver_event_signature AS (
    SELECT
        circuit,
        vehicle_id,
        vehicle_number,
        event_order,
        event_source AS signature_event_source,
        event_instances,
        laps_with_event,
        event_instances / NULLIF(SUM(event_instances) OVER (
            PARTITION BY circuit, vehicle_id, event_order
        ), 0) AS signature_event_share,
        ROW_NUMBER() OVER (
            PARTITION BY circuit, vehicle_id, event_order
            ORDER BY event_instances DESC, event_source
        ) AS signature_rank
    FROM driver_event_source_counts
    QUALIFY signature_rank = 1
),
-- Summarize telemetry metrics per driver sequence step
driver_order_metrics AS (
    SELECT
        circuit,
        vehicle_id,
        event_order,
        AVG(laptrigger_lapdist_dls) AS avg_laptrigger_lapdist_dls,
        STDDEV_POP(laptrigger_lapdist_dls) AS laptrigger_variability,
        AVG(speed) AS avg_speed,
        STDDEV_POP(speed) AS speed_variability,
        AVG(gear) AS avg_gear,
        STDDEV_POP(gear) AS gear_variability,
        AVG(aps) AS avg_aps,
        STDDEV_POP(aps) AS aps_variability,
        AVG(steering_angle) AS avg_steering_angle,
        STDDEV_POP(steering_angle) AS steering_variability,
        AVG(front_brake_pressure) AS avg_front_brake_pressure,
        STDDEV_POP(front_brake_pressure) AS front_brake_variability,
        AVG(rear_brake_pressure) AS avg_rear_brake_pressure,
        STDDEV_POP(rear_brake_pressure) AS rear_brake_variability,
        COUNT(DISTINCT lap) AS laps_covered
    FROM ranked_events
    GROUP BY 1, 2, 3
),
-- Compare each lap event with the driver's signature expectations
lap_event_alignment AS (
    SELECT
        r.circuit,
        r.race_number,
        r.vehicle_id,
        r.vehicle_number,
        r.outing,
        r.lap,
        r.event_order,
        r.event_source,
        r.laptrigger_lapdist_dls,
        r.speed,
        r.gear,
        r.aps,
        r.steering_angle,
        r.front_brake_pressure,
        r.rear_brake_pressure,
        r.events_per_lap,
        sig.signature_event_source,
        sig.signature_event_share,
        metrics.avg_laptrigger_lapdist_dls,
        metrics.laptrigger_variability,
        CASE WHEN r.event_source = sig.signature_event_source THEN 1 ELSE 0 END AS event_match_flag,
        ABS(r.laptrigger_lapdist_dls - metrics.avg_laptrigger_lapdist_dls) AS laptrigger_delta,
        ABS(r.speed - metrics.avg_speed) AS speed_delta,
        ABS(r.gear - metrics.avg_gear) AS gear_delta,
        ABS(r.aps - metrics.avg_aps) AS aps_delta,
        ABS(r.steering_angle - metrics.avg_steering_angle) AS steering_delta,
        ABS(r.front_brake_pressure - metrics.avg_front_brake_pressure) AS front_brake_delta,
        ABS(r.rear_brake_pressure - metrics.avg_rear_brake_pressure) AS rear_brake_delta
    FROM ranked_events AS r
    INNER JOIN driver_event_signature AS sig
        ON r.circuit = sig.circuit
       AND r.vehicle_id = sig.vehicle_id
       AND r.event_order = sig.event_order
    INNER JOIN driver_order_metrics AS metrics
        ON r.circuit = metrics.circuit
       AND r.vehicle_id = metrics.vehicle_id
       AND r.event_order = metrics.event_order
),
-- Aggregate lap-level consistency metrics
lap_consistency AS (
    SELECT
        circuit,
        vehicle_id,
        vehicle_number,
        outing,
        lap,
        COUNT(*) AS evaluated_events,
        SUM(event_match_flag) AS matched_events,
        AVG(event_match_flag) AS alignment_ratio,
        AVG(laptrigger_delta) AS avg_laptrigger_delta,
        AVG(speed_delta) AS avg_speed_delta,
        AVG(gear_delta) AS avg_gear_delta,
        AVG(aps_delta) AS avg_aps_delta,
        AVG(steering_delta) AS avg_steering_delta,
        AVG(front_brake_delta) AS avg_front_brake_delta,
        AVG(rear_brake_delta) AS avg_rear_brake_delta
    FROM lap_event_alignment
    GROUP BY 1, 2, 3, 4, 5
),
-- Summarize driver consistency across laps
driver_consistency AS (
    SELECT
        circuit,
        vehicle_id,
        vehicle_number,
        COUNT(DISTINCT lap) AS laps_analyzed,
        AVG(alignment_ratio) AS avg_alignment_ratio,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY alignment_ratio) AS median_alignment_ratio,
        PERCENTILE_CONT(0.1) WITHIN GROUP (ORDER BY alignment_ratio) AS p10_alignment_ratio,
        PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY alignment_ratio) AS p90_alignment_ratio,
        AVG(avg_laptrigger_delta) AS avg_laptrigger_delta,
        AVG(avg_speed_delta) AS avg_speed_delta,
        AVG(avg_gear_delta) AS avg_gear_delta,
        AVG(avg_aps_delta) AS avg_aps_delta,
        AVG(avg_steering_delta) AS avg_steering_delta,
        AVG(avg_front_brake_delta) AS avg_front_brake_delta,
        AVG(avg_rear_brake_delta) AS avg_rear_brake_delta
    FROM lap_consistency
    GROUP BY 1, 2, 3
),
-- Quantify maneuver variability per 1-meter track section
meter_maneuver_change AS (
    SELECT
        circuit,
        vehicle_id,
        vehicle_number,
        FLOOR(laptrigger_lapdist_dls) AS meter_position_m,
        COUNT(*) AS evaluated_events,
        COUNT(DISTINCT outing) AS outings_covered,
        COUNT(DISTINCT lap) AS laps_covered,
        AVG(event_match_flag) AS meter_alignment_ratio,
        1 - AVG(event_match_flag) AS meter_change_ratio,
        AVG(signature_event_share) AS avg_signature_event_share,
        AVG(speed_delta) AS avg_speed_delta,
        AVG(gear_delta) AS avg_gear_delta,
        AVG(aps_delta) AS avg_aps_delta,
        AVG(steering_delta) AS avg_steering_delta,
        AVG(front_brake_delta) AS avg_front_brake_delta,
        AVG(rear_brake_delta) AS avg_rear_brake_delta,
        STDDEV_POP(speed) AS speed_stddev,
        STDDEV_POP(gear) AS gear_stddev,
        STDDEV_POP(aps) AS aps_stddev,
        STDDEV_POP(steering_angle) AS steering_stddev,
        STDDEV_POP(front_brake_pressure) AS front_brake_stddev,
        STDDEV_POP(rear_brake_pressure) AS rear_brake_stddev
    FROM lap_event_alignment
    GROUP BY 1, 2, 3, 4
)
SELECT
    meter.circuit,
    meter.vehicle_number,
    meter.vehicle_id,
    meter.meter_position_m,
    meter.laps_covered,
    meter.outings_covered,
    meter.evaluated_events,
    driver.laps_analyzed,
    driver.avg_alignment_ratio,
    meter.meter_alignment_ratio,
    meter.meter_change_ratio,
    meter.avg_signature_event_share,
    meter.avg_speed_delta,
    meter.avg_gear_delta,
    meter.avg_aps_delta,
    meter.avg_steering_delta,
    meter.avg_front_brake_delta,
    meter.avg_rear_brake_delta,
    meter.speed_stddev,
    meter.gear_stddev,
    meter.aps_stddev,
    meter.steering_stddev,
    meter.front_brake_stddev,
    meter.rear_brake_stddev
FROM meter_maneuver_change AS meter
LEFT JOIN driver_consistency AS driver
    ON meter.circuit = driver.circuit
   AND meter.vehicle_id = driver.vehicle_id
WHERE meter.laps_covered >= 3
ORDER BY meter.meter_change_ratio DESC, meter.meter_position_m ASC, meter.vehicle_number
LIMIT 200;

-- Example: Add WHERE filters near the end to focus on one circuit or outing
-- e.g., append "AND meter.circuit = 'Le Mans'" before the ORDER BY clause to isolate that track
