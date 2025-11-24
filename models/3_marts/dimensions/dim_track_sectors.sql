{{ config(materialized='table') }}

WITH
    -- Define sector lengths in inches and their display names
    sector_lengths AS (
        SELECT *
        FROM VALUES
            (1, 'Sector 1', 40512),
            (2, 'Sector 2', 62220),
            (3, 'Sector 3', 41940)
            AS s(sector_number, sector_name, length_inches)
    ),
    -- Convert sector lengths from inches to meters
    sector_lengths_meters AS (
        SELECT
            sector_number,
            sector_name,
            length_inches,
            CAST(length_inches * 0.0254 AS DECIMAL(18,4)) AS length_meters
        FROM sector_lengths
    ),
    -- Compute cumulative distances across sectors
    sector_metrics AS (
        SELECT
            sector_number,
            sector_name,
            length_inches,
            length_meters,
            SUM(length_meters)
                OVER (ORDER BY sector_number ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_length_meters
        FROM sector_lengths_meters
    ),
    -- Summarize overall circuit distance in meters
    sector_summary AS (
        SELECT
            MAX(cumulative_length_meters) AS total_length_meters
        FROM sector_metrics
    ),
    -- Determine start and end meter boundaries for each sector
    sector_ranges AS (
        SELECT
            sector_number,
            sector_name,
            cumulative_length_meters - length_meters AS sector_start_distance_m,
            cumulative_length_meters AS sector_end_distance_m
        FROM sector_metrics
    ),
    -- Generate a meter-by-meter spine across the circuit
    meter_spine AS (
        SELECT
            gs.value AS meter_position_m
        FROM sector_summary
        CROSS JOIN GENERATE_SERIES(
            0,
            CAST(FLOOR(total_length_meters) AS INTEGER),
            1
        ) AS gs(value)
    )

SELECT
    meter_spine.meter_position_m,
    sector_ranges.sector_number,
    sector_ranges.sector_name,
    sector_ranges.sector_start_distance_m,
    sector_ranges.sector_end_distance_m,
    sector_ranges.sector_end_distance_m - sector_ranges.sector_start_distance_m AS sector_length_m
FROM meter_spine
INNER JOIN sector_ranges
    ON meter_spine.meter_position_m >= sector_ranges.sector_start_distance_m
   AND meter_spine.meter_position_m < sector_ranges.sector_end_distance_m
ORDER BY
    meter_spine.meter_position_m

