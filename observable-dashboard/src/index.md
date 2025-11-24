# Toyota Gazoo Racing Analytics

Welcome to the Toyota Gazoo Racing Analytics Dashboard! This dashboard provides insights into telemetry data and race performance across multiple tracks.

## Quick Stats

```js
// Import DuckDB client
import {DuckDBClient} from "npm:@observablehq/duckdb";

// Connect to DuckDB database using FileAttachment
const db = await DuckDBClient.of({
  database: FileAttachment("dev.duckdb")
});

// Query to get total races using sql method
const races = await db.sql`
  SELECT 
    COUNT(DISTINCT race_number) as total_races,
    COUNT(DISTINCT vehicle_id) as total_vehicles,
    COUNT(DISTINCT circuit) as total_circuits
  FROM database.main_marts.fact_telemetry_data
  WHERE race_number IS NOT NULL
`;

// Convert to array and get first row
const racesArray = Array.from(races);
const stats = racesArray[0] || { total_races: 0, total_vehicles: 0, total_circuits: 0 };
```

<div class="grid grid-cols-3">
  <div class="card">
    <h2>${stats.total_races}</h2>
    <p>Total Races</p>
  </div>
  <div class="card">
    <h2>${stats.total_vehicles}</h2>
    <p>Total Vehicles</p>
  </div>
  <div class="card">
    <h2>${stats.total_circuits}</h2>
    <p>Total Circuits</p>
  </div>
</div>

## Recent Race Data

```js
// Get recent race summary
const recentRacesResult = await db.sql`
  SELECT 
    circuit,
    race_number,
    COUNT(DISTINCT vehicle_id) as vehicle_count,
    COUNT(DISTINCT lap) as total_laps,
    AVG(speed) as avg_speed
  FROM database.main_marts.fact_telemetry_data
  WHERE race_number IS NOT NULL AND speed IS NOT NULL
  GROUP BY circuit, race_number
  ORDER BY race_number DESC
  LIMIT 10
`;

const recentRaces = Array.from(recentRacesResult);
```

```js
display(Inputs.table(recentRaces, {
  columns: [
    "circuit",
    "race_number", 
    "vehicle_count",
    "total_laps",
    "avg_speed"
  ],
  header: {
    circuit: "Circuit",
    race_number: "Race",
    vehicle_count: "Vehicles",
    total_laps: "Laps",
    avg_speed: "Avg Speed"
  },
  format: {
    avg_speed: d => d.toFixed(2)
  }
}))
```

## Navigation

- [Telemetry Analysis](/telemetry) - Detailed telemetry data visualization
- [Driver Consistency Histograms](/driver-consistency) - Histogram view of telemetry metrics across track sectors
- [Race Analysis](/race-analysis) - Race performance metrics and comparisons
- [Lap Insights](/lap-insights) - Lap durations with weather context
- [Wind Conditions](/fact-wind) - Wind speed and direction snapshots by race
- [Track Map](/track-map) - Interactive racing lines and track overlays

