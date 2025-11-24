# Telemetry Analysis

Interactive telemetry data analysis for Toyota Gazoo Racing.

## Filter Options

```js
// Import DuckDB client
import {DuckDBClient} from "npm:@observablehq/duckdb";

// Connect to DuckDB database using FileAttachment
const db = await DuckDBClient.of({
  database: FileAttachment("dev.duckdb")
});

// Get available circuits
const circuitsResult = await db.sql`
  SELECT DISTINCT circuit 
  FROM database.main_marts.fact_telemetry_data 
  WHERE circuit IS NOT NULL
  ORDER BY circuit
`;

const circuits = Array.from(circuitsResult);
const circuitOptions = ["All", ...circuits.map(d => d.circuit)];
const selectedCircuit = view(Inputs.select(circuitOptions, {label: "Circuit", value: "All"}));
```

```js
// Get available vehicles for selected circuit
const vehiclesQuery = selectedCircuit === "All" 
  ? `SELECT DISTINCT vehicle_id FROM database.main_marts.fact_telemetry_data WHERE vehicle_id IS NOT NULL ORDER BY vehicle_id`
  : `SELECT DISTINCT vehicle_id FROM database.main_marts.fact_telemetry_data WHERE circuit = '${selectedCircuit}' AND vehicle_id IS NOT NULL ORDER BY vehicle_id`;

const vehiclesResult = await db.query(vehiclesQuery);
const vehicles = Array.from(vehiclesResult);

const vehicleOptions = ["All", ...vehicles.map(d => d.vehicle_id)];
const selectedVehicle = view(Inputs.select(vehicleOptions, {label: "Vehicle", value: "All"}));
```

```js
// Get available laps for selected circuit and vehicle
const lapsQuery = [
  "SELECT DISTINCT lap FROM database.main_marts.fact_telemetry_data WHERE lap IS NOT NULL",
  selectedCircuit !== "All" ? `circuit = '${selectedCircuit}'` : null,
  selectedVehicle !== "All" ? `vehicle_id = '${selectedVehicle}'` : null
].filter(Boolean).join(" AND ") + " ORDER BY lap";

const lapsResult = await db.query(lapsQuery);
const laps = Array.from(lapsResult);

const lapOptions = ["All", ...laps.map(d => d.lap)];
const selectedLap = view(Inputs.select(lapOptions, {label: "Lap", value: "All"}));
```

## Speed Analysis

```js
// Build where clause
const whereClause = [
  selectedCircuit !== "All" ? `circuit = '${selectedCircuit}'` : "1=1",
  selectedVehicle !== "All" ? `vehicle_id = '${selectedVehicle}'` : "1=1",
  selectedLap !== "All" ? `lap = ${selectedLap}` : "1=1"
].join(" AND ");

// Query telemetry metrics aggregated by lap for box plots (much faster than loading all raw data)
const cacheKey = JSON.stringify({
  circuit: selectedCircuit,
  vehicle: selectedVehicle,
  lap: selectedLap
});

const telemetryCache = globalThis.telemetryCache ?? (globalThis.telemetryCache = new Map());
let telemetryData = telemetryCache.get(cacheKey);

if (!telemetryData) {
  // Aggregate data at database level - compute box plot statistics per lap
  // This reduces data transfer from millions of rows to hundreds (one per lap)
  // PERCENTILE_CONT automatically ignores NULL values
  const telemetryDataResult = await db.query(`
    SELECT 
      lap,
      -- Speed statistics
      MIN(speed) AS speed_min,
      PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY speed) AS speed_q1,
      PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY speed) AS speed_median,
      PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY speed) AS speed_q3,
      MAX(speed) AS speed_max,
      -- RPM (NMOT) statistics
      MIN(nmot) AS nmot_min,
      PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY nmot) AS nmot_q1,
      PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY nmot) AS nmot_median,
      PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY nmot) AS nmot_q3,
      MAX(nmot) AS nmot_max,
      -- Throttle (APS) statistics
      MIN(aps) AS aps_min,
      PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY aps) AS aps_q1,
      PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY aps) AS aps_median,
      PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY aps) AS aps_q3,
      MAX(aps) AS aps_max,
      -- Front brake pressure statistics
      MIN(front_brake_pressure) AS front_brake_min,
      PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY front_brake_pressure) AS front_brake_q1,
      PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY front_brake_pressure) AS front_brake_median,
      PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY front_brake_pressure) AS front_brake_q3,
      MAX(front_brake_pressure) AS front_brake_max,
      -- Rear brake pressure statistics
      MIN(rear_brake_pressure) AS rear_brake_min,
      PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY rear_brake_pressure) AS rear_brake_q1,
      PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY rear_brake_pressure) AS rear_brake_median,
      PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY rear_brake_pressure) AS rear_brake_q3,
      MAX(rear_brake_pressure) AS rear_brake_max
    FROM database.main_marts.fact_telemetry_data
    WHERE ${whereClause}
      AND lap IS NOT NULL
    GROUP BY lap
    ORDER BY lap
  `);

  telemetryData = Array.from(telemetryDataResult, d => ({
    ...d,
    lap_number: Number(d.lap)
  }));

  telemetryCache.set(cacheKey, telemetryData);
}

// Transform aggregated data for box plots
const speedData = telemetryData
  .filter(d => d.speed_median !== null)
  .map(d => ({
    lap_number: d.lap_number,
    speed: [d.speed_min, d.speed_q1, d.speed_median, d.speed_q3, d.speed_max]
  }));

const rpmData = telemetryData
  .filter(d => d.nmot_median !== null)
  .map(d => ({
    lap_number: d.lap_number,
    nmot: [d.nmot_min, d.nmot_q1, d.nmot_median, d.nmot_q3, d.nmot_max]
  }));

const throttleData = telemetryData
  .filter(d => d.aps_median !== null)
  .map(d => ({
    lap_number: d.lap_number,
    aps: [d.aps_min, d.aps_q1, d.aps_median, d.aps_q3, d.aps_max]
  }));

const brakeBoxData = telemetryData.flatMap(d => {
  const entries = [];
  if (d.front_brake_median !== null) {
    entries.push({
      lap_number: d.lap_number,
      metric: "Front Brake",
      value: [d.front_brake_min, d.front_brake_q1, d.front_brake_median, d.front_brake_q3, d.front_brake_max]
    });
  }
  if (d.rear_brake_median !== null) {
    entries.push({
      lap_number: d.lap_number,
      metric: "Rear Brake",
      value: [d.rear_brake_min, d.rear_brake_q1, d.rear_brake_median, d.rear_brake_q3, d.rear_brake_max]
    });
  }
  return entries;
});
```

```js
Plot.plot({
  title: "Speed Distribution by Lap",
  width: 1000,
  height: 400,
  x: {label: "Lap Number", type: "band"},
  y: {label: "Speed", grid: true},
  marks: [
    // Box plot using pre-computed quartiles
    Plot.rectY(speedData, {
      x: "lap_number",
      y1: d => d.speed[0], // min
      y2: d => d.speed[4], // max
      stroke: "steelblue",
      fill: "none",
      strokeWidth: 1
    }),
    Plot.rectY(speedData, {
      x: "lap_number",
      y1: d => d.speed[1], // q1
      y2: d => d.speed[3], // q3
      fill: "steelblue",
      fillOpacity: 0.3,
      stroke: "steelblue",
      strokeWidth: 1.5
    }),
    Plot.ruleY(speedData, {
      x: "lap_number",
      y: d => d.speed[2], // median
      stroke: "steelblue",
      strokeWidth: 2
    }),
    Plot.ruleY([0])
  ]
})
```

## RPM Analysis (NMOT)

```js
// rpmData derived from telemetryData (see Speed Analysis block)
```

```js
Plot.plot({
  title: "RPM (NMOT) Distribution by Lap",
  width: 1000,
  height: 400,
  x: {label: "Lap Number", type: "band"},
  y: {label: "RPM (NMOT)", grid: true},
  marks: [
    // Box plot using pre-computed quartiles
    Plot.rectY(rpmData, {
      x: "lap_number",
      y1: d => d.nmot[0], // min
      y2: d => d.nmot[4], // max
      stroke: "purple",
      fill: "none",
      strokeWidth: 1
    }),
    Plot.rectY(rpmData, {
      x: "lap_number",
      y1: d => d.nmot[1], // q1
      y2: d => d.nmot[3], // q3
      fill: "purple",
      fillOpacity: 0.3,
      stroke: "purple",
      strokeWidth: 1.5
    }),
    Plot.ruleY(rpmData, {
      x: "lap_number",
      y: d => d.nmot[2], // median
      stroke: "purple",
      strokeWidth: 2
    }),
    Plot.ruleY([0])
  ]
})
```

## Throttle and Brake Analysis

```js
// throttleData and brakeBoxData derived from telemetryData (see Speed Analysis block)
```

```js
Plot.plot({
  title: "Throttle Position (APS) Distribution",
  width: 1000,
  height: 400,
  x: {label: "Lap Number", type: "band"},
  y: {label: "Throttle Position (APS)", grid: true},
  marks: [
    // Box plot using pre-computed quartiles
    Plot.rectY(throttleData, {
      x: "lap_number",
      y1: d => d.aps[0], // min
      y2: d => d.aps[4], // max
      stroke: "green",
      fill: "none",
      strokeWidth: 1
    }),
    Plot.rectY(throttleData, {
      x: "lap_number",
      y1: d => d.aps[1], // q1
      y2: d => d.aps[3], // q3
      fill: "green",
      fillOpacity: 0.3,
      stroke: "green",
      strokeWidth: 1.5
    }),
    Plot.ruleY(throttleData, {
      x: "lap_number",
      y: d => d.aps[2], // median
      stroke: "green",
      strokeWidth: 2
    }),
    Plot.ruleY([0])
  ]
})
```

```js
Plot.plot({
  title: "Brake Pressure Distribution",
  width: 1000,
  height: 400,
  x: {label: "Lap Number", type: "band"},
  y: {label: "Pressure", grid: true},
  color: {legend: true, domain: ["Front Brake", "Rear Brake"], range: ["red", "darkred"]},
  marks: [
    // Box plots using pre-computed quartiles
    Plot.rectY(brakeBoxData, {
      x: "lap_number",
      y1: d => d.value[0], // min
      y2: d => d.value[4], // max
      stroke: d => d.metric === "Front Brake" ? "red" : "darkred",
      fill: "none",
      strokeWidth: 1
    }),
    Plot.rectY(brakeBoxData, {
      x: "lap_number",
      y1: d => d.value[1], // q1
      y2: d => d.value[3], // q3
      fill: d => d.metric === "Front Brake" ? "red" : "darkred",
      fillOpacity: 0.3,
      stroke: d => d.metric === "Front Brake" ? "red" : "darkred",
      strokeWidth: 1.5
    }),
    Plot.ruleY(brakeBoxData, {
      x: "lap_number",
      y: d => d.value[2], // median
      stroke: d => d.metric === "Front Brake" ? "red" : "darkred",
      strokeWidth: 2
    }),
    Plot.ruleY([0])
  ]
})
```
