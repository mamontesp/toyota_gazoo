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

// Query telemetry metrics once per filter combo and cache the result
const cacheKey = JSON.stringify({
  circuit: selectedCircuit,
  vehicle: selectedVehicle,
  lap: selectedLap
});

const telemetryCache = globalThis.telemetryCache ?? (globalThis.telemetryCache = new Map());
let telemetryData = telemetryCache.get(cacheKey);

if (!telemetryData) {
  const telemetryDataResult = await db.query(`
    SELECT 
      lap,
      speed,
      nmot,
      aps,
      front_brake_pressure,
      rear_brake_pressure
    FROM database.main_marts.fact_telemetry_data
    WHERE ${whereClause}
      AND lap IS NOT NULL
    ORDER BY lap
  `);

  telemetryData = Array.from(telemetryDataResult, d => ({
    ...d,
    lap_number: Number(d.lap)
  }));

  telemetryCache.set(cacheKey, telemetryData);
}

const speedData = telemetryData.filter(d => d.speed !== null && d.speed !== undefined);
const rpmData = telemetryData.filter(d => d.nmot !== null && d.nmot !== undefined);
const throttleData = telemetryData.filter(d => d.aps !== null && d.aps !== undefined);
const brakeBoxData = telemetryData.flatMap(d => {
  const entries = [];
  if (d.front_brake_pressure !== null && d.front_brake_pressure !== undefined) {
    entries.push({lap: d.lap, lap_number: d.lap_number, metric: "Front Brake", value: d.front_brake_pressure});
  }
  if (d.rear_brake_pressure !== null && d.rear_brake_pressure !== undefined) {
    entries.push({lap: d.lap, lap_number: d.lap_number, metric: "Rear Brake", value: d.rear_brake_pressure});
  }
  return entries;
});
```

```js
Plot.plot({
  title: "Speed Distribution by Lap",
  width: 1000,
  height: 400,
  x: {label: "Lap Number"},
  y: {label: "Speed", grid: true},
  marks: [
    Plot.boxY(speedData, {
      x: "lap_number",
      y: "speed",
      stroke: "steelblue",
      fill: "steelblue",
      fillOpacity: 0.2,
      strokeWidth: 1.5
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
  x: {label: "Lap Number"},
  y: {label: "RPM (NMOT)", grid: true},
  marks: [
    Plot.boxY(rpmData, {
      x: "lap_number",
      y: "nmot",
      stroke: "purple",
      fill: "purple",
      fillOpacity: 0.2,
      strokeWidth: 1.5
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
  x: {label: "Lap Number"},
  y: {label: "Throttle Position (APS)", grid: true},
  marks: [
    Plot.boxY(throttleData, {
      x: "lap_number",
      y: "aps",
      stroke: "green",
      fill: "green",
      fillOpacity: 0.2,
      strokeWidth: 1.5
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
  x: {label: "Lap Number"},
  y: {label: "Pressure", grid: true},
  color: {legend: true, domain: ["Front Brake", "Rear Brake"], range: ["red", "darkred"]},
  marks: [
    Plot.boxY(brakeBoxData, {
      x: "lap_number",
      y: "value",
      stroke: "metric",
      fill: "metric",
      fillOpacity: 0.2,
      strokeWidth: 1.5
    }),
    Plot.ruleY([0])
  ]
})
```
