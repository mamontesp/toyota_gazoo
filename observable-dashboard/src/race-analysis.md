# Race Analysis

Comprehensive race performance analysis and vehicle comparisons.

## Circuit Selection

```js
// Import DuckDB client
import {DuckDBClient} from "npm:@observablehq/duckdb";

// Connect to DuckDB database using FileAttachment
const db = await DuckDBClient.of({
  database: FileAttachment("dev.duckdb")
});

const circuitsResult = await db.sql`
  SELECT DISTINCT circuit 
  FROM database.main_marts.fact_telemetry_data 
  WHERE circuit IS NOT NULL
  ORDER BY circuit
`;

const circuits = Array.from(circuitsResult);

const selectedCircuit = view(Inputs.select(circuits.map(d => d.circuit), {
  label: "Select Circuit",
  value: circuits[0]?.circuit
}));
```

## Vehicle Performance Comparison

```js
// Get performance metrics by vehicle for selected circuit
const vehiclePerformanceResult = await db.sql`
  SELECT 
    vehicle_id,
    vehicle_number,
    COUNT(DISTINCT lap) as total_laps,
    AVG(speed) as avg_speed,
    AVG(nmot) as avg_rpm,
    AVG(aps) as avg_throttle,
    AVG(gear) as avg_gear
  FROM database.main_marts.fact_telemetry_data
  WHERE circuit = ${selectedCircuit}
    AND vehicle_id IS NOT NULL
  GROUP BY vehicle_id, vehicle_number
  ORDER BY avg_speed DESC
`;

const vehiclePerformance = Array.from(vehiclePerformanceResult);
```

```js
display(Inputs.table(vehiclePerformance, {
  columns: [
    "vehicle_id",
    "vehicle_number",
    "total_laps",
    "avg_speed",
    "avg_rpm",
    "avg_throttle",
    "avg_gear"
  ],
  header: {
    vehicle_id: "Vehicle ID",
    vehicle_number: "Number",
    total_laps: "Laps",
    avg_speed: "Avg Speed",
    avg_rpm: "Avg RPM",
    avg_throttle: "Avg Throttle",
    avg_gear: "Avg Gear"
  },
  format: {
    avg_speed: d => d.toFixed(2),
    avg_rpm: d => d.toFixed(0),
    avg_throttle: d => d.toFixed(1),
    avg_gear: d => d.toFixed(1)
  }
}))
```

## Average Speed by Vehicle

```js
// Debug: Show what data we have
display("Vehicle Performance Data:", vehiclePerformance);
display("Number of vehicles:", vehiclePerformance.length);

Plot.plot({
  title: `Vehicle Speed Comparison - ${selectedCircuit}`,
  width: 1000,
  height: Math.max(400, vehiclePerformance.length * 30),
  marginLeft: 150,
  x: {label: "Average Speed (mph)", grid: true, domain: [100, null]},
  y: {label: "Vehicle", grid: true},
  marks: [
    Plot.barX(vehiclePerformance, {
      x: "avg_speed", 
      y: "vehicle_id",
      fill: "#e60012",
      sort: {y: "-x"},
      tip: true
    }),
    Plot.ruleX([100], {stroke: "#999", strokeDasharray: "4,4"})
  ]
})
```

## Speed Distribution

```js
// Get speed distribution for the circuit
const speedDistributionResult = await db.sql`
  SELECT 
    speed
  FROM database.main_marts.fact_telemetry_data
  WHERE circuit = ${selectedCircuit}
    AND speed IS NOT NULL
    AND speed > 0
  LIMIT 50000
`;

const speedDistribution = Array.from(speedDistributionResult);
```

```js
Plot.plot({
  title: `Speed Distribution - ${selectedCircuit}`,
  width: 1000,
  height: 400,
  x: {label: "Speed (mph)", grid: true},
  y: {label: "Frequency", grid: true},
  color: {legend: false},
  marks: [
    Plot.rectY(
      speedDistribution, 
      Plot.binX(
        {y: "count"}, 
        {
          x: "speed", 
          fill: "#e60012",
          thresholds: 40,
          tip: true
        }
      )
    ),
    Plot.ruleY([0])
  ]
})
```

## Gear Usage Analysis

```js
const gearUsageResult = await db.sql`
  SELECT 
    gear,
    COUNT(*) as count
  FROM database.main_marts.fact_telemetry_data
  WHERE circuit = ${selectedCircuit}
    AND gear IS NOT NULL
  GROUP BY gear
  ORDER BY gear
`;

const gearUsage = Array.from(gearUsageResult);
```

```js
Plot.plot({
  title: `Gear Usage Distribution - ${selectedCircuit}`,
  width: 1000,
  height: 400,
  x: {label: "Gear Position", grid: true},
  y: {label: "Count", grid: true},
  marks: [
    Plot.barY(gearUsage, {
      x: "gear",
      y: "count",
      fill: "orange"
    }),
    Plot.ruleY([0])
  ]
})
```

## Track Map - Racing Lines

```js
// Get GPS coordinates colored by speed
const trackMapResult = await db.sql`
  SELECT 
    vbox_long_minutes,
    vbox_lat_minutes,
    speed,
    vehicle_id,
    lap
  FROM database.main_marts.fact_telemetry_data
  WHERE circuit = ${selectedCircuit}
    AND vbox_long_minutes IS NOT NULL
    AND vbox_lat_minutes IS NOT NULL
    AND speed IS NOT NULL
    AND lap <= 5
  ORDER BY vehicle_id, lap, event_timestamp
  LIMIT 10000
`;

const trackMap = Array.from(trackMapResult);
```

```js
Plot.plot({
  title: `Track Map - ${selectedCircuit} (Speed Heat Map)`,
  width: 1000,
  height: 800,
  aspectRatio: 1,
  x: {label: "Longitude", grid: true},
  y: {label: "Latitude", grid: true},
  color: {
    type: "linear",
    scheme: "YlOrRd",
    label: "Speed (mph)",
    legend: true
  },
  marks: [
    Plot.dot(trackMap, {
      x: "vbox_long_minutes",
      y: "vbox_lat_minutes",
      fill: "speed",
      r: 2,
      opacity: 0.6,
      tip: true
    })
  ]
})
```

## Track Map - Vehicle Comparison

```js
// Get racing lines for multiple vehicles
const vehicleTracksResult = await db.sql`
  SELECT 
    vbox_long_minutes,
    vbox_lat_minutes,
    speed,
    vehicle_id,
    lap
  FROM database.main_marts.fact_telemetry_data
  WHERE circuit = ${selectedCircuit}
    AND vbox_long_minutes IS NOT NULL
    AND vbox_lat_minutes IS NOT NULL
    AND lap = 1
  ORDER BY vehicle_id, event_timestamp
  LIMIT 20000
`;

const vehicleTracks = Array.from(vehicleTracksResult);
```

```js
Plot.plot({
  title: `Racing Lines - ${selectedCircuit} (Lap 1, Multiple Vehicles)`,
  width: 1000,
  height: 800,
  aspectRatio: 1,
  x: {label: "Longitude", grid: true},
  y: {label: "Latitude", grid: true},
  color: {
    legend: true,
    label: "Vehicle"
  },
  marks: [
    Plot.line(vehicleTracks, {
      x: "vbox_long_minutes",
      y: "vbox_lat_minutes",
      stroke: "vehicle_id",
      strokeWidth: 2,
      opacity: 0.7
    })
  ]
})
```

## Lap Time Progression

```js
// Show lap progression for all vehicles
const lapProgressionResult = await db.sql`
  SELECT 
    lap,
    vehicle_id,
    AVG(speed) as avg_speed
  FROM database.main_marts.fact_telemetry_data
  WHERE circuit = ${selectedCircuit}
    AND lap IS NOT NULL
    AND speed IS NOT NULL
  GROUP BY lap, vehicle_id
  ORDER BY lap, vehicle_id
  LIMIT 500
`;

const lapProgression = Array.from(lapProgressionResult);
```

```js
Plot.plot({
  title: `Lap Speed Progression - ${selectedCircuit}`,
  width: 1000,
  height: 400,
  x: {label: "Lap Number", grid: true},
  y: {label: "Average Speed", grid: true},
  color: {legend: true, label: "Vehicle"},
  marks: [
    Plot.line(lapProgression, {
      x: "lap", 
      y: "avg_speed", 
      stroke: "vehicle_id",
      strokeWidth: 2
    }),
    Plot.ruleY([0])
  ]
})
```
