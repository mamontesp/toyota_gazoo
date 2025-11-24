# Lap Insights

Detailed lap timing and weather context derived from `fact_laps`.

## Filters

```js
import {DuckDBClient} from "npm:@observablehq/duckdb";

const db = await DuckDBClient.of({
  database: FileAttachment("dev.duckdb")
});
```

```js
const escapeSqlLiteral = (value) => `${value}`.replace(/'/g, "''");
```

```js
const circuitsResult = await db.sql`
  SELECT DISTINCT circuit
  FROM database.main_marts.fact_laps
  WHERE circuit IS NOT NULL
  ORDER BY circuit
`;

const circuits = Array.from(circuitsResult);
const circuitOptions = ["All", ...circuits.map((d) => d.circuit)];
const selectedCircuit = view(
  Inputs.select(circuitOptions, {
    label: "Circuit",
    value: circuitOptions[0] ?? "All"
  })
);
```

```js
const racesQuery =
  selectedCircuit === "All"
    ? `
        SELECT DISTINCT race_number
        FROM database.main_marts.fact_laps
        WHERE race_number IS NOT NULL
        ORDER BY race_number
      `
    : `
        SELECT DISTINCT race_number
        FROM database.main_marts.fact_laps
        WHERE circuit = '${escapeSqlLiteral(selectedCircuit)}'
          AND race_number IS NOT NULL
        ORDER BY race_number
      `;

const racesResult = await db.query(racesQuery);
const races = Array.from(racesResult);
const raceOptions = ["All", ...races.map((d) => d.race_number)];
const selectedRace = view(
  Inputs.select(raceOptions, {
    label: "Race",
    value: raceOptions[0] ?? "All"
  })
);
```

```js
const vehicleWhereClauses = ["vehicle_id IS NOT NULL"];

if (selectedCircuit !== "All") {
  vehicleWhereClauses.push(`circuit = '${escapeSqlLiteral(selectedCircuit)}'`);
}

if (selectedRace !== "All") {
  vehicleWhereClauses.push(`race_number = ${selectedRace}`);
}

const vehiclesQuery = `
  SELECT DISTINCT vehicle_id
  FROM database.main_marts.fact_laps
  WHERE ${vehicleWhereClauses.join(" AND ")}
  ORDER BY vehicle_id
`;

const vehiclesResult = await db.query(vehiclesQuery);
const vehicles = Array.from(vehiclesResult);
const vehicleOptions = ["All", ...vehicles.map((d) => d.vehicle_id)];
const selectedVehicle = view(
  Inputs.select(vehicleOptions, {
    label: "Vehicle",
    value: vehicleOptions[0] ?? "All"
  })
);
```

```js
html`<div style="background:#f4f4f4;padding:0.75rem 1rem;border-radius:6px;margin-bottom:0.5rem;">
  <strong>Active Filters:</strong>
  Circuit = ${selectedCircuit},
  Race = ${selectedRace},
  Vehicle = ${selectedVehicle}
</div>`;
```

## Lap Summary

```js
const filterClauses = [
  selectedCircuit !== "All"
    ? `circuit = '${escapeSqlLiteral(selectedCircuit)}'`
    : "1 = 1",
  selectedRace !== "All" ? `race_number = ${selectedRace}` : "1 = 1",
  selectedVehicle !== "All"
    ? `vehicle_id = '${escapeSqlLiteral(selectedVehicle)}'`
    : "1 = 1"
];

const filterConditions = filterClauses.join(" AND ");
```

```js
const lapSummaryQuery = `
  WITH
    -- Aggregate lap metrics for the active filters
    lap_summary AS (
      SELECT
        COUNT(*) AS total_laps,
        AVG(lap_duration_seconds) AS avg_lap_duration_seconds,
        MIN(lap_duration_seconds) AS fastest_lap_seconds,
        MAX(lap_duration_seconds) AS slowest_lap_seconds
      FROM database.main_marts.fact_laps
      WHERE ${filterConditions}
        AND lap_duration_seconds IS NOT NULL
    ),
    -- Aggregate weather context for the active filters
    weather_snapshot AS (
      SELECT
        AVG(lap_mean_air_temp_celsius) AS mean_air_temp_f,
        AVG(lap_mean_track_temp_celsius) AS mean_track_temp_f,
        AVG(lap_mean_humidity_percentage) AS mean_humidity_pct,
        AVG(lap_mean_pressure_inches) AS mean_pressure_in
      FROM database.main_marts.fact_laps
      WHERE ${filterConditions}
    )
  SELECT
    total_laps,
    avg_lap_duration_seconds,
    fastest_lap_seconds,
    slowest_lap_seconds,
    mean_air_temp_f,
    mean_track_temp_f,
    mean_humidity_pct,
    mean_pressure_in
  FROM lap_summary
  CROSS JOIN weather_snapshot
`;

const lapSummaryResult = await db.query(lapSummaryQuery);
const lapSummary =
  Array.from(lapSummaryResult)[0] ?? {
    total_laps: 0,
    avg_lap_duration_seconds: null,
    fastest_lap_seconds: null,
    slowest_lap_seconds: null,
    mean_air_temp_f: null,
    mean_track_temp_f: null,
    mean_humidity_pct: null,
    mean_pressure_in: null
  };
```

```js
const formatMetric = (value, fractionDigits = 2, suffix = "") =>
  value != null ? `${value.toFixed(fractionDigits)}${suffix}` : "N/A";

html`<div class="grid grid-cols-4" style="gap:1rem;margin-bottom:1.5rem;">
  <div class="card">
    <h3>${lapSummary.total_laps ?? 0}</h3>
    <p>Total Laps</p>
  </div>
  <div class="card">
    <h3>${formatMetric(
      lapSummary.fastest_lap_seconds,
      3,
      "s"
    )}</h3>
    <p>Fastest Lap</p>
  </div>
  <div class="card">
    <h3>${formatMetric(
      lapSummary.avg_lap_duration_seconds,
      3,
      "s"
    )}</h3>
    <p>Average Lap</p>
  </div>
  <div class="card">
    <h3>${formatMetric(
      lapSummary.slowest_lap_seconds,
      3,
      "s"
    )}</h3>
    <p>Slowest Lap</p>
  </div>
</div>`;
```

```js
html`<div class="grid grid-cols-4" style="gap:1rem;margin-bottom:1.5rem;">
  <div class="card">
    <h3>${formatMetric(lapSummary.mean_track_temp_f, 1, "°F")}</h3>
    <p>Mean Track Temp</p>
  </div>
  <div class="card">
    <h3>${formatMetric(lapSummary.mean_air_temp_f, 1, "°F")}</h3>
    <p>Mean Air Temp</p>
  </div>
  <div class="card">
    <h3>${formatMetric(lapSummary.mean_humidity_pct, 1, "%")}</h3>
    <p>Mean Humidity</p>
  </div>
  <div class="card">
    <h3>${formatMetric(lapSummary.mean_pressure_in, 3, " inHg")}</h3>
    <p>Mean Pressure</p>
  </div>
</div>`;
```

## Lap Duration Trend

```js
const lapDetailsQuery = `
  WITH
    -- Lap level detail for charting and tables
    filtered_laps AS (
      SELECT
        circuit,
        race_number,
        vehicle_id,
        vehicle_number,
        lap,
        lap_duration_seconds,
        lap_start_timestamp_utc,
        lap_end_timestamp_utc,
        lap_mean_air_temp_celsius,
        lap_mean_track_temp_celsius,
        lap_mean_humidity_percentage,
        lap_mean_pressure_inches
      FROM database.main_marts.fact_laps
      WHERE ${filterConditions}
        AND lap IS NOT NULL
      ORDER BY lap
      LIMIT 500
    )
  SELECT * FROM filtered_laps
`;

const lapDetailsResult = await db.query(lapDetailsQuery);
const lapDetails = Array.from(lapDetailsResult);
```

```js
if (lapDetails.length === 0) {
  display(
    html`<div style="background:#fff3cd;padding:1rem;border-radius:8px;border:1px solid #ffeeba;">
      ⚠️ No lap data available for the selected filters.
    </div>`
  );
} else {
  const lapDurationData = lapDetails.filter(
    (d) => d.lap_duration_seconds != null
  );

  if (lapDurationData.length === 0) {
    display(
      html`<div style="background:#fff3cd;padding:1rem;border-radius:8px;border:1px solid #ffeeba;">
        ⚠️ Lap durations are missing for the selected filters.
      </div>`
    );
  } else {
    display(
      Plot.plot({
        title: "Lap Duration Distribution",
        width: 1000,
        height: 400,
        x: {label: "Lap", grid: true},
        y: {label: "Duration (s)", grid: true},
        marks: [
          Plot.boxY(lapDurationData, {
            x: "lap",
            y: "lap_duration_seconds",
            fill: "#e60012",
            stroke: "#941510",
            tip: true
          }),
          Plot.ruleY([0])
        ]
      })
    );
  }
}
```

## Weather Context by Lap

```js
const temperatureSeries = lapDetails
  .flatMap((d) => [
    {
      lap: d.lap,
      metric: "Track Temp (°F)",
      value: d.lap_mean_track_temp_celsius
    },
    {
      lap: d.lap,
      metric: "Air Temp (°F)",
      value: d.lap_mean_air_temp_celsius
    }
  ])
  .filter((d) => d.value != null);

if (temperatureSeries.length > 0) {
  display(
    Plot.plot({
      title: "Temperature Distribution",
      width: 1000,
      height: 400,
      x: {label: "Metric", grid: true},
      y: {label: "Temperature (°F)", grid: true},
      color: {legend: true, label: "Metric"},
      marks: [
        Plot.boxY(temperatureSeries, {
          x: "metric",
          y: "value",
          fill: "metric",
          tip: true
        }),
        Plot.dot(temperatureSeries, {
          x: "metric",
          y: "value",
          fill: "metric",
          r: 2,
          opacity: 0.5
        }),
        Plot.ruleY([0])
      ]
    })
  );
}
```

```js
const humiditySeries = lapDetails
  .map((d) => ({
    lap: d.lap,
    value: d.lap_mean_humidity_percentage
  }))
  .filter((d) => d.value != null);

if (humiditySeries.length > 0) {
  display(
    Plot.plot({
      title: "Humidity by Lap",
      width: 1000,
      height: 300,
      x: {label: "Lap", grid: true},
      y: {label: "Humidity (%)", grid: true, domain: [0, 100]},
      
      marks: [
        Plot.line(humiditySeries, {
          x: "lap",
          y: "value",
          stroke: "#1f77b4",
          strokeWidth: 2
        }),
        Plot.ruleY([0])
      ]
    })
  );
}
```

## Lap Detail Table

```js
if (lapDetails.length > 0) {
  const lapTimingTableData = lapDetails.map((d) => ({
    lap: d.lap,
    lap_duration_seconds: d.lap_duration_seconds,
    lap_start_timestamp_utc: d.lap_start_timestamp_utc,
    lap_end_timestamp_utc: d.lap_end_timestamp_utc
  }));

  display(
    html`<h3 style="margin-top:1.5rem;">Lap Timing Details</h3>`
  );

  const lapTimingTable = Inputs.table(lapTimingTableData, {
    columns: [
      "lap",
      "lap_duration_seconds",
      "lap_start_timestamp_utc",
      "lap_end_timestamp_utc"
    ],
    header: {
      lap: "Lap",
      lap_duration_seconds: "Lap Duration (s)",
      lap_start_timestamp_utc: "Start UTC",
      lap_end_timestamp_utc: "End UTC"
    },
    format: {
      lap_duration_seconds: (d) => (d != null ? d.toFixed(3) : "N/A")
    },
    rows: 20
  });

  display(html`<div class="lap-table-wrapper">${lapTimingTable}</div>`);

  const lapWeatherTableData = lapDetails.map((d) => ({
    lap: d.lap,
    lap_mean_track_temp_celsius: d.lap_mean_track_temp_celsius,
    lap_mean_air_temp_celsius: d.lap_mean_air_temp_celsius,
    lap_mean_humidity_percentage: d.lap_mean_humidity_percentage,
    lap_mean_pressure_inches: d.lap_mean_pressure_inches
  }));

  display(
    html`<h3 style="margin-top:1.5rem;">Lap Weather Snapshot</h3>`
  );

  const lapWeatherTable = Inputs.table(lapWeatherTableData, {
    columns: [
      "lap",
      "lap_mean_track_temp_celsius",
      "lap_mean_air_temp_celsius",
      "lap_mean_humidity_percentage",
      "lap_mean_pressure_inches"
    ],
    header: {
      lap: "Lap",
      lap_mean_track_temp_celsius: "Track Temp (°C)",
      lap_mean_air_temp_celsius: "Air Temp (°C)",
      lap_mean_humidity_percentage: "Humidity (%)",
      lap_mean_pressure_inches: "Pressure (inHg)"
    },
    format: {
      lap_mean_track_temp_celsius: (d) => (d != null ? d.toFixed(1) : "N/A"),
      lap_mean_air_temp_celsius: (d) => (d != null ? d.toFixed(1) : "N/A"),
      lap_mean_humidity_percentage: (d) => (d != null ? d.toFixed(1) : "N/A"),
      lap_mean_pressure_inches: (d) => (d != null ? d.toFixed(3) : "N/A")
    },
    rows: 20
  });

  display(html`<div class="lap-table-wrapper">${lapWeatherTable}</div>`);
}
```


