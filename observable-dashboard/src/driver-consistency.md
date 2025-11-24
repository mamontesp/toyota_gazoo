# Driver Consistency Histograms

Explore the distribution of key telemetry metrics with configurable filters sourced from `viz_driver_consistency`.

## Filters

```js
import {DuckDBClient} from "npm:@observablehq/duckdb";

const db = await DuckDBClient.of({
  database: FileAttachment("dev.duckdb")
});

const telemetryTable = "database.main_marts.viz_driver_consistency";
const escapeSqlLiteral = (value) => `${value}`.replace(/'/g, "''");
```

```js
html`<style>
output {
  background: #ffffff !important;
  color: #111111 !important;
  padding: 0.1rem 0.5rem !important;
  border-radius: 4px !important;
  border: 1px solid #d1d5db !important;
  font-weight: 600 !important;
  box-shadow: none !important;
}

input[type="number"] {
  background: #ffffff !important;
  color: #111111 !important;
  padding: 0.1rem 0.5rem !important;
  border-radius: 4px !important;
  border: 1px solid #d1d5db !important;
  font-weight: 600 !important;
  box-shadow: none !important;
}

input[type="range"] {
  accent-color: #e60012;
}
</style>`;
```

```js
const vehiclesQuery = `
  WITH
    -- Unique vehicles recorded in driver consistency model
    vehicle_inventory AS (
      SELECT DISTINCT vehicle_id
      FROM ${telemetryTable}
      WHERE vehicle_id IS NOT NULL
    )
  SELECT vehicle_id
  FROM vehicle_inventory
  ORDER BY vehicle_id
`;

const vehiclesResult = await db.query(vehiclesQuery);
const vehicleRows = Array.from(vehiclesResult);
const vehicleOptions = ["All", ...vehicleRows.map((row) => row.vehicle_id)];
const selectedVehicle = view(
  Inputs.select(vehicleOptions, {
    label: "Vehicle",
    value: vehicleOptions[0] ?? "All"
  })
);
```

```js
const sectorsQuery = `
  WITH
    -- Sector numbers available for telemetry histograms
    sector_inventory AS (
      SELECT DISTINCT sector_number
      FROM ${telemetryTable}
      WHERE sector_number IS NOT NULL
    )
  SELECT sector_number
  FROM sector_inventory
  ORDER BY sector_number
`;

const sectorsResult = await db.query(sectorsQuery);
const sectorRows = Array.from(sectorsResult);
const sectorOptions = sectorRows.map((row) => `${row.sector_number}`);
const sectorChoices = ["All", ...sectorOptions];
const defaultSector = sectorOptions.includes("1") ? "1" : sectorChoices[0] ?? "All";
const selectedSector = view(
  Inputs.select(sectorChoices, {
    label: "Sector Number",
    value: defaultSector
  })
);
```

```js
const lapClauses = ["lap IS NOT NULL"];

if (selectedVehicle !== "All") {
  lapClauses.push(`vehicle_id = '${escapeSqlLiteral(selectedVehicle)}'`);
}

if (selectedSector !== "All") {
  lapClauses.push(`sector_number = ${Number(selectedSector)}`);
}

const lapsQuery = `
  WITH
    -- Lap values for the active vehicle and sector filters
    lap_inventory AS (
      SELECT DISTINCT lap
      FROM ${telemetryTable}
      WHERE ${lapClauses.join(" AND ")}
    )
  SELECT lap
  FROM lap_inventory
  ORDER BY lap
`;

const lapsResult = await db.query(lapsQuery);
const lapRows = Array.from(lapsResult);
const lapOptions = ["All", ...lapRows.map((row) => `${row.lap}`)];
const selectedLap = view(
  Inputs.select(lapOptions, {
    label: "Lap",
    value: lapOptions[0] ?? "All"
  })
);
```

```js
const buildBaseClauses = () => {
  const clauses = ["meter_position_m IS NOT NULL"];

  if (selectedVehicle !== "All") {
    clauses.push(`vehicle_id = '${escapeSqlLiteral(selectedVehicle)}'`);
  }

  if (selectedSector !== "All") {
    clauses.push(`sector_number = ${Number(selectedSector)}`);
  }

  if (selectedLap !== "All") {
    clauses.push(`lap = ${Number(selectedLap)}`);
  }

  return clauses;
};

const meterExtentQuery = `
  WITH
    -- Range of telemetry meter positions for current filters
    meter_extent AS (
      SELECT
        MIN(meter_position_m) AS min_meter_position,
        MAX(meter_position_m) AS max_meter_position
      FROM ${telemetryTable}
      WHERE ${buildBaseClauses().join(" AND ")}
    )
  SELECT
    min_meter_position,
    max_meter_position
  FROM meter_extent
`;

const meterExtentResult = await db.query(meterExtentQuery);
const meterExtentRow =
  Array.from(meterExtentResult)[0] ?? {
    min_meter_position: 0,
    max_meter_position: 0
  };

let rangeMin = meterExtentRow.min_meter_position ?? 0;
let rangeMax = meterExtentRow.max_meter_position ?? rangeMin;

if (!Number.isFinite(rangeMin)) {
  rangeMin = 0;
}

if (!Number.isFinite(rangeMax)) {
  rangeMax = rangeMin;
}

if (rangeMax <= rangeMin) {
  rangeMax = rangeMin + 1;
}

const previousMeterSelection =
  globalThis.driverConsistencyMeterRange ?? {
    start: rangeMin,
    end: rangeMax
  };

const clampMeterValue = (value, fallback) => {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return fallback;
  }
  return Math.min(Math.max(numeric, rangeMin), rangeMax);
};

const initialMeterStart = clampMeterValue(previousMeterSelection.start, rangeMin);
const initialMeterEnd = clampMeterValue(previousMeterSelection.end, rangeMax);

const meterRangeForm = view(
  Inputs.form(
    {
      start: Inputs.range([rangeMin, rangeMax], {
        label: "Meter Start (m)",
        value: initialMeterStart,
        step: 1,
        format: (value) => value.toFixed(0),
        transform: Number
      }),
      end: Inputs.range([rangeMin, rangeMax], {
        label: "Meter End (m)",
        value: initialMeterEnd,
        step: 1,
        format: (value) => value.toFixed(0),
        transform: Number
      })
    },
    {
      value: {
        start: initialMeterStart,
        end: initialMeterEnd
      }
    }
  )
);

const extractMeterValue = (value, fallback) =>
  Number.isFinite(Number(value)) ? Number(value) : fallback;

const startValue = clampMeterValue(
  extractMeterValue(meterRangeForm?.start, initialMeterStart),
  initialMeterStart
);

const endValue = clampMeterValue(
  extractMeterValue(meterRangeForm?.end, initialMeterEnd),
  initialMeterEnd
);

const selectedMeterStart = Math.min(startValue, endValue);
const selectedMeterEnd = Math.max(startValue, endValue);

globalThis.driverConsistencyMeterRange = {
  start: selectedMeterStart,
  end: selectedMeterEnd
};
```

```js
html`<div style="background:#f4f4f4;padding:0.75rem 1rem;border-radius:6px;margin-bottom:1rem;">
  <strong>Active Filters:</strong>
  Vehicle = ${selectedVehicle},
  Lap = ${selectedLap},
  Sector = ${selectedSector},
  Meter Position = ${selectedMeterStart.toFixed(0)}m – ${selectedMeterEnd.toFixed(0)}m
</div>`;
```

## Metric Histograms

```js
const telemetryClauses = ["meter_position_m IS NOT NULL"];

if (selectedVehicle !== "All") {
  telemetryClauses.push(`vehicle_id = '${escapeSqlLiteral(selectedVehicle)}'`);
}

if (selectedSector !== "All") {
  telemetryClauses.push(`sector_number = ${Number(selectedSector)}`);
}

if (selectedLap !== "All") {
  telemetryClauses.push(`lap = ${Number(selectedLap)}`);
}

telemetryClauses.push(
  `meter_position_m BETWEEN ${selectedMeterStart} AND ${selectedMeterEnd}`
);

const telemetryQuery = `
  WITH
    -- Telemetry measurements trimmed to active filters
    filtered_telemetry AS (
      SELECT
        vehicle_id,
        lap,
        sector_number,
        meter_position_m,
        speed,
        aps,
        front_brake_pressure,
        rear_brake_pressure,
        steering_angle
      FROM ${telemetryTable}
      WHERE ${telemetryClauses.join(" AND ")}
    )
  SELECT *
  FROM filtered_telemetry
`;

const telemetryResult = await db.query(telemetryQuery);
const telemetryData = Array.from(telemetryResult);

html`<div style="background:#eef2ff;border:1px solid #c7d2fe;border-radius:8px;padding:0.75rem;margin-bottom:1rem;font-family:monospace;font-size:0.9rem;">
  <strong>WHERE clause debug:</strong><br />
  ${telemetryClauses.join(" AND ")}
</div>`;
```

```js
if (telemetryData.length === 0) {
  display(
    html`<div style="background:#fff3cd;padding:1rem;border-radius:8px;border:1px solid #ffeeba;">
      ⚠️ No telemetry records match the current filters. Adjust the selections to populate the histograms.
    </div>`
  );
} else {
  const histogramConfigs = [
    {
      field: "speed",
      title: "Speed Distribution",
      xLabel: "Speed",
      color: "#e60012"
    },
    {
      field: "aps",
      title: "Throttle (APS) Distribution",
      xLabel: "Throttle Position (APS)",
      color: "#1f77b4"
    },
    {
      field: "front_brake_pressure",
      title: "Front Brake Pressure Distribution",
      xLabel: "Front Brake Pressure",
      color: "#ff7f0e"
    },
    {
      field: "rear_brake_pressure",
      title: "Rear Brake Pressure Distribution",
      xLabel: "Rear Brake Pressure",
      color: "#2ca02c"
    }
  ];

  for (const config of histogramConfigs) {
    const series = telemetryData.filter((row) => row[config.field] != null);

    if (series.length === 0) {
      display(
        html`<div style="background:#f8d7da;padding:1rem;border-radius:8px;border:1px solid #f5c2c7;margin-bottom:1rem;">
          ${config.title}: No data available after filtering.
        </div>`
      );
      continue;
    }

    display(
      Plot.plot({
        title: config.title,
        width: 1000,
        height: 320,
        marginLeft: 60,
        marginBottom: 55,
        x: {label: config.xLabel, grid: true},
        y: {label: "Count", grid: true},
        marks: [
          Plot.rectY(
            series,
            Plot.binX(
              {y: "count"},
              {
                x: config.field,
                thresholds: 30,
                fill: config.color,
                tip: true
              }
            )
          ),
          Plot.ruleY([0])
        ]
      })
    );
  }
}
```


