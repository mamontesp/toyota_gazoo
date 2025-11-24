# Wind Conditions

Explore wind speed and direction readings from `fact_wind` to understand how weather evolves by circuit and race. For example, pick *Barber Motorsports Park – Race 1* to compare gusts across the start window.

## Filters

```js
import {DuckDBClient} from "npm:@observablehq/duckdb";

const db = await DuckDBClient.of({
  database: FileAttachment("dev.duckdb")
});

const escapeSqlLiteral = (value) => `${value}`.replace(/'/g, "''");
```

```js
import * as d3 from "npm:d3";
```

```js
const circuitsResult = await db.sql`
  SELECT DISTINCT circuit
  FROM database.main_marts.fact_wind
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
const raceWhereClauses = ["race_number IS NOT NULL"];

if (selectedCircuit !== "All") {
  raceWhereClauses.push(`circuit = '${escapeSqlLiteral(selectedCircuit)}'`);
}

const racesQuery = `
  WITH
    -- Distinct races matching the active circuit filter
    race_list AS (
      SELECT DISTINCT race_number
      FROM database.main_marts.fact_wind
      WHERE ${raceWhereClauses.join(" AND ")}
    )
  SELECT race_number
  FROM race_list
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
html`<div style="background:#f4f4f4;padding:0.75rem 1rem;border-radius:6px;margin-bottom:0.5rem;">
  <strong>Active Filters:</strong>
  Circuit = ${selectedCircuit},
  Race = ${selectedRace}
</div>`;
```

```js
const filterClauses = [
  selectedCircuit !== "All"
    ? `circuit = '${escapeSqlLiteral(selectedCircuit)}'`
    : "1 = 1",
  selectedRace !== "All" ? `race_number = ${selectedRace}` : "1 = 1"
];

const filterConditions = filterClauses.join(" AND ");
```

## Wind Speed Snapshot

```js
const windSummaryQuery = `
  WITH
    -- Wind samples for the active filters
    filtered_wind AS (
      SELECT
        wind_speed_mph,
        wind_speed_kph
      FROM database.main_marts.fact_wind
      WHERE ${filterConditions}
    ),
    -- Speed statistics in miles per hour
    mph_stats AS (
      SELECT
        AVG(wind_speed_mph) AS avg_wind_speed_mph,
        MIN(wind_speed_mph) AS min_wind_speed_mph,
        MAX(wind_speed_mph) AS max_wind_speed_mph
      FROM filtered_wind
      WHERE wind_speed_mph IS NOT NULL
    ),
    -- Speed statistics in kilometers per hour
    kph_stats AS (
      SELECT
        AVG(wind_speed_kph) AS avg_wind_speed_kph,
        MIN(wind_speed_kph) AS min_wind_speed_kph,
        MAX(wind_speed_kph) AS max_wind_speed_kph
      FROM filtered_wind
      WHERE wind_speed_kph IS NOT NULL
    )
  SELECT
    avg_wind_speed_mph,
    min_wind_speed_mph,
    max_wind_speed_mph,
    avg_wind_speed_kph,
    min_wind_speed_kph,
    max_wind_speed_kph
  FROM mph_stats
  CROSS JOIN kph_stats
`;

const windSummaryResult = await db.query(windSummaryQuery);
const windSummary =
  Array.from(windSummaryResult)[0] ?? {
    avg_wind_speed_mph: null,
    min_wind_speed_mph: null,
    max_wind_speed_mph: null,
    avg_wind_speed_kph: null,
    min_wind_speed_kph: null,
    max_wind_speed_kph: null
  };

const formatMetric = (value, fractionDigits = 1, suffix = "") =>
  value != null ? `${value.toFixed(fractionDigits)}${suffix}` : "N/A";
```

```js
html`<div class="grid grid-cols-3" style="gap:1rem;margin-bottom:1.5rem;">
  <div class="card">
    <h3>${formatMetric(windSummary.avg_wind_speed_mph, 1, " mph")}</h3>
    <p>Avg Wind Speed (mph)</p>
  </div>
  <div class="card">
    <h3>${formatMetric(windSummary.max_wind_speed_mph, 1, " mph")}</h3>
    <p>Peak Wind Speed (mph)</p>
  </div>
  <div class="card">
    <h3>${formatMetric(windSummary.min_wind_speed_mph, 1, " mph")}</h3>
    <p>Lowest Wind Speed (mph)</p>
  </div>
</div>`;
```

```js
html`<div class="grid grid-cols-3" style="gap:1rem;margin-bottom:1.5rem;">
  <div class="card">
    <h3>${formatMetric(windSummary.avg_wind_speed_kph, 1, " km/h")}</h3>
    <p>Avg Wind Speed (km/h)</p>
  </div>
  <div class="card">
    <h3>${formatMetric(windSummary.max_wind_speed_kph, 1, " km/h")}</h3>
    <p>Peak Wind Speed (km/h)</p>
  </div>
  <div class="card">
    <h3>${formatMetric(windSummary.min_wind_speed_kph, 1, " km/h")}</h3>
    <p>Lowest Wind Speed (km/h)</p>
  </div>
</div>`;
```

## Direction Breakdown

```js
let hasWindSpeedBandColumn = true;
try {
  await db.query(`
    SELECT wind_speed_band_mph
    FROM database.main_marts.fact_wind
    LIMIT 1
  `);
} catch (error) {
  if (`${error}`.includes("wind_speed_band_mph")) {
    hasWindSpeedBandColumn = false;
  } else {
    throw error;
  }
}
```

```js
const windSpeedBandExpression = hasWindSpeedBandColumn
  ? "wind_speed_band_mph"
  : `CASE
        WHEN wind_speed_mph < 5 THEN '00-05 mph'
        WHEN wind_speed_mph < 10 THEN '05-10 mph'
        WHEN wind_speed_mph < 15 THEN '10-15 mph'
        WHEN wind_speed_mph < 20 THEN '15-20 mph'
        ELSE '20+ mph'
    END`;

const windSpeedBandCondition = hasWindSpeedBandColumn
  ? "AND wind_speed_band_mph IS NOT NULL"
  : "";
```

```js
const windRoseQuery = `
  WITH
    -- Directional wind samples with valid speed readings
    filtered_wind AS (
      SELECT
        wind_direction_degrees,
        wind_speed_mph,
        ${windSpeedBandExpression} AS wind_speed_band_mph
      FROM database.main_marts.fact_wind
      WHERE ${filterConditions}
        AND wind_direction_degrees IS NOT NULL
        AND wind_speed_mph IS NOT NULL
        ${windSpeedBandCondition}
    ),
    -- Bucket samples into 16 compass sectors (22.5° each)
    direction_bins AS (
      SELECT
        FLOOR((wind_direction_degrees % 360) / 22.5) AS direction_index,
        wind_speed_band_mph AS speed_bin
      FROM filtered_wind
    ),
    -- Aggregate counts by direction sector and speed band
    aggregated AS (
      SELECT
        direction_index,
        speed_bin,
        COUNT(*) AS sample_count
      FROM direction_bins
      GROUP BY direction_index, speed_bin
    )
  SELECT
    direction_index,
    speed_bin,
    sample_count
  FROM aggregated
  ORDER BY direction_index, speed_bin
`;

const windRoseResult = await db.query(windRoseQuery);
const windRoseRaw = Array.from(windRoseResult);
const totalWindSamples = windRoseRaw.reduce(
  (sum, row) => sum + (row.sample_count ?? 0),
  0
);

const directionLabels = [
  "North",
  "North-Northeast",
  "Northeast",
  "East-Northeast",
  "East",
  "East-Southeast",
  "Southeast",
  "South-Southeast",
  "South",
  "South-Southwest",
  "Southwest",
  "West-Southwest",
  "West",
  "West-Northwest",
  "Northwest",
  "North-Northwest"
];

const speedBins = [
  "00-05 mph",
  "05-10 mph",
  "10-15 mph",
  "15-20 mph",
  "20+ mph"
];

const windRoseData = windRoseRaw
  .map((row) => ({
    direction_index: row.direction_index,
    direction_label: directionLabels[row.direction_index] ?? "Unknown",
    speed_bin: row.speed_bin,
    sample_count: row.sample_count,
    percentage:
      totalWindSamples > 0
        ? (row.sample_count / totalWindSamples) * 100
        : 0
  }))
  .filter((row) => row.direction_label !== "Unknown");

windRoseData.sort(
  (a, b) =>
    a.direction_index - b.direction_index ||
    speedBins.indexOf(a.speed_bin) - speedBins.indexOf(b.speed_bin)
);

const directionPercentages = windRoseData.reduce((acc, row) => {
  acc[row.direction_index] =
    (acc[row.direction_index] ?? 0) + row.percentage;
  return acc;
}, {});

const radialMax = Object.values(directionPercentages).reduce(
  (max, value) => Math.max(max, value),
  0
);
const radialDomainMax = radialMax > 0 ? radialMax * 1.1 : 1;
```

```js
if (totalWindSamples === 0) {
  display(
    html`<div style="background:#fff3cd;padding:1rem;border-radius:8px;border:1px solid #ffeeba;">
      ⚠️ No directional wind samples available for the selected filters.
    </div>`
  );
} else {
  const width = 560;
  const height = 560;
  const margin = 60;
  const radius = Math.min(width, height) / 2 - margin;

  const radiusScale = d3
    .scaleLinear()
    .domain([0, radialDomainMax])
    .range([0, radius]);

  const color = d3
    .scaleOrdinal()
    .domain(speedBins)
    .range(["#cfe8ff", "#79c0ff", "#2378c9", "#0b4f9c", "#002b64"]);

  const directionStep = (2 * Math.PI) / directionLabels.length;
  const segments = [];

  for (let directionIndex = 0; directionIndex < directionLabels.length; directionIndex++) {
    const rows = windRoseData.filter(
      (row) => row.direction_index === directionIndex
    );

    if (rows.length === 0) {
      continue;
    }

    rows.sort(
      (a, b) => speedBins.indexOf(a.speed_bin) - speedBins.indexOf(b.speed_bin)
    );

    let cumulative = 0;

    for (const row of rows) {
      const startAngle = directionIndex * directionStep - Math.PI / 2;
      const endAngle = startAngle + directionStep;
      const innerValue = cumulative;
      const outerValue = cumulative + row.percentage;

      cumulative = outerValue;

      if (row.percentage <= 0) {
        continue;
      }

      segments.push({
        directionIndex,
        directionLabel: directionLabels[directionIndex],
        speedBin: row.speed_bin,
        startAngle,
        endAngle,
        innerValue,
        outerValue,
        sampleCount: row.sample_count,
        percentage: row.percentage
      });
    }
  }

  const svg = d3
    .create("svg")
    .attr("viewBox", `0 0 ${width} ${height}`)
    .attr("width", width)
    .attr("height", height);

  svg
    .append("text")
    .attr("x", width / 2)
    .attr("y", 28)
    .attr("text-anchor", "middle")
    .attr("font-size", 18)
    .attr("font-weight", 600)
    .text("Wind Rose (speed band stack)");

  const chartGroup = svg
    .append("g")
    .attr("transform", `translate(${width / 2},${height / 2})`);

  const radialTicks = d3.ticks(0, radialDomainMax, 4).slice(1);
  const axisGroup = chartGroup.append("g").attr("class", "radial-grid");

  axisGroup
    .selectAll("circle")
    .data(radialTicks)
    .join("circle")
    .attr("r", (tick) => radiusScale(tick))
    .attr("fill", "none")
    .attr("stroke", "#d0d7de")
    .attr("stroke-width", 0.8);

  axisGroup
    .selectAll("text")
    .data(radialTicks)
    .join("text")
    .attr("y", (tick) => -radiusScale(tick) - 4)
    .attr("text-anchor", "middle")
    .attr("fill", "#555")
    .attr("font-size", 10)
    .text((tick) => `${tick.toFixed(0)}%`);

  const directionAxis = chartGroup.append("g").attr("class", "direction-grid");

  directionAxis
    .selectAll("line")
    .data(directionLabels.map((_, index) => index))
    .join("line")
    .attr("x1", 0)
    .attr("y1", 0)
    .attr("x2", (index) =>
      radiusScale(radialDomainMax) *
      Math.cos(index * directionStep - Math.PI / 2)
    )
    .attr("y2", (index) =>
      radiusScale(radialDomainMax) *
      Math.sin(index * directionStep - Math.PI / 2)
    )
    .attr("stroke", "#d0d7de")
    .attr("stroke-width", 0.8);

  directionAxis
    .selectAll("text")
    .data(directionLabels.map((label, index) => ({label, index})))
    .join("text")
    .attr("x", (d) =>
      (radiusScale(radialDomainMax) + 16) *
      Math.cos(d.index * directionStep - Math.PI / 2)
    )
    .attr("y", (d) =>
      (radiusScale(radialDomainMax) + 16) *
      Math.sin(d.index * directionStep - Math.PI / 2)
    )
    .attr("text-anchor", "middle")
    .attr("alignment-baseline", "middle")
    .attr("font-size", 10)
    .attr("fill", "#444")
    .text((d) => d.label.replace(" ", "\n"));

  const arcGenerator = d3
    .arc()
    .innerRadius((segment) => radiusScale(segment.innerValue))
    .outerRadius((segment) => radiusScale(segment.outerValue))
    .startAngle((segment) => segment.startAngle)
    .endAngle((segment) => segment.endAngle)
    .padAngle(0.005)
    .padRadius(radius);

  const segmentsGroup = chartGroup.append("g").attr("class", "segments");

  segmentsGroup
    .selectAll("path")
    .data(segments)
    .join("path")
    .attr("d", arcGenerator)
    .attr("fill", (segment) => color(segment.speedBin))
    .attr("stroke", "#fff")
    .attr("stroke-width", 0.7)
    .append("title")
    .text(
      (segment) => `${segment.directionLabel}
${segment.speedBin}
Samples: ${segment.sampleCount.toLocaleString()}
Share: ${segment.percentage.toFixed(1)}%`
    );

  const legendGroup = svg
    .append("g")
    .attr(
      "transform",
      `translate(${width - margin - 120}, ${margin})`
    );

  legendGroup
    .append("text")
    .attr("x", 0)
    .attr("y", 0)
    .attr("font-size", 12)
    .attr("font-weight", 600)
    .text("Wind speed band");

  legendGroup
    .selectAll("g.legend-item")
    .data(speedBins)
    .join("g")
    .attr("class", "legend-item")
    .attr("transform", (_, index) => `translate(0, ${16 + index * 18})`)
    .call((legendItem) => {
      legendItem
        .append("rect")
        .attr("width", 14)
        .attr("height", 14)
        .attr("fill", (bin) => color(bin))
        .attr("stroke", "#c0c0c0");

      legendItem
        .append("text")
        .attr("x", 20)
        .attr("y", 10)
        .attr("fill", "#333")
        .attr("font-size", 11)
        .text((bin) => bin);
    });

  display(svg.node());

  display(
    html`<p style="margin-top:0.75rem;">
      ${totalWindSamples.toLocaleString()} labeled wind samples grouped into ${
        Object.keys(directionPercentages).length
      } direction sectors. Radial distance represents cumulative share
      of samples per sector while segment color encodes the wind speed band.
    </p>`
  );
}
```

## Wind Timeline

```js
const windTimelineQuery = `
  WITH
    -- Ordered wind timeline for charting
    filtered_wind AS (
      SELECT
        time_utc,
        wind_speed_mph,
        wind_speed_kph,
        wind_direction_degrees,
        wind_direction_label
      FROM database.main_marts.fact_wind
      WHERE ${filterConditions}
      ORDER BY time_utc
      LIMIT 2000
    )
  SELECT *
  FROM filtered_wind
`;

const windTimelineResult = await db.query(windTimelineQuery);
const windTimeline = Array.from(windTimelineResult).map((d) => ({
  ...d,
  time_utc: d.time_utc ? new Date(d.time_utc) : null
}));
```

```js
const windVectorSeries = windTimeline
  .filter(
    (d) =>
      d.time_utc &&
      d.wind_speed_mph != null &&
      d.wind_direction_degrees != null
  )
  .map((d) => {
    const baseDegrees = d.wind_direction_degrees;
    const flowDegrees = ((baseDegrees ?? 0) + 180) % 360;
    const flowRadians = (flowDegrees * Math.PI) / 180;
    const uComponentMph =
      Math.sin(flowRadians) * (d.wind_speed_mph ?? 0);
    const vComponentMph =
      Math.cos(flowRadians) * (d.wind_speed_mph ?? 0);

    return {
      time_utc: d.time_utc,
      wind_speed_mph: d.wind_speed_mph,
      wind_speed_kph: d.wind_speed_kph,
      wind_direction_label: d.wind_direction_label,
      wind_direction_degrees: baseDegrees,
      flow_direction_degrees: flowDegrees,
      arrow_rotation: 90 - flowDegrees,
      u_component_mph: uComponentMph,
      v_component_mph: vComponentMph
    };
  });
```

```js
const speedSeries = windTimeline
  .filter((d) => d.time_utc && d.wind_speed_mph != null)
  .map((d) => ({
    time_utc: d.time_utc,
    wind_speed_mph: d.wind_speed_mph,
    wind_speed_kph: d.wind_speed_kph
  }));

if (speedSeries.length === 0) {
  display(
    html`<div style="background:#fff3cd;padding:1rem;border-radius:8px;border:1px solid #ffeeba;">
      ⚠️ No wind speed readings available for the selected filters.
    </div>`
  );
} else {
  display(
    Plot.plot({
      title: "Wind Speed Over Time",
      width: 1000,
      height: 400,
      x: {label: "Timestamp (UTC)", grid: true},
      y: {label: "Wind Speed (mph)", grid: true},
      marks: [
        Plot.line(speedSeries, {
          x: "time_utc",
          y: "wind_speed_mph",
          stroke: "#1f77b4",
          strokeWidth: 2,
          tip: true
        }),
        Plot.ruleY([0])
      ]
    })
  );
}
```

```js
const directionSeries = windTimeline.filter(
  (d) => d.time_utc && d.wind_direction_degrees != null
);

if (directionSeries.length > 0) {
  display(
    Plot.plot({
      title: "Wind Direction Over Time",
      width: 1000,
      height: 400,
      x: {label: "Timestamp (UTC)", grid: true},
      y: {label: "Direction (degrees)", grid: true, domain: [0, 360]},
      marks: [
        Plot.line(directionSeries, {
          x: "time_utc",
          y: "wind_direction_degrees",
          stroke: "#e60012",
          strokeWidth: 2,
          tip: true
        }),
        Plot.ruleY([0]),
        Plot.ruleY([360])
      ]
    })
  );
}
```

## Recent Wind Samples

```js
const windSamplesQuery = `
  WITH
    -- Filtered wind samples for tabular review
    filtered_wind AS (
      SELECT
        time_utc,
        wind_speed_mph,
        wind_speed_kph,
        wind_direction_degrees,
        wind_direction_label,
        circuit,
        race_number
      FROM database.main_marts.fact_wind
      WHERE ${filterConditions}
        AND (wind_speed_mph IS NOT NULL OR wind_direction_degrees IS NOT NULL)
      ORDER BY time_utc DESC
      LIMIT 500
    )
  SELECT * FROM filtered_wind
`;

const windSamplesResult = await db.query(windSamplesQuery);
const windSamples = Array.from(windSamplesResult);
```

```js
if (windSamples.length === 0) {
  display(
    html`<div style="background:#fff3cd;padding:1rem;border-radius:8px;border:1px solid #ffeeba;">
      ⚠️ No wind readings available for the selected filters.
    </div>`
  );
} else {
  display(
    html`<h3 style="margin-top:1.5rem;">Latest Wind Observations</h3>`
  );

  display(
    Inputs.table(windSamples, {
      columns: [
        "time_utc",
        "wind_speed_mph",
        "wind_speed_kph",
        "wind_direction_degrees",
        "wind_direction_label",
        "circuit",
        "race_number"
      ],
      format: {
        wind_speed_mph: (d) => (d != null ? d.toFixed(1) : null),
        wind_speed_kph: (d) => (d != null ? d.toFixed(1) : null),
        wind_direction_degrees: (d) => (d != null ? d.toFixed(1) : null)
      }
    })
  );
}
```
