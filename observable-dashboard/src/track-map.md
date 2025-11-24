# Track Map Analysis

Advanced track mapping and vehicle comparison for understanding driver behavior and racing lines.

## Filters

```js
// Import DuckDB client
import {DuckDBClient} from "npm:@observablehq/duckdb";

// Connect to DuckDB database
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
const selectedCircuit = view(Inputs.select(circuits.map(d => d.circuit), {
  label: "Circuit",
  value: circuits[0]?.circuit
}));
```

```js
// Get available vehicles for the selected circuit
const vehiclesResult = await db.sql`
  SELECT DISTINCT vehicle_id 
  FROM database.main_marts.fact_telemetry_data 
  WHERE circuit = ${selectedCircuit}
    AND vehicle_id IS NOT NULL
  ORDER BY vehicle_id
`;

const allVehicles = Array.from(vehiclesResult);
const vehicleIds = allVehicles.map(d => d.vehicle_id);
```

```js
// Use checkboxes for better multi-select experience
const selectedVehicles = view(Inputs.checkbox(vehicleIds, {
  label: "Select Vehicles (select up to 3)",
  value: vehicleIds.slice(0, 3)
}));
```

```js
// Limit to 3 vehicles
const limitedVehicles = (selectedVehicles || []).slice(0, 3);
```

```js
// Get available laps
const lapsResult = await db.sql`
  SELECT DISTINCT lap 
  FROM database.main_marts.fact_telemetry_data 
  WHERE circuit = ${selectedCircuit}
    AND lap IS NOT NULL
  ORDER BY lap
`;

const laps = Array.from(lapsResult);
const selectedLap = view(Inputs.select(laps.map(d => d.lap), {
  label: "Lap Number",
  value: laps[0]?.lap
}));
```

## Racing Lines Comparison

```js
html`<div style="background: #f0f0f0; padding: 1rem; border-radius: 8px; margin: 1rem 0;">
  <strong>Selected:</strong> ${limitedVehicles.length} vehicle(s) - ${limitedVehicles.join(', ')}
  ${limitedVehicles.length > 3 ? '<br><span style="color: #e60012;">⚠️ Only first 3 vehicles will be shown</span>' : ''}
</div>`
```

```js
// Get track data for selected vehicles - works with 1, 2, or 3 vehicles
const trackData = await (async () => {
  if (!limitedVehicles || limitedVehicles.length === 0) {
    display(html`<div style="background: #fff3cd; padding: 1rem; border-radius: 8px; border: 1px solid #ffc107;">
      ⚠️ No vehicles selected! Please select at least one vehicle above.
    </div>`);
    return [];
  }

  const vehicleFilter = limitedVehicles.map(v => `vehicle_id = '${v}'`).join(' OR ');

  const trackDataQuery = `
    SELECT 
      vbox_long_minutes,
      vbox_lat_minutes,
      speed,
      vehicle_id,
      lap,
      event_timestamp
    FROM database.main_marts.fact_telemetry_data
    WHERE circuit = '${selectedCircuit}'
      AND lap = ${selectedLap}
      AND (${vehicleFilter})
      AND vbox_long_minutes IS NOT NULL
      AND vbox_lat_minutes IS NOT NULL
    ORDER BY vehicle_id, event_timestamp
  `;

  display("Debug - Query:", trackDataQuery);

  const trackDataResult = await db.query(trackDataQuery);
  return Array.from(trackDataResult);
})();

display(`✓ Loaded ${trackData.length} data points for ${limitedVehicles.length} vehicle(s)`);

// If no data, check each vehicle individually
if (trackData.length === 0) {
  for (const vehicle of limitedVehicles) {
    const testQuery = `
      SELECT COUNT(*) as count
      FROM database.main_marts.fact_telemetry_data
      WHERE circuit = '${selectedCircuit}'
        AND lap = ${selectedLap}
        AND vehicle_id = '${vehicle}'
        AND vbox_long_minutes IS NOT NULL
        AND vbox_lat_minutes IS NOT NULL
    `;
    const testResult = await db.query(testQuery);
    const count = Array.from(testResult)[0].count;
    display(`Vehicle ${vehicle}: ${count} GPS records for lap ${selectedLap}`);
  }
}
```

```js
// Show Racing Lines if we have data
if (trackData && trackData.length > 0) {
  display(Plot.plot({
    title: `Racing Lines - ${selectedCircuit} (Lap ${selectedLap})`,
    width: 1200,
    height: 900,
    x: {label: "Longitude", grid: true},
    y: {label: "Latitude", grid: true},
    color: {
      legend: true,
      label: "Vehicle",
      domain: limitedVehicles,
      range: ["#e60012", "#0066cc", "#00cc66"].slice(0, limitedVehicles.length)
    },
    marks: [
      Plot.line(trackData, {
        x: "vbox_long_minutes",
        y: "vbox_lat_minutes",
        z: "vehicle_id",
        stroke: "vehicle_id",
        strokeWidth: 3,
        opacity: 0.8
      }),
      Plot.dot(trackData, {
        x: "vbox_long_minutes",
        y: "vbox_lat_minutes",
        fill: "vehicle_id",
        r: 1.5,
        opacity: 0.4
      })
    ]
  }));
} else {
  display(html`<div style="background: #fff3cd; padding: 1.5rem; border-radius: 8px; border: 1px solid #ffc107;">
    ⚠️ No data to display. Please select at least one vehicle above.
  </div>`);
}
```

## Speed Heat Map Overlay

```js
// Only show if we have data
if (trackData && trackData.length > 0) {
  // Map vehicle IDs to symbols
  const vehicleSymbols = ["circle", "square", "triangle"];
  const symbolScale = Object.fromEntries(
    limitedVehicles.map((v, i) => [v, vehicleSymbols[i % vehicleSymbols.length]])
  );
  
  display(html`<div style="background: #f0f0f0; padding: 1rem; border-radius: 8px; margin: 1rem 0;">
    <strong>Vehicle Symbols:</strong>
    ${limitedVehicles.map((v, i) => `<span style="margin-right: 1.5rem;">● ${vehicleSymbols[i]} = ${v}</span>`).join('')}
    <br><strong>Colors:</strong> Blue = Slow → Green/Yellow = Medium → Red = Fast
  </div>`);
  
  display(Plot.plot({
    title: `Vehicle Comparison with Speed Heat Map - ${selectedCircuit} (Lap ${selectedLap})`,
    subtitle: "Shape = Vehicle | Color = Speed",
    width: 1200,
    height: 900,
    x: {label: "Longitude", grid: true},
    y: {label: "Latitude", grid: true},
    color: {
      type: "linear",
      scheme: "turbo",
      label: "Speed (mph)",
      legend: true
    },
    symbol: {
      legend: true,
      label: "Vehicle",
      domain: limitedVehicles
    },
    marks: [
      // Track outline for context
      Plot.line(trackData, {
        x: "vbox_long_minutes",
        y: "vbox_lat_minutes",
        z: "vehicle_id",
        stroke: "#999999",
        strokeWidth: 1,
        opacity: 0.3
      }),
      // Main visualization: shapes by vehicle, color by speed
      Plot.dot(trackData, {
        x: "vbox_long_minutes",
        y: "vbox_lat_minutes",
        fill: "speed",
        symbol: "vehicle_id",
        r: 5,
        stroke: "white",
        strokeWidth: 0.5,
        opacity: 0.8,
        tip: {
          format: {
            x: false,
            y: false,
            fill: false,
            symbol: false,
            vehicle_id: true,
            speed: d => `${d.toFixed(1)} mph`
          }
        }
      })
    ]
  }));
} else {
  display(html`<div style="background: #fff3cd; padding: 2rem; border-radius: 12px; border: 1px solid #ffc107; text-align: center;">
    <h3 style="color: #856404; margin-top: 0;">⚠️ No Data Available</h3>
    <p>No track data loaded. Please ensure:</p>
    <ul style="text-align: left; max-width: 600px; margin: 0 auto;">
      <li>You have selected at least one vehicle using the checkboxes above</li>
      <li>The selected lap has GPS data</li>
      <li>The vehicles you selected have data for this lap</li>
    </ul>
  </div>`);
}
```

## Speed Comparison by Distance

```js
// Calculate distance along track (approximate)
const trackDataWithDistance = trackData.map((d, i, arr) => {
  if (i === 0) return {...d, distance: 0};
  const prev = arr[i - 1];
  if (d.vehicle_id !== prev.vehicle_id) return {...d, distance: 0};
  
  // Simple euclidean distance
  const latDiff = (d.vbox_lat_minutes - prev.vbox_lat_minutes) * 60;
  const lonDiff = (d.vbox_long_minutes - prev.vbox_long_minutes) * 60;
  const segmentDist = Math.sqrt(latDiff * latDiff + lonDiff * lonDiff);
  
  return {...d, distance: prev.distance + segmentDist};
});

Plot.plot({
  title: `Speed vs Track Position - ${selectedCircuit} (Lap ${selectedLap})`,
  width: 1200,
  height: 400,
  x: {label: "Distance Along Track", grid: true},
  y: {label: "Speed (mph)", grid: true},
  color: {
    legend: true,
    label: "Vehicle"
  },
  marks: [
    Plot.line(trackDataWithDistance, {
      x: "distance",
      y: "speed",
      stroke: "vehicle_id",
      strokeWidth: 2,
      tip: true
    }),
    Plot.ruleY([0])
  ]
})
```

## Braking Points Analysis

```js
// Identify braking zones using a lookback window for better detection
const brakingZones = [];
const BRAKING_THRESHOLD = 8; // mph threshold for braking detection
const LOOKBACK_POINTS = 30; // Compare speed across more points to capture real braking zones

for (let i = LOOKBACK_POINTS; i < trackData.length; i++) {
  const curr = trackData[i];
  const lookback = trackData[i - LOOKBACK_POINTS];
  
  // Only compare points from the same vehicle
  if (curr.vehicle_id === lookback.vehicle_id) {
    const speedDrop = lookback.speed - curr.speed;
    
    // Detect deceleration above threshold
    if (speedDrop > BRAKING_THRESHOLD) {
      brakingZones.push({
        vehicle_id: curr.vehicle_id,
        vbox_long_minutes: curr.vbox_long_minutes,
        vbox_lat_minutes: curr.vbox_lat_minutes,
        speed_drop: speedDrop,
        initial_speed: lookback.speed,
        final_speed: curr.speed,
        deceleration_rate: speedDrop / LOOKBACK_POINTS  // mph per sample point
      });
    }
  }
}

display(`Found ${brakingZones.length} braking points (threshold: ${BRAKING_THRESHOLD} mph over ${LOOKBACK_POINTS} samples)`);

// Calculate color scale domain for better heat map visibility
if (brakingZones.length > 0) {
  const speedDrops = brakingZones.map(d => d.speed_drop);
  const minSpeedDrop = Math.min(...speedDrops);
  const maxSpeedDrop = Math.max(...speedDrops);
  const range = maxSpeedDrop - minSpeedDrop;
  
  display(`Speed drops range: ${minSpeedDrop.toFixed(1)} - ${maxSpeedDrop.toFixed(1)} mph (range: ${range.toFixed(1)} mph)`);
  
  // Show distribution of braking intensities
  const lightBraking = speedDrops.filter(d => d < 20).length;
  const mediumBraking = speedDrops.filter(d => d >= 20 && d < 40).length;
  const heavyBraking = speedDrops.filter(d => d >= 40).length;
  display(`Distribution: ${lightBraking} light (<20), ${mediumBraking} medium (20-40), ${heavyBraking} heavy (>40)`);
  
  // Warn if range is too narrow for good visualization
  if (range < 10) {
    display(html`<div style="background: #fff3cd; padding: 0.75rem; border-radius: 6px; border-left: 4px solid #ffc107; margin: 0.5rem 0;">
      ⚠️ <strong>Narrow data range detected (${range.toFixed(1)} mph).</strong> Color variation may be subtle. The visualization uses enhanced contrast to maximize visibility.
    </div>`);
  }
} else {
  display(`⚠️ No braking points detected. Try adjusting the threshold or selecting different vehicles.`);
}
```

```js
// Only show if we have braking data
if (brakingZones && brakingZones.length > 0) {
  // Get min/max for color scale
  const speedDrops = brakingZones.map(d => d.speed_drop);
  const minSpeedDrop = Math.min(...speedDrops);
  const maxSpeedDrop = Math.max(...speedDrops);
  const range = maxSpeedDrop - minSpeedDrop;
  
  // Use a high-contrast color scheme for narrow ranges
  const colorScheme = range < 10 ? "RdYlGn" : "YlOrRd";
  const colorType = range < 10 ? "quantile" : "linear";  // Quantile spreads colors evenly
  
  display(Plot.plot({
    title: `Braking Analysis - ${selectedCircuit} (Lap ${selectedLap})`,
    subtitle: `Shape = Vehicle | Color = Braking Intensity (${range < 10 ? 'Enhanced Contrast Mode' : 'Heat Map: Yellow→Orange→Red'}) | Size = Speed Drop`,
    width: 1200,
    height: 900,
    x: {label: "Longitude", grid: true},
    y: {label: "Latitude", grid: true},
    color: {
      type: colorType,
      scheme: colorScheme,
      label: "Speed Drop (mph)",
      legend: true,
      domain: colorType === "linear" ? [minSpeedDrop, maxSpeedDrop] : undefined,
      reverse: range < 10  // Reverse for RdYlGn so red = more braking
    },
    symbol: {
      legend: true,
      label: "Vehicle",
      domain: limitedVehicles
    },
    marks: [
      // Track outline - thicker and darker
      Plot.line(trackData, {
        x: "vbox_long_minutes",
        y: "vbox_lat_minutes",
        z: "vehicle_id",
        stroke: "#666666",
        strokeWidth: 3,
        opacity: 0.8
      }),
      // Braking points with shapes, color heat map, and size
      Plot.dot(brakingZones, {
        x: "vbox_long_minutes",
        y: "vbox_lat_minutes",
        fill: "speed_drop",  // Heat map color
        symbol: "vehicle_id",  // Different shapes per vehicle
        r: 10,  // Fixed size for clarity
        stroke: "white",
        strokeWidth: 2,
        opacity: 0.95,
        tip: {
          format: {
            x: false,
            y: false,
            fill: false,
            symbol: false,
            vehicle_id: true,
            speed_drop: d => `${d.toFixed(1)} mph drop`,
            initial_speed: d => `${d.toFixed(1)} mph`,
            final_speed: d => `${d.toFixed(1)} mph`
          }
        }
      })
    ]
  }));
  
  // Add explanation
  if (range < 10) {
    display(html`<div style="background: #f0f9ff; padding: 1rem; margin-top: 1rem; border-radius: 8px; border-left: 4px solid #0284c7;">
      <strong>💡 Reading the Braking Heat Map (Enhanced Contrast Mode):</strong><br>
      • <strong>Color (Green → Yellow → Red):</strong> Light to heavy braking - using enhanced contrast for narrow data range<br>
      • <strong>Shape:</strong> Different symbol for each vehicle (circle, square, triangle)<br>
      • <strong>Note:</strong> Data range is ${range.toFixed(1)} mph - colors are distributed evenly to maximize visibility<br>
      • Hover over dots to see exact speed drop values
    </div>`);
  } else {
    display(html`<div style="background: #f0f9ff; padding: 1rem; margin-top: 1rem; border-radius: 8px; border-left: 4px solid #0284c7;">
      <strong>💡 Reading the Braking Heat Map:</strong><br>
      • <strong>Color (Yellow → Orange → Red):</strong> Light braking to heavy braking intensity<br>
      • <strong>Shape:</strong> Different symbol for each vehicle (circle, square, triangle)<br>
      • Hover over dots to see exact speed drop values
    </div>`);
  }
} else {
  display(html`<div style="background: #fff3cd; padding: 1rem; border-radius: 8px; border: 1px solid #ffc107;">
    ⚠️ No significant braking points detected (speed drops > 10 mph). Try selecting a different lap or vehicles.
  </div>`);
}
```

## Vehicle Statistics Comparison

```js
// Calculate statistics for each vehicle
const vehicleStats = limitedVehicles.map(vehicleId => {
  const vehicleData = trackData.filter(d => d.vehicle_id === vehicleId);
  
  if (vehicleData.length === 0) return null;
  
  const speeds = vehicleData.map(d => d.speed);
  const avgSpeed = speeds.reduce((a, b) => a + b, 0) / speeds.length;
  const maxSpeed = Math.max(...speeds);
  const minSpeed = Math.min(...speeds);
  
  return {
    vehicle_id: vehicleId,
    avg_speed: avgSpeed,
    max_speed: maxSpeed,
    min_speed: minSpeed,
    data_points: vehicleData.length
  };
}).filter(d => d !== null);

display(Inputs.table(vehicleStats, {
  columns: [
    "vehicle_id",
    "avg_speed",
    "max_speed",
    "min_speed",
    "data_points"
  ],
  header: {
    vehicle_id: "Vehicle ID",
    avg_speed: "Average Speed",
    max_speed: "Max Speed",
    min_speed: "Min Speed",
    data_points: "Data Points"
  },
  format: {
    avg_speed: d => d.toFixed(2),
    max_speed: d => d.toFixed(2),
    min_speed: d => d.toFixed(2)
  }
}))
```

## Key Insights

```js
html`
<div style="background: linear-gradient(135deg, #ffffff 0%, #f8f9fa 100%); padding: 2rem; border-radius: 12px; margin: 2rem 0; border: 1px solid #e0e0e0;">
  <h3 style="color: #e60012; margin-top: 0;">How to Read These Charts</h3>
  <ul style="line-height: 1.8;">
    <li><strong>Racing Lines:</strong> Shows the exact path each driver takes around the track. Tighter lines may indicate different racing strategies.</li>
    <li><strong>Speed Heat Map:</strong> Blue = slow, Red = fast. Helps identify braking zones (blue) and straightaways (red).</li>
    <li><strong>Speed vs Position:</strong> Compare how each driver's speed varies around the lap. Look for differences in braking and acceleration points.</li>
    <li><strong>Braking Points:</strong> Larger dots = harder braking. Different braking points can reveal different driving styles and risk tolerance.</li>
  </ul>
</div>
`
```

