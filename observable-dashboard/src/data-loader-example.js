// Data loader example for Observable Framework
// This file shows how to use a data loader to query DuckDB
import * as duckdb from "duckdb";

// Connect to the database
const db = new duckdb.Database("../../dev.duckdb", { readonly: true });

// Run a query
const query = `
  SELECT 
    circuit,
    race_number,
    COUNT(DISTINCT vehicle_id) as vehicle_count,
    AVG(speed) as avg_speed
  FROM main_marts.fact_telemetry_data
  WHERE race_number IS NOT NULL AND speed IS NOT NULL
  GROUP BY circuit, race_number
  ORDER BY race_number DESC
  LIMIT 10
`;

db.all(query, (err, rows) => {
  if (err) {
    console.error(err);
    process.exit(1);
  }
  // Output JSON
  process.stdout.write(JSON.stringify(rows));
});

