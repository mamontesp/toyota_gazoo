export default {
  title: "Toyota Gazoo Racing Analytics",
  pages: [
    {name: "Overview", path: "/index"},
    {name: "Telemetry Analysis", path: "/telemetry"},
    {name: "Race Analysis", path: "/race-analysis"},
    {name: "Lap Insights", path: "/lap-insights"},
    {name: "Wind Conditions", path: "/fact-wind"},
    {name: "Track Map", path: "/track-map"},
    {name: "Driver Consistency Analysis", path: "/driver-consistency"},
  ],
  // Path to DuckDB database
  duckdb: {
    // Use the parent directory's DuckDB database
    path: "../dev.duckdb"
  },
  // Add custom CSS
  head: '<link rel="stylesheet" type="text/css" href="./custom.css">'
};

