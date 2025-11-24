# Toyota Gazoo Racing Analytics

This dbt project analyzes Toyota Gazoo Racing telemetry and race data.

## 📊 Interactive Dashboard

An **Observable Framework** dashboard is now available for interactive data visualization!

🚀 **Quick Start**: 
```bash
cd observable-dashboard
npm run dev
```
Then open http://localhost:3000 in your browser.

See `observable-dashboard/QUICKSTART.md` for more details.

## Project Structure

```
├── models/
│   ├── staging/       # Raw data cleaned and typed
│   ├── intermediate/  # Business logic transformations
│   └── marts/         # Final analytical models
├── analyses/          # Ad-hoc analysis queries and audits
├── seeds/             # CSV files for reference data
├── macros/            # Reusable SQL snippets
├── tests/             # Data quality tests
└── observable-dashboard/  # Interactive visualization dashboard
```

## Data Sources

The project includes race data from multiple tracks:
- Barber Motorsports Park
- Circuit of The Americas (COTA)
- Indianapolis Motor Speedway
- Road America
- Sebring International Raceway
- Sonoma Raceway
- Virginia International Raceway (VIR)

## Setup

1. Install dbt dependencies:
```bash
dbt deps
```

2. Run the project:
```bash
dbt run
```

3. Test data quality:
```bash
dbt test
```

## Database

This project uses DuckDB as the data warehouse. Connection details are in `~/.dbt/profiles.yml`.

## Visualization

### Observable Framework Dashboard

The `observable-dashboard/` directory contains an interactive web dashboard that connects directly to the DuckDB database.

Features:
- **Overview page**: Quick stats and recent race summaries
- **Telemetry Analysis**: Interactive charts for speed, RPM, throttle, and brake data
- **Race Analysis**: Driver comparisons and track-specific analytics

To use the dashboard:
1. Make sure you've run `dbt run` to build the data models
2. Navigate to the dashboard directory: `cd observable-dashboard`
3. Start the dev server: `npm run dev`
4. Open http://localhost:3000 in your browser

See `observable-dashboard/README.md` for full documentation.

