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

## Project Structure

```
├── models/
│   ├── 1_raw/         # Raw data cleaned and typed
│   ├── 2_intermediate/  # Business logic transformations
│   └── 3_marts/       # Final analytical models
├── analyses/          # Ad-hoc analysis queries and audits
├── data_files/        # Raw CSV files organized by track/race
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

### Downloading Data Files

**You need to download the data files yourself** before running the project. Data files are available from the [Toyota Racing Development Hackathon 2025](https://trddev.com/hackathon-2025/) website.

1. Visit https://trddev.com/hackathon-2025/
2. Download the zip files for each track you want to analyze:
   - `barber-motorsports-park.zip`
   - `circuit-of-the-americas.zip`
   - `indianapolis.zip`
   - `road-america.zip`
   - `sebring.zip`
   - `sonoma.zip`
   - `virginia-international-raceway.zip`
3. Extract the zip files into the `data_files/` folder, maintaining the track-specific directory structure
4. The extracted files should be organized by track and race (e.g., `data_files/COTA/Race 1/`, `data_files/COTA/Race 2/`)

**Note:** Track maps are also available on the same website and correspond to the "analysis with sections" files in the data. Sections on the map are split up by red lines and the start/finish line.

## Setup

1. **Download and extract data files** (see [Data Sources](#data-sources) section above for detailed instructions):
   - Download the zip files from https://trddev.com/hackathon-2025/
   - Extract them into the `data_files/` folder, maintaining the track-specific directory structure

2. Install dbt dependencies:
```bash
dbt deps
```

3. Run the project:
```bash
dbt run
```

4. Test data quality:
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

