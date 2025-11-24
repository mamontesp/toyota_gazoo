# Toyota Gazoo Racing Analytics Dashboard

An interactive data visualization dashboard built with [Observable Framework](https://observablehq.com/framework/) that connects to your DuckDB database to visualize Toyota Gazoo Racing telemetry and race data.

## Features

- **Overview Dashboard**: Quick stats and recent race summaries
- **Telemetry Analysis**: Interactive visualizations of speed, RPM, throttle, and brake data
- **Race Analysis**: Driver performance comparisons and track-specific analytics
- **Direct DuckDB Integration**: Queries data directly from your `dev.duckdb` database

## Getting Started

### Prerequisites

- Node.js (v18 or higher)
- Your dbt project with DuckDB database already set up

### Installation

The Observable Framework is already installed. To start the development server:

```bash
cd observable-dashboard
npm run dev
```

This will start a local development server at `http://localhost:3000` where you can view and interact with your dashboard.

### Available Commands

- `npm run dev` - Start development server with live reload
- `npm run build` - Build static site for production
- `npm run deploy` - Deploy to Observable (requires authentication)

## Project Structure

```
observable-dashboard/
├── src/
│   ├── index.md              # Overview page
│   ├── telemetry.md          # Telemetry analysis page
│   ├── race-analysis.md      # Race analysis page
│   └── custom.css            # Custom styling
├── observablehq.config.js    # Observable Framework configuration
├── package.json              # Node.js dependencies
└── README.md                 # This file
```

## Connecting to Your Data

The dashboard connects to the DuckDB database at `../dev.duckdb` (relative to the observable-dashboard folder). The configuration is set in `observablehq.config.js`.

All queries use the dbt marts schema: `main_marts.fact_telemetry_data`

## Customization

### Adding New Pages

1. Create a new `.md` file in the `src/` directory
2. Add SQL queries using the DuckDB connection
3. Create visualizations using Observable Plot
4. Update `observablehq.config.js` to add the page to navigation

### Modifying Queries

Edit the SQL queries in the markdown files. All queries use standard SQL syntax and can access any tables in your DuckDB database.

Example query:
```js
const data = await db.query(`
  SELECT 
    vehicle_id,
    AVG(speed) as avg_speed
  FROM main_marts.fact_telemetry_data
  WHERE circuit = 'barber'
  GROUP BY vehicle_id
`);
```

### Styling

Edit `src/custom.css` to customize colors, fonts, and layout. The dashboard uses CSS custom properties for easy theming.

## Data Requirements

The dashboard expects the following columns in `main_marts.fact_telemetry_data`:
- `race_number` - Race identifier
- `circuit` - Circuit/track name
- `vehicle_id` - Vehicle identifier
- `vehicle_number` - Vehicle number
- `lap` - Lap number
- `speed` - Vehicle speed
- `nmot` - Engine RPM
- `aps` - Throttle position (Accelerator Pedal Sensor)
- `front_brake_pressure` - Front brake pressure
- `rear_brake_pressure` - Rear brake pressure
- `gear` - Gear position

Ensure your dbt models have created this table with these columns.

## Troubleshooting

### Dashboard shows no data
- Make sure you've run `dbt run` to build your data models
- Check that `dev.duckdb` exists in the parent directory
- Verify the table `main_marts.fact_telemetry_data` exists in your database
- Run `duckdb ../dev.duckdb -c "SELECT COUNT(*) FROM main_marts.fact_telemetry_data"` to check if data exists

### Server won't start
- Make sure port 3000 is not already in use
- Try deleting `node_modules` and running `npm install` again

### Queries fail
- Check the column names in your queries match your actual database schema
- Use `dbt docs generate` to see your model documentation

## Learn More

- [Observable Framework Documentation](https://observablehq.com/framework/)
- [Observable Plot Documentation](https://observablehq.com/plot/)
- [DuckDB SQL Reference](https://duckdb.org/docs/sql/introduction)

## Support

For issues with:
- **Observable Framework**: Check the [Observable Framework docs](https://observablehq.com/framework/)
- **dbt models**: Check the main project README
- **DuckDB**: Check [DuckDB documentation](https://duckdb.org/docs/)

