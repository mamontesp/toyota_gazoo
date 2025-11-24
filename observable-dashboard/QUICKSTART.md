# Quick Start Guide

## 🚀 Your Dashboard is Ready!

Your Observable Framework dashboard is now set up and connected to your DuckDB database.

### Access Your Dashboard

The development server is running at: **http://localhost:3000**

Open this URL in your web browser to view your dashboard.

### What You'll See

The dashboard includes three main pages:

1. **Overview** (Home)
   - Quick statistics about total races, drivers, and tracks
   - Recent race data summary table
   - Navigation to other pages

2. **Telemetry Analysis** (`/telemetry`)
   - Interactive filters for track and driver selection
   - Speed analysis charts (average, max, min by lap)
   - RPM analysis visualization
   - Throttle and brake pressure comparison

3. **Race Analysis** (`/race-analysis`)
   - Track selection dropdown
   - Driver performance comparison table
   - Speed comparison bar chart
   - Speed distribution histogram
   - Gear usage analysis

### How It Works

The dashboard connects directly to your DuckDB database (`../dev.duckdb`) and queries the dbt marts:
- `main_marts.fact_telemetry_data` - Main telemetry data table

All visualizations are created using **Observable Plot** and update automatically when you change filters.

### Making Changes

1. **Edit the dashboard**: Modify the `.md` files in the `src/` directory
2. **See changes live**: The page will automatically reload when you save
3. **Add new queries**: Use the `db.query()` function with SQL
4. **Customize styling**: Edit `src/custom.css`

### Example: Adding a New Chart

```js
// Query your data
const myData = await db.query(`
  SELECT 
    lap,
    AVG(speed) as speed
  FROM main_marts.fact_telemetry_data
  WHERE circuit = 'barber'
  GROUP BY lap
`);

// Create a visualization
Plot.plot({
  marks: [
    Plot.line(myData, {x: "lap", y: "speed"})
  ]
})
```

### Useful Commands

- **Stop the server**: Press `Ctrl+C` in the terminal
- **Start again**: Run `npm run dev` in the `observable-dashboard` directory
- **Build for production**: Run `npm run build`

### Next Steps

- Explore the existing visualizations
- Try changing the SQL queries to show different data
- Add new pages for specific analyses you need
- Customize colors and styling in `custom.css`

### Troubleshooting

**No data showing?**
- Make sure you've run `dbt run` to populate the database
- Check that the table `main_marts.fact_telemetry_data` exists
- The dashboard currently shows data for the 'barber' circuit

**Charts look weird?**
- Check the browser console (F12) for errors
- Verify your SQL queries return the expected columns

**Need help?**
- Check the main README.md for more details
- Visit [Observable Framework docs](https://observablehq.com/framework/)

---

**Happy visualizing! 🎉**

