# Toyota Gazoo Racing Analytics Dashboard

Interactive data visualization dashboard built with [Observable Framework](https://observablehq.com/framework/) that connects to DuckDB to visualize Toyota Gazoo Racing telemetry and race data.

## Getting Started

**Prerequisites**: Node.js (v18+), dbt project with DuckDB set up

```bash
cd observable-dashboard
npm run dev
```

Access at `http://localhost:3000`

**Commands**:
- `npm run dev` - Development server with live reload
- `npm run build` - Build for production
- `npm run deploy` - Deploy to Observable

## Pages

- **Overview** - Quick stats and race summaries
- **Telemetry Analysis** - Speed, RPM, throttle, brake visualizations with filters
- **Race Analysis** - Driver performance comparisons and track analytics
- **Lap Insights** - Detailed lap timing and weather context
- **Wind Conditions** - Wind analysis and impact on performance
- **Track Map** - Track mapping and vehicle comparison for racing lines
- **Driver Consistency Analysis** - Driver performance consistency metrics

## Data Connection

Connects to `../dev.duckdb` and queries `main_marts.fact_telemetry_data`. Configuration in `observablehq.config.js`.

## Customization

### Add a New Page
1. Create `src/my-page.md`
2. Add queries and visualizations
3. Update `observablehq.config.js` navigation

### Example Query & Chart
```js
const data = await db.query(`
  SELECT lap, AVG(speed) as avg_speed
  FROM main_marts.fact_telemetry_data
  WHERE circuit = 'barber'
  GROUP BY lap
  ORDER BY lap
`);

Plot.plot({
  marks: [Plot.line(data, {x: "lap", y: "avg_speed"})]
})
```

### Styling
Edit `src/custom.css` - uses CSS custom properties for theming.

## Troubleshooting

**No data**: Run `dbt run`, verify `dev.duckdb` exists, check table exists  
**Server won't start**: Check port 3000 availability, try `npm install`  
**Queries fail**: Verify column names match schema, check browser console (F12)

## Learn More

- [Observable Framework](https://observablehq.com/framework/)
- [Observable Plot](https://observablehq.com/plot/)
- [DuckDB SQL](https://duckdb.org/docs/sql/introduction)
