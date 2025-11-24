# ✅ Observable Framework Setup Complete!

## 🎉 Your Dashboard is Live!

Your Observable Framework dashboard is now running and connected to your DuckDB database.

### 🌐 Access Your Dashboard

**URL**: http://localhost:3000

Open this link in your browser to start exploring your Toyota Gazoo Racing data!

---

## 📊 What's Included

### Three Interactive Pages

1. **Overview** (http://localhost:3000)
   - Total races, vehicles, and circuits
   - Recent race data summary table

2. **Telemetry Analysis** (http://localhost:3000/telemetry)
   - Circuit and vehicle filters
   - Speed analysis (avg, max, min by lap)
   - RPM (NMOT) analysis
   - Throttle (APS) analysis
   - Brake pressure (front & rear) analysis

3. **Race Analysis** (http://localhost:3000/race-analysis)
   - Vehicle performance comparison
   - Speed comparison bar chart
   - Speed distribution histogram
   - Gear usage analysis
   - Lap speed progression

---

## 📈 Your Current Data

**Database**: `dev.duckdb`
- **Total Records**: 4,052,738 telemetry data points
- **Circuits**: 1 (Barber Motorsports Park)
- **Vehicles**: 20 different vehicles
- **Race**: Race 1

---

## 🛠️ Key Files

```
observable-dashboard/
├── src/
│   ├── index.md           # Overview page - edit to modify homepage
│   ├── telemetry.md       # Telemetry analysis - interactive charts
│   ├── race-analysis.md   # Race analysis - vehicle comparisons
│   └── custom.css         # Styling - customize colors & layout
├── observablehq.config.js # Configuration - add pages, change title
├── package.json           # Dependencies
└── README.md             # Full documentation
```

---

## 🚀 Quick Commands

### Stop the Server
Press `Ctrl+C` in the terminal where the server is running

### Start the Server Again
```bash
cd observable-dashboard
npm run dev
```

### Build for Production
```bash
npm run build
```

---

## 📝 Database Schema

Your dashboard queries: `main_marts.fact_telemetry_data`

### Key Columns:
- `circuit` - Circuit name (e.g., 'barber')
- `race_number` - Race identifier
- `vehicle_id` - Vehicle identifier (e.g., 'GR86-004-78')
- `vehicle_number` - Vehicle number (e.g., 78)
- `lap` - Lap number
- `speed` - Vehicle speed
- `nmot` - Engine RPM
- `aps` - Throttle position (Accelerator Pedal Sensor)
- `front_brake_pressure` - Front brake pressure
- `rear_brake_pressure` - Rear brake pressure
- `gear` - Gear position
- `event_timestamp` - Timestamp of the event
- `vbox_long_minutes` - Longitude
- `vbox_lat_minutes` - Latitude
- `steering_angle` - Steering angle
- `accx_can` - X-axis acceleration
- `accy_can` - Y-axis acceleration

---

## 🎨 Customization Tips

### Change Colors
Edit `src/custom.css` and modify the `:root` variables:
```css
:root {
  --theme-foreground: #1a1a1a;
  --theme-background: #ffffff;
  --theme-accent: #e60012; /* Toyota Red */
}
```

### Add a New Chart
In any `.md` file, add:
```js
const myData = await db.query(`
  SELECT lap, AVG(speed) as avg_speed
  FROM main_marts.fact_telemetry_data
  WHERE circuit = 'barber'
  GROUP BY lap
  ORDER BY lap
`);

Plot.plot({
  marks: [
    Plot.line(myData, {x: "lap", y: "avg_speed"})
  ]
})
```

### Add a New Page
1. Create `src/my-page.md`
2. Add content with queries and visualizations
3. Update `observablehq.config.js`:
```js
pages: [
  {name: "Overview", path: "/index"},
  {name: "Telemetry Analysis", path: "/telemetry"},
  {name: "Race Analysis", path: "/race-analysis"},
  {name: "My Page", path: "/my-page"}  // Add this
]
```

---

## 🔧 Troubleshooting

### Dashboard Not Loading?
1. Check the terminal for errors
2. Make sure port 3000 is not blocked
3. Try restarting: `Ctrl+C` then `npm run dev`

### No Data Showing?
1. Verify database: `duckdb ../dev.duckdb -c "SELECT COUNT(*) FROM main_marts.fact_telemetry_data"`
2. Check browser console (F12) for SQL errors
3. Make sure you've run `dbt run` in the parent directory

### Charts Look Weird?
1. Open browser developer tools (F12)
2. Check Console tab for errors
3. Verify your SQL returns the expected columns

---

## 📚 Learn More

- **Observable Framework**: https://observablehq.com/framework/
- **Observable Plot**: https://observablehq.com/plot/
- **DuckDB SQL**: https://duckdb.org/docs/sql/introduction

---

## 🎯 Next Steps

1. **Open the dashboard**: http://localhost:3000
2. **Explore the data**: Use filters to drill down
3. **Customize**: Edit `.md` files to add your own analyses
4. **Add more data**: Run dbt models for other circuits (COTA, Indianapolis, etc.)
5. **Share**: Build for production with `npm run build`

---

## ✨ Features to Try

- Filter by different vehicles in the Telemetry Analysis page
- Compare speed distributions in Race Analysis
- Check gear usage patterns by circuit
- Analyze lap-by-lap speed progression

---

**Enjoy exploring your racing data! 🏁**

For questions or issues, see the full documentation in `README.md`.

