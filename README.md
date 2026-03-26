# Polymarket BTC Dashboard

This is a real-time tracking dashboard for Polymarket 15-minute BTC Up/Down events.

## Features
- Connects to Polymarket's WebSocket CLOB for real-time orderbook updates.
- Records all market changes to CSV files inside the `data/` directory.
- Maintains a unified CSV file (`data/unified_events.csv`) updated every second for all events.
- Displays a live frontend UI at port 8765.

## How to Run (Docker)

To launch the project, simply have Docker installed and run:

```bash
docker-compose up -d --build
```

The dashboard will be available at `http://localhost:8765`.

To stop the dashboard, run:
```bash
docker-compose down
```

## Logs and Data

All historical pricing and tracking data is stored in the `data/` folder as CSV files.
