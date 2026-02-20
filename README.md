$ Installation

Install on server:
```
git clone https://github.com/aiditz/centrifugo-ws-nats-test.git
cd centrifugo-ws-nats-test
docker compose build && docker compose up -d
```

Run load client (windows): `./run_win.bat` (edit it to set env vars)

# Set up Grafana
1. http://localhost:3000 (admin/admin)
1. Connections → Data Sources → Add data source
1. Select "Prometheus"
1. Enter URL: http://prometheus:9090
1. Click "Save & Test"
1. Dashboards → New → Import
1. Enter ID: 13039
1. Click "Load"
1. Select "Prometheus data source"
