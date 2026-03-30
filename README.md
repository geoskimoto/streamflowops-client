# StreamflowOps Client

A Python HTTP client for the [StreamflowOps DataOps API](https://streamflowops.3rdplaces.io/api/v1/docs/).  Handles authentication, pagination, and parallel data fetching.

---

## Requirements

- Python 3.10+
- `requests`
- `pandas`
- `tqdm`

---

## Installation

### Editable local install (recommended for development)

Changes to the repo are reflected immediately — no reinstall needed.

```bash
pip install -e /path/to/streamflowops-client
```

### Install directly from GitHub

```bash
pip install git+ssh://git@github.com/geoskimoto/streamflowops-client.git
```

### Install with dev dependencies (includes pytest)

```bash
pip install -e "/path/to/streamflowops-client[dev]"
```

---

## Configuration

The client reads connection settings from a `config` module that exposes a `cfg` object with the following attributes:

| Attribute | Description |
|---|---|
| `cfg.api_base_url` | API root, e.g. `https://streamflowops.3rdplaces.io/api/v1` |
| `cfg.api_token` | Token Auth credential |
| `cfg.page_size` | Records per page for paginated requests (default `100`) |
| `cfg.max_download_workers` | Threads used by `download_forecasts()` (default `4`) |

The `config` module is not bundled with the package — it is the user-supplied credential and settings layer that lives in your project directory.  Create a `config.py` alongside your script:

```python
# config.py  (in your project directory, not committed to version control)
from types import SimpleNamespace

cfg = SimpleNamespace(
    api_base_url="https://streamflowops.3rdplaces.io/api/v1",
    api_token="your-token-here",
    page_size=100,           # records per page; server max is 1000
    max_download_workers=4,  # parallel threads for download_forecasts()
)
```

You can also pass `base_url` and `api_token` directly to the constructor to override config values:

```python
from client import StreamflowOpsClient

client = StreamflowOpsClient(
    base_url="https://streamflowops.3rdplaces.io/api/v1",
    api_token="your-token-here",
)
```

---

## Quick Start

```python
from client import StreamflowOpsClient

client = StreamflowOpsClient()

# List all stations
stations = client.list_stations()
print(stations[0])
# {'station_number': 'MRGO1', 'name': 'Marys River at Philomath', ...}

# Get the latest forecast across all stations
latest = client.get_latest_forecast()
print(latest["run_date"], latest["station"])
```

---

## Reference

### Stations

#### List stations

```python
# All stations
stations = client.list_stations()

# Filter by agency
usgs_stations = client.list_stations(agency="USGS")
ec_stations   = client.list_stations(agency="EC")
rfc_stations  = client.list_stations(agency="NOAA_RFC")

# Filter by state and active status
oregon_active = client.list_stations(state="OR", is_active=True)

# Filter by HUC code
huc_stations = client.list_stations(huc_code="17090011")

# Full-text search
results = client.list_stations(search="Willamette")

# Control sort order
sorted_stations = client.list_stations(ordering="station_number")
```

#### Get a single station

```python
station = client.get_station("MRGO1")
print(station["name"], station["latitude"], station["longitude"])
```

#### Station statistics

```python
stats = client.get_station_statistics("MRGO1")
# Returns observation counts, date ranges, and data availability
print(stats)
```

#### Stations grouped by region

```python
by_state = client.get_stations_by_region(group_by="state")
by_huc   = client.get_stations_by_region(group_by="huc")
```

---

### Master Stations (cross-network ID reference)

The master station table maps stations across networks — USGS gauge IDs, NOAA Location IDs (LIDs), and RFC codes all on a single record.  Useful for translating IDs on the fly in analysis workflows.

#### Resolve any station ID to all network IDs

```python
# Look up by NOAA LID
ids = client.lookup_station_ids("PATW1")
print(ids["station_number"])  # '12149000'
print(ids["rfc_code"])        # 'NWRFC'

# Look up by USGS gauge number — same result
ids = client.lookup_station_ids("12149000")
print(ids["noaa_lid"])        # 'PATW1'
```

Response fields: `station_number`, `noaa_lid`, `rfc_code`, `station_name`,
`agency`, `state_code`, `huc_code`, `latitude`, `longitude`, `altitude_ft`,
`drainage_area_sqmi`.

Raises `requests.HTTPError` (404) if no station matches the supplied ID.

#### Browse the full reference table

```python
# All master stations (paginated)
all_stations = client.list_master_stations()

# Filter by RFC and state
nwrfc_wa = client.list_master_stations(rfc_code="NWRFC", state_code="WA")

# Full-text search across station_number, noaa_lid, station_name
results = client.list_master_stations(search="Methow")

# Retrieve a single record by primary key
record = client.get_master_station(42)
```

---

### Forecasts

#### List forecasts

```python
# All forecasts (paginated, returns list)
forecasts = client.list_forecasts()

# Filter by station
forecasts = client.list_forecasts(station_number="MRGO1")

# Filter by source and date range
forecasts = client.list_forecasts(
    source="NOAA_RFC",
    start_date="2026-01-01T00:00:00Z",
    end_date="2026-03-16T00:00:00Z",
)

# Specific run date
forecasts = client.list_forecasts(run_date="2026-03-15T12:00:00Z")
```

#### Forecasts by station (dedicated endpoint)

```python
# Returns runs most-recent-first
runs = client.list_forecasts_by_station("MRGO1")

# Narrow to a source or run date
runs = client.list_forecasts_by_station(
    "MRGO1",
    source="NOAA_RFC",
    run_date="2026-03-15T12:00:00Z",
)
```

#### Get a single forecast run (with full data array)

```python
run = client.get_forecast(99)
print(run["run_date"])
for point in run["data"]:
    print(point["date"], point["value"])
```

#### Latest forecast

```python
latest = client.get_latest_forecast()
```

#### Forecast statistics

```python
# Global aggregate
stats = client.get_forecast_statistics()
print(stats["total_runs"], stats["avg_rmse"])

# Scoped to a source and date range
stats = client.get_forecast_statistics(
    source="NOAA_RFC",
    start_date="2026-01-01T00:00:00Z",
    end_date="2026-03-16T00:00:00Z",
)
```

#### Download forecasts as a DataFrame

`download_forecasts()` fetches all runs for a station in parallel and returns a flat `pandas.DataFrame`.

```python
df = client.download_forecasts("MRGO1")
# Columns: station_number, run_date, lead_date, lead_day, forecast_value_cfs
print(df.head())

# Skip already-cached run IDs
cached_ids = {1, 2, 3, 44}
df_new = client.download_forecasts("MRGO1", known_ids=cached_ids)
```

#### Fetch a single forecast by run date

```python
import pandas as pd

df = client.fetch_forecast_for_run_date(
    station_number="MRGO1",
    run_date=pd.Timestamp("2026-03-15"),
)
# Columns: station_number, run_date, lead_date, lead_day, forecast_value_cfs
print(df)
```

---

### Observations

#### List discharge observations

```python
# All observations (paginated)
obs = client.list_observations()

# Filter by data type
daily  = client.list_observations(type="daily_mean")
rt15   = client.list_observations(type="realtime_15min")

# Filter by quality code
approved    = client.list_observations(quality_code="A")
provisional = client.list_observations(quality_code="P")

# Request a specific unit
obs_cms = client.list_observations(unit="cms")
obs_cfs = client.list_observations(unit="cfs")

# Combine filters
daily_approved = client.list_observations(type="daily_mean", quality_code="A")
```

#### Get a single observation

```python
obs = client.get_observation(8)
print(obs["discharge"], obs["quality_code"])
```

#### Percentile bands

```python
# Latest available date (all stations)
bands = client.get_percentile_bands()

# A specific date
bands = client.get_percentile_bands(date="2026-03-15")

# A specific date and station
bands = client.get_percentile_bands(date="2026-03-15", station="MRGO1")
```

#### Percentile date range

```python
date_range = client.get_percentile_date_range()
print(date_range["min_date"], date_range["max_date"])
```

#### Observation statistics

```python
# Global summary
stats = client.get_observation_statistics()

# Scoped
stats = client.get_observation_statistics(
    station_number="MRGO1",
    start_date="2025-01-01",
    end_date="2026-01-01",
    type="daily_mean",
)
```

#### Download observations as a DataFrame

```python
# Query by NWRFC station number
df = client.download_observations("MRGO1")

# When the observations endpoint requires a USGS gage ID
df = client.download_observations("MRGO1", usgs_id="14171000")

# Columns: station_number, usgs_id, date, discharge_cfs, quality_code
print(df.head())
```

---

### Pull Configurations

#### List configurations

```python
# All configurations
configs = client.list_configurations()

# Filter
daily_cfgs = client.list_configurations(data_type="daily_mean")
enabled    = client.list_configurations(is_enabled=True)
hourly     = client.list_configurations(schedule_type="hourly")
forecast   = client.list_configurations(
    data_type="forecast",
    is_enabled=True,
    ordering="-id",
)

# Search by name
results = client.list_configurations(search="USGS daily")
```

#### Get a single configuration

```python
cfg_detail = client.get_configuration(10)
```

#### Execution history and statistics

```python
history = client.get_configuration_execution_history(10)
stats   = client.get_configuration_statistics(10)
```

---

### Logs (Data-pull execution logs)

```python
# All logs
logs = client.list_logs()

# Filter by status
failed  = client.list_logs(status="failed")
running = client.list_logs(status="running")
success = client.list_logs(status="success")

# Filter by configuration ID
cfg_logs = client.list_logs(configuration=3)

# Single log entry
log = client.get_log(55)
print(log["status"], log.get("error_message"))
```

---

### Analytics Computations

```python
# List scheduled computations
computations = client.list_computations()
computations = client.list_computations(ordering="-id")

# Single computation (includes recent logs)
computation = client.get_computation(42)
```

#### Analytics logs

```python
logs = client.list_analytics_logs()
log  = client.get_analytics_log(7)
```

---

### Raster Configurations

```python
# List
raster_cfgs = client.list_raster_configurations()
raster_cfgs = client.list_raster_configurations(search="SNODAS")

# Single
raster_cfg = client.get_raster_configuration(3)

# Pull logs for a configuration
cfg_logs = client.get_raster_configuration_logs(3)
```

---

### Raster Datasets

```python
# List
datasets = client.list_raster_datasets()
datasets = client.list_raster_datasets(search="PRISM")

# Single
dataset = client.get_raster_dataset(2)

# Sub-resources
coverage  = client.get_raster_dataset_coverage(2)
variables = client.get_raster_dataset_variables(2)
```

---

### Raster Layers

```python
# List
layers = client.list_raster_layers()
layers = client.list_raster_layers(search="swe", ordering="-valid_time")

# Single
layer = client.get_raster_layer(11)

# Sub-resources
download  = client.get_raster_layer_download(11)   # download URL/metadata
thumbnail = client.get_raster_layer_thumbnail(11)  # thumbnail URL/metadata

# Aggregate endpoints
coverage   = client.get_raster_layers_coverage()
statistics = client.get_raster_layers_statistics()
```

---

### Raster Logs

```python
logs = client.list_raster_logs()
log  = client.get_raster_log(22)
```

---

### Raster Variables

```python
variables = client.list_raster_variables()
variable  = client.get_raster_variable(4)
print(variable["name"], variable["unit"])
```

---

### Spatial Extents

```python
extents = client.list_spatial_extents()
extent  = client.get_spatial_extent(2)
print(extent["name"], extent["bbox"])
```

---

## Pagination

All `list_*` methods transparently follow DRF `next` links and return a single flat `list`.  The `page_size` configured in `cfg` is sent on the first request; subsequent pages are fetched automatically.

```python
# Returns every station regardless of how many pages exist
all_stations = client.list_stations()
print(f"{len(all_stations)} stations retrieved")
```

---

## Error Handling

`requests.HTTPError` is raised for non-2xx responses.  A `401` produces an enhanced message that includes the token state and configured URL to aid debugging:

```python
import requests
from client import StreamflowOpsClient

client = StreamflowOpsClient(api_token="bad-token")

try:
    client.list_stations()
except requests.HTTPError as exc:
    print(exc)
    # 401 Unauthorized for https://…/stations/. Check STREAMFLOWOPS_TOKEN (currently set) …
```

---

## Running Tests

```bash
pip install pytest requests pandas tqdm
pytest
```

Tests use a fully mocked HTTP session — no network connection or API token required.

---

## API Reference

Full interactive docs: <https://streamflowops.3rdplaces.io/api/v1/docs/>  
OpenAPI schema: <https://streamflowops.3rdplaces.io/api/v1/schema/>
