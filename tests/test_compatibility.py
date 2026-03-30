"""Compatibility tests: StreamflowOpsClient ↔ StreamflowOps Django API.

These tests verify that the client's URL paths, query parameter names, and
response-field expectations are aligned with the actual server-side code.

All HTTP is mocked – no live server required.  Where a known mismatch exists
between client and server it is called out with a clear comment and the test
asserts the current (broken) behaviour so the mismatch is not silently missed.

Server-side reference points:
  apps/api/urls.py               – registered router prefixes and action paths
  apps/api/views/                – filterset_fields, custom query-param handling
  apps/api/serializers/          – response field names
  apps/api/pagination.py         – StandardResultsSetPagination (page_size_query_param = 'limit')
"""

from __future__ import annotations

import pytest

from helpers import BASE_URL, make_paginated, make_response


# ---------------------------------------------------------------------------#
# Realistic server payloads (match actual serializer field names)            #
# ---------------------------------------------------------------------------#

# DischargeObservationSerializer fields: id, station, station_number,
#   observed_at, discharge, unit, type, quality_code
OBS_RECORD = {
    "id": 1,
    "station": 42,
    "station_number": "14103000",
    "observed_at": "2026-01-15T00:00:00Z",
    "discharge": 3450.0,
    "unit": "cfs",
    "type": "daily_mean",
    "quality_code": "A",
}

# ForecastRunListSerializer fields: id, station_number, station_name,
#   source, run_date, rmse, forecast_point_count
FORECAST_LIST_RECORD = {
    "id": 99,
    "station_number": "MRGO1",
    "station_name": "Marys River at Philomath",
    "source": "NOAA_RFC",
    "run_date": "2026-03-15T12:00:00Z",
    "rmse": 0.08,
    "forecast_point_count": 10,
}

# ForecastRunSerializer fields (detail): adds 'station', 'data'
FORECAST_DETAIL_RECORD = {
    "id": 99,
    "station": 7,
    "station_number": "MRGO1",
    "station_name": "Marys River at Philomath",
    "source": "NOAA_RFC",
    "run_date": "2026-03-15T12:00:00Z",
    "rmse": 0.08,
    "forecast_point_count": 3,
    "data": [
        {"date": "2026-03-16T00:00:00Z", "value": 1100.0},
        {"date": "2026-03-17T00:00:00Z", "value": 1050.0},
        {"date": "2026-03-18T00:00:00Z", "value": 980.0},
    ],
}

# StationListSerializer fields: id, station_number, name, agency,
#   latitude, longitude, is_active
STATION_RECORD = {
    "id": 7,
    "station_number": "MRGO1",
    "name": "Marys River at Philomath",
    "agency": "NOAA_RFC",
    "latitude": 44.5401,
    "longitude": -123.3696,
    "is_active": True,
}

# PullConfigurationSerializer fields: id, name, description, data_source,
#   data_type, data_strategy, pull_start_date, is_enabled, schedule_type,
#   schedule_value, created_at, updated_at, station_count
CONFIG_RECORD = {
    "id": 1,
    "name": "USGS Daily OR",
    "description": "Daily discharge for Oregon USGS stations",
    "data_source": "USGS",
    "data_type": "daily_mean",
    "data_strategy": "append",
    "pull_start_date": "2020-01-01",
    "is_enabled": True,
    "schedule_type": "daily",
    "schedule_value": "0 6 * * *",
    "created_at": "2025-01-01T00:00:00Z",
    "updated_at": "2026-01-01T00:00:00Z",
    "station_count": 5,
}

# DataPullLogListSerializer fields: id, configuration, configuration_name,
#   status, records_processed, start_time, end_time
LOG_RECORD = {
    "id": 55,
    "configuration": 1,
    "configuration_name": "USGS Daily OR",
    "status": "success",
    "records_processed": 1200,
    "start_time": "2026-03-29T06:00:00Z",
    "end_time": "2026-03-29T06:02:31Z",
}


# ===========================================================================
# URL Routing Compatibility
# Server router prefixes (apps/api/urls.py):
#   r'stations'                  → /stations/
#   r'configurations'            → /configurations/
#   r'observations/discharge'    → /observations/discharge/
#   r'forecasts'                 → /forecasts/
#   r'logs'                      → /logs/
#   r'raster-datasets'           → /raster-datasets/
#   r'raster-variables'          → /raster-variables/
#   r'spatial-extents'           → /spatial-extents/
#   r'raster-layers'             → /raster-layers/
#   r'raster-configurations'     → /raster-configurations/
#   r'raster-logs'               → /raster-logs/
#   r'analytics/computations'    → /analytics/computations/
#   r'analytics/logs'            → /analytics/logs/
# Custom actions:
#   ForecastRunViewSet.by_station  url_path='by-station/(?P<station_number>[^/.]+)'
#   ForecastRunViewSet.latest      url_path='latest'
#   ForecastRunViewSet.statistics  → /forecasts/statistics/
#   StationViewSet.by_region       → /stations/by_region/
#   DischargeObservationViewSet.statistics       → /observations/discharge/statistics/
#   DischargeObservationViewSet.percentile_bands → /observations/discharge/percentile-bands/
#   DischargeObservationViewSet.percentile_date_range → /observations/discharge/percentile-date-range/
#   PullConfigurationViewSet.execution_history   → /configurations/{pk}/execution_history/
#   PullConfigurationViewSet.statistics          → /configurations/{pk}/statistics/
# ===========================================================================


class TestUrlRouting:
    """Client URLs match the server's registered route patterns."""

    def test_list_stations_url(self, client) -> None:
        client.session.get.return_value = make_response(make_paginated([]))
        client.list_stations()
        assert client.session.get.call_args[0][0] == f"{BASE_URL}/stations/"

    def test_get_station_url(self, client) -> None:
        client.session.get.return_value = make_response(STATION_RECORD)
        client.get_station("MRGO1")
        assert client.session.get.call_args[0][0] == f"{BASE_URL}/stations/MRGO1/"

    def test_get_station_statistics_url(self, client) -> None:
        client.session.get.return_value = make_response({})
        client.get_station_statistics("MRGO1")
        assert client.session.get.call_args[0][0] == f"{BASE_URL}/stations/MRGO1/statistics/"

    def test_get_stations_by_region_url(self, client) -> None:
        # Server action url: /stations/by_region/ (underscore, not hyphen)
        client.session.get.return_value = make_response({})
        client.get_stations_by_region()
        assert client.session.get.call_args[0][0] == f"{BASE_URL}/stations/by_region/"

    def test_list_configurations_url(self, client) -> None:
        client.session.get.return_value = make_response(make_paginated([]))
        client.list_configurations()
        assert client.session.get.call_args[0][0] == f"{BASE_URL}/configurations/"

    def test_get_configuration_url(self, client) -> None:
        client.session.get.return_value = make_response({})
        client.get_configuration(10)
        assert client.session.get.call_args[0][0] == f"{BASE_URL}/configurations/10/"

    def test_get_configuration_execution_history_url(self, client) -> None:
        client.session.get.return_value = make_response({})
        client.get_configuration_execution_history(10)
        assert client.session.get.call_args[0][0] == f"{BASE_URL}/configurations/10/execution_history/"

    def test_get_configuration_statistics_url(self, client) -> None:
        client.session.get.return_value = make_response({})
        client.get_configuration_statistics(10)
        assert client.session.get.call_args[0][0] == f"{BASE_URL}/configurations/10/statistics/"

    def test_list_observations_url(self, client) -> None:
        # Server: router.register(r'observations/discharge', ...)
        client.session.get.return_value = make_response(make_paginated([]))
        client.list_observations()
        assert client.session.get.call_args[0][0] == f"{BASE_URL}/observations/discharge/"

    def test_get_observation_url(self, client) -> None:
        client.session.get.return_value = make_response(OBS_RECORD)
        client.get_observation(1)
        assert client.session.get.call_args[0][0] == f"{BASE_URL}/observations/discharge/1/"

    def test_observation_statistics_url(self, client) -> None:
        client.session.get.return_value = make_response({})
        client.get_observation_statistics()
        assert client.session.get.call_args[0][0] == f"{BASE_URL}/observations/discharge/statistics/"

    def test_percentile_bands_url(self, client) -> None:
        client.session.get.return_value = make_response({})
        client.get_percentile_bands()
        assert client.session.get.call_args[0][0] == f"{BASE_URL}/observations/discharge/percentile-bands/"

    def test_percentile_date_range_url(self, client) -> None:
        client.session.get.return_value = make_response({})
        client.get_percentile_date_range()
        assert client.session.get.call_args[0][0] == f"{BASE_URL}/observations/discharge/percentile-date-range/"

    def test_list_forecasts_url(self, client) -> None:
        client.session.get.return_value = make_response(make_paginated([]))
        client.list_forecasts()
        assert client.session.get.call_args[0][0] == f"{BASE_URL}/forecasts/"

    def test_get_forecast_url(self, client) -> None:
        client.session.get.return_value = make_response(FORECAST_DETAIL_RECORD)
        client.get_forecast(99)
        assert client.session.get.call_args[0][0] == f"{BASE_URL}/forecasts/99/"

    def test_list_forecasts_by_station_url(self, client) -> None:
        # Server action: url_path='by-station/(?P<station_number>[^/.]+)'
        client.session.get.return_value = make_response(make_paginated([]))
        client.list_forecasts_by_station("MRGO1")
        assert client.session.get.call_args[0][0] == f"{BASE_URL}/forecasts/by-station/MRGO1/"

    def test_get_latest_forecast_url(self, client) -> None:
        client.session.get.return_value = make_response(FORECAST_DETAIL_RECORD)
        client.get_latest_forecast()
        assert client.session.get.call_args[0][0] == f"{BASE_URL}/forecasts/latest/"

    def test_get_forecast_statistics_url(self, client) -> None:
        client.session.get.return_value = make_response({})
        client.get_forecast_statistics()
        assert client.session.get.call_args[0][0] == f"{BASE_URL}/forecasts/statistics/"

    def test_list_logs_url(self, client) -> None:
        client.session.get.return_value = make_response(make_paginated([]))
        client.list_logs()
        assert client.session.get.call_args[0][0] == f"{BASE_URL}/logs/"

    def test_get_log_url(self, client) -> None:
        client.session.get.return_value = make_response(LOG_RECORD)
        client.get_log(55)
        assert client.session.get.call_args[0][0] == f"{BASE_URL}/logs/55/"

    def test_raster_configurations_url(self, client) -> None:
        client.session.get.return_value = make_response(make_paginated([]))
        client.list_raster_configurations()
        assert client.session.get.call_args[0][0] == f"{BASE_URL}/raster-configurations/"

    def test_raster_datasets_url(self, client) -> None:
        client.session.get.return_value = make_response(make_paginated([]))
        client.list_raster_datasets()
        assert client.session.get.call_args[0][0] == f"{BASE_URL}/raster-datasets/"

    def test_raster_layers_url(self, client) -> None:
        client.session.get.return_value = make_response(make_paginated([]))
        client.list_raster_layers()
        assert client.session.get.call_args[0][0] == f"{BASE_URL}/raster-layers/"

    def test_raster_logs_url(self, client) -> None:
        client.session.get.return_value = make_response(make_paginated([]))
        client.list_raster_logs()
        assert client.session.get.call_args[0][0] == f"{BASE_URL}/raster-logs/"

    def test_raster_variables_url(self, client) -> None:
        client.session.get.return_value = make_response(make_paginated([]))
        client.list_raster_variables()
        assert client.session.get.call_args[0][0] == f"{BASE_URL}/raster-variables/"

    def test_spatial_extents_url(self, client) -> None:
        client.session.get.return_value = make_response(make_paginated([]))
        client.list_spatial_extents()
        assert client.session.get.call_args[0][0] == f"{BASE_URL}/spatial-extents/"

    def test_analytics_computations_url(self, client) -> None:
        client.session.get.return_value = make_response(make_paginated([]))
        client.list_computations()
        assert client.session.get.call_args[0][0] == f"{BASE_URL}/analytics/computations/"

    def test_analytics_logs_url(self, client) -> None:
        client.session.get.return_value = make_response(make_paginated([]))
        client.list_analytics_logs()
        assert client.session.get.call_args[0][0] == f"{BASE_URL}/analytics/logs/"


# ===========================================================================
# Query Parameter Compatibility
# Verifies that param names the client sends match what the server reads.
# ===========================================================================


class TestQueryParamCompatibility:
    """Client query params match what the server filters and views expect."""

    # --- Stations ---

    def test_stations_agency_param(self, client) -> None:
        # Server filterset_fields includes 'agency'
        client.session.get.return_value = make_response(make_paginated([]))
        client.list_stations(agency="USGS")
        params = client.session.get.call_args[1].get("params", {})
        assert params.get("agency") == "USGS"

    def test_stations_state_param(self, client) -> None:
        client.session.get.return_value = make_response(make_paginated([]))
        client.list_stations(state="OR")
        params = client.session.get.call_args[1].get("params", {})
        assert params.get("state") == "OR"

    def test_stations_is_active_param(self, client) -> None:
        client.session.get.return_value = make_response(make_paginated([]))
        client.list_stations(is_active=True)
        params = client.session.get.call_args[1].get("params", {})
        assert params.get("is_active") is True

    def test_stations_huc_code_param(self, client) -> None:
        client.session.get.return_value = make_response(make_paginated([]))
        client.list_stations(huc_code="17090011")
        params = client.session.get.call_args[1].get("params", {})
        assert params.get("huc_code") == "17090011"

    def test_stations_search_param(self, client) -> None:
        # Server search_fields includes station_number, name, basin
        client.session.get.return_value = make_response(make_paginated([]))
        client.list_stations(search="Willamette")
        params = client.session.get.call_args[1].get("params", {})
        assert params.get("search") == "Willamette"

    def test_stations_by_region_group_by_state(self, client) -> None:
        # Server view reads request.query_params.get('group_by')
        client.session.get.return_value = make_response({})
        client.get_stations_by_region(group_by="state")
        params = client.session.get.call_args[1].get("params", {})
        assert params.get("group_by") == "state"

    # --- Observations ---

    def test_observations_type_filter(self, client) -> None:
        # Server filterset_fields includes 'type'
        client.session.get.return_value = make_response(make_paginated([]))
        client.list_observations(type="daily_mean")
        params = client.session.get.call_args[1].get("params", {})
        assert params.get("type") == "daily_mean"

    def test_observations_quality_code_filter(self, client) -> None:
        client.session.get.return_value = make_response(make_paginated([]))
        client.list_observations(quality_code="A")
        params = client.session.get.call_args[1].get("params", {})
        assert params.get("quality_code") == "A"

    def test_observations_unit_filter(self, client) -> None:
        client.session.get.return_value = make_response(make_paginated([]))
        client.list_observations(unit="cfs")
        params = client.session.get.call_args[1].get("params", {})
        assert params.get("unit") == "cfs"

    def test_download_observations_uses_station_number_param(self, client) -> None:
        # Server get_queryset() reads request.query_params.get('station_number')
        # and filters station__station_number – client must send 'station_number'.
        client.session.get.return_value = make_response(make_paginated([]))
        client.download_observations("14103000")
        params = client.session.get.call_args[1].get("params", {})
        assert params.get("station_number") == "14103000"

    def test_download_observations_usgs_id_overrides_station_number(self, client) -> None:
        # When usgs_id is provided, it replaces station_number in the query.
        client.session.get.return_value = make_response(make_paginated([]))
        client.download_observations("MRGO1", usgs_id="14171000")
        params = client.session.get.call_args[1].get("params", {})
        assert params.get("station_number") == "14171000", (
            "usgs_id should override station_number in the query param"
        )

    def test_download_observations_type_param(self, client) -> None:
        # Server filters by 'type' via filterset_fields
        client.session.get.return_value = make_response(make_paginated([]))
        client.download_observations("14103000")
        params = client.session.get.call_args[1].get("params", {})
        assert params.get("type") == "daily_mean"

    # --- Forecasts ---

    def test_forecasts_station_number_filter(self, client) -> None:
        # Server list() reads request.query_params.get('station_number')
        client.session.get.return_value = make_response(make_paginated([]))
        client.list_forecasts(station_number="MRGO1")
        params = client.session.get.call_args[1].get("params", {})
        assert params.get("station_number") == "MRGO1"

    def test_forecasts_source_filter(self, client) -> None:
        # Server filterset_fields includes 'source'
        client.session.get.return_value = make_response(make_paginated([]))
        client.list_forecasts(source="NOAA_RFC")
        params = client.session.get.call_args[1].get("params", {})
        assert params.get("source") == "NOAA_RFC"

    def test_forecasts_date_range_params(self, client) -> None:
        # Server list() reads 'start_date' and 'end_date'
        client.session.get.return_value = make_response(make_paginated([]))
        client.list_forecasts(
            start_date="2026-01-01T00:00:00Z",
            end_date="2026-03-31T00:00:00Z",
        )
        params = client.session.get.call_args[1].get("params", {})
        assert params.get("start_date") == "2026-01-01T00:00:00Z"
        assert params.get("end_date") == "2026-03-31T00:00:00Z"

    # --- Logs ---

    def test_logs_status_filter(self, client) -> None:
        # Server filterset_fields includes 'status'
        client.session.get.return_value = make_response(make_paginated([]))
        client.list_logs(status="failed")
        params = client.session.get.call_args[1].get("params", {})
        assert params.get("status") == "failed"

    def test_logs_configuration_filter_by_id(self, client) -> None:
        # Server filterset_fields includes 'configuration' (FK, matched by PK)
        client.session.get.return_value = make_response(make_paginated([]))
        client.list_logs(configuration=3)
        params = client.session.get.call_args[1].get("params", {})
        assert params.get("configuration") == 3

    # --- Configurations ---

    def test_configurations_data_type_filter(self, client) -> None:
        # Server filterset_fields includes 'data_type'
        client.session.get.return_value = make_response(make_paginated([]))
        client.list_configurations(data_type="daily_mean")
        params = client.session.get.call_args[1].get("params", {})
        assert params.get("data_type") == "daily_mean"

    def test_configurations_is_enabled_filter(self, client) -> None:
        client.session.get.return_value = make_response(make_paginated([]))
        client.list_configurations(is_enabled=True)
        params = client.session.get.call_args[1].get("params", {})
        assert params.get("is_enabled") is True

    def test_configurations_schedule_type_filter(self, client) -> None:
        client.session.get.return_value = make_response(make_paginated([]))
        client.list_configurations(schedule_type="daily")
        params = client.session.get.call_args[1].get("params", {})
        assert params.get("schedule_type") == "daily"

    def test_configurations_data_strategy_filter(self, client) -> None:
        client.session.get.return_value = make_response(make_paginated([]))
        client.list_configurations(data_strategy="append")
        params = client.session.get.call_args[1].get("params", {})
        assert params.get("data_strategy") == "append"


# ===========================================================================
# Pagination Compatibility
# ===========================================================================


class TestPaginationCompatibility:
    """Client pagination params align with server StandardResultsSetPagination."""

    def test_client_sends_limit(self, client) -> None:
        """Client sends 'limit', matching the server's page_size_query_param.

        StandardResultsSetPagination uses ``page_size_query_param = 'limit'``.
        The client must send ``limit`` so cfg.page_size is respected by the server.
        """
        client.session.get.return_value = make_response(make_paginated([]))
        client.list_stations()
        params = client.session.get.call_args[1].get("params", {})
        assert "limit" in params, "Client must send 'limit' to control server page size"
        assert "page_size" not in params, "Client must not send 'page_size' (server ignores it)"

    def test_pagination_follows_next_link_regardless(self, client) -> None:
        """Despite the page_size mismatch, multi-page results are still fetched correctly."""
        page1 = make_paginated([OBS_RECORD], next_url=f"{BASE_URL}/observations/discharge/?page=2")
        page2 = make_paginated([{**OBS_RECORD, "id": 2}], next_url=None)
        client.session.get.side_effect = [
            make_response(page1),
            make_response(page2),
        ]

        result = client.list_observations()

        assert len(result) == 2
        assert client.session.get.call_count == 2


# ===========================================================================
# Response Schema Compatibility
# Verifies that field names the server serializers emit match what the
# client code reads when parsing higher-level helpers.
# ===========================================================================


class TestResponseSchemaCompatibility:
    """Server response fields match what the client parses."""

    # --- download_observations ---

    def test_download_observations_reads_observed_at(self, client) -> None:
        """Client reads 'observed_at'; server DischargeObservationSerializer emits it."""
        client.session.get.return_value = make_response(make_paginated([OBS_RECORD]))
        df = client.download_observations("14103000")
        assert not df.empty
        assert "date" in df.columns  # client normalises observed_at → date

    def test_download_observations_reads_discharge(self, client) -> None:
        """Client reads 'discharge'; server emits it as a float field."""
        client.session.get.return_value = make_response(make_paginated([OBS_RECORD]))
        df = client.download_observations("14103000")
        assert df["discharge_cfs"].iloc[0] == 3450.0

    def test_download_observations_reads_quality_code(self, client) -> None:
        """Client reads 'quality_code'; server emits it."""
        client.session.get.return_value = make_response(make_paginated([OBS_RECORD]))
        df = client.download_observations("14103000")
        assert df["quality_code"].iloc[0] == "A"

    def test_download_observations_output_columns(self, client) -> None:
        """Output DataFrame has exactly the expected columns."""
        client.session.get.return_value = make_response(make_paginated([OBS_RECORD]))
        df = client.download_observations("14103000")
        assert list(df.columns) == [
            "station_number", "usgs_id", "date", "discharge_cfs", "quality_code"
        ]

    # --- download_forecasts ---

    def test_download_forecasts_reads_run_list_id_and_run_date(self, client) -> None:
        """Client reads 'id' and 'run_date' from list endpoint; server emits both."""
        import pandas as pd
        list_resp = make_response(make_paginated([FORECAST_LIST_RECORD]))
        detail_resp = make_response(FORECAST_DETAIL_RECORD)
        client.session.get.side_effect = [list_resp, detail_resp]

        df = client.download_forecasts("MRGO1")

        assert not df.empty
        assert set(df.columns) >= {
            "station_number", "run_date", "lead_date", "lead_day", "forecast_value_cfs"
        }

    def test_download_forecasts_reads_data_array_from_detail(self, client) -> None:
        """Client reads 'data' from detail endpoint; server ForecastRunSerializer emits it."""
        list_resp = make_response(make_paginated([FORECAST_LIST_RECORD]))
        detail_resp = make_response(FORECAST_DETAIL_RECORD)
        client.session.get.side_effect = [list_resp, detail_resp]

        df = client.download_forecasts("MRGO1")

        assert len(df) == 3  # 3 forecast points in FORECAST_DETAIL_RECORD
        assert df["forecast_value_cfs"].tolist() == [1100.0, 1050.0, 980.0]

    def test_download_forecasts_data_item_fields(self, client) -> None:
        """Server data items use 'date' and 'value' keys; client parses both."""
        list_resp = make_response(make_paginated([FORECAST_LIST_RECORD]))
        detail_resp = make_response(FORECAST_DETAIL_RECORD)
        client.session.get.side_effect = [list_resp, detail_resp]

        df = client.download_forecasts("MRGO1")

        assert df["lead_date"].dtype.name.startswith("datetime")
        assert df["run_date"].dtype.name.startswith("datetime")

    # --- Forecast statistics field names ---

    def test_forecast_statistics_server_fields(self, client) -> None:
        """Server ForecastStatisticsSerializer emits these fields; client returns dict as-is."""
        # Server response: start_date, end_date, count, stations,
        #                  total_forecast_points, avg_rmse
        payload = {
            "start_date": "2025-01-01T00:00:00Z",
            "end_date": "2026-03-15T00:00:00Z",
            "count": 500,
            "stations": 12,
            "total_forecast_points": 5000,
            "avg_rmse": 0.09,
        }
        client.session.get.return_value = make_response(payload)
        result = client.get_forecast_statistics()
        for field in ("start_date", "end_date", "count", "stations", "total_forecast_points", "avg_rmse"):
            assert field in result, f"Expected field '{field}' in forecast statistics response"

    # --- Station statistics field names ---

    def test_station_statistics_server_fields(self, client) -> None:
        """Server StationViewSet.statistics() emits these keys."""
        payload = {
            "station_number": "MRGO1",
            "name": "Marys River at Philomath",
            "agency": "NOAA_RFC",
            "observation_counts": {
                "total": 4380,
                "realtime_15min": 0,
                "daily_mean": 4380,
            },
            "latest_observation": "2026-03-29T00:00:00Z",
            "record_period": {"start": "2014-10-01", "end": "2026-03-29", "years": 11.5},
        }
        client.session.get.return_value = make_response(payload)
        result = client.get_station_statistics("MRGO1")
        assert result["station_number"] == "MRGO1"
        assert "observation_counts" in result

    # --- Configuration list serializer field names ---

    def test_configuration_list_includes_data_source(self, client) -> None:
        """PullConfigurationSerializer emits 'data_source'; client returns raw dict."""
        client.session.get.return_value = make_response(make_paginated([CONFIG_RECORD]))
        result = client.list_configurations()
        assert result[0]["data_source"] == "USGS"
        assert result[0]["station_count"] == 5

    # --- Log serializer field names ---

    def test_log_list_includes_configuration_name(self, client) -> None:
        """DataPullLogListSerializer emits 'configuration_name'."""
        client.session.get.return_value = make_response(make_paginated([LOG_RECORD]))
        result = client.list_logs()
        assert result[0]["configuration_name"] == "USGS Daily OR"
        assert result[0]["status"] == "success"

    # --- Percentile bands response envelope ---

    def test_percentile_bands_response_envelope(self, client) -> None:
        """Server PercentileBandsResponseSerializer envelope: date, computed_at, count, results."""
        payload = {
            "date": "2026-03-29",
            "computed_at": "2026-03-29T08:00:00Z",
            "count": 2,
            "results": [
                {
                    "station_number": "MRGO1",
                    "discharge": 540.0,
                    "percentile_rank": 72.5,
                    "band": "above_normal",
                    "historical_record_count": 11,
                }
            ],
        }
        client.session.get.return_value = make_response(payload)
        result = client.get_percentile_bands(date="2026-03-29", station="MRGO1")
        assert result["date"] == "2026-03-29"
        assert "results" in result

    # --- Percentile date range ---

    def test_percentile_date_range_fields(self, client) -> None:
        """Server PercentileDateRangeSerializer emits min_date and max_date."""
        payload = {"min_date": "2014-10-01", "max_date": "2026-03-29"}
        client.session.get.return_value = make_response(payload)
        result = client.get_percentile_date_range()
        assert "min_date" in result
        assert "max_date" in result


# ===========================================================================
# Original Bug Regression
# The error that prompted this investigation: passing the API token as the
# base_url argument causes the token hash to be used as the hostname.
# ===========================================================================


class TestConfigurationRegression:
    """Regression: API token must not be passed as base_url."""

    def test_token_as_base_url_raises_or_produces_bad_host(self) -> None:
        """Passing token as positional arg makes it the base_url, not the token.

        StreamflowOpsClient(DATAOPS_API_TOKEN)  ← original broken call
        StreamflowOpsClient(base_url=URL, api_token=TOKEN)  ← correct usage

        The 'base_url' gets normalized via _normalize_api_base_url, which
        prepends 'https://' and uses the token string as the hostname.
        That produces a URL like https://<token_value>/api/v1 which fails
        DNS resolution at runtime.
        """
        from client import StreamflowOpsClient, _normalize_api_base_url

        fake_token = "a4c4846da0403a13a7abbdf82955d4ec2f23ae7c"

        # When the token is mistakenly passed as base_url, it becomes the host.
        normalized = _normalize_api_base_url(fake_token)
        assert fake_token in normalized, (
            "Token string ends up as the hostname when passed as base_url"
        )

        # Correct usage: token goes to api_token, not base_url.
        correct_client = StreamflowOpsClient(
            base_url="https://streamflowops.3rdplaces.io/api/v1",
            api_token=fake_token,
        )
        assert fake_token not in correct_client.api_base_url
        assert "Authorization" in correct_client.session.headers
        assert correct_client.session.headers["Authorization"] == f"Token {fake_token}"
