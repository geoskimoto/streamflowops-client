"""Tests for StreamflowOpsClient GET endpoints.

Each test class covers one API resource group.  All HTTP calls are intercepted
via a mocked ``requests.Session`` – no network traffic is produced.

Conventions
-----------
- ``client`` fixture:   a ``StreamflowOpsClient`` whose ``.session`` is a Mock.
- ``make_response()``:  builds a ``requests.Response`` mock from raw data.
- ``make_paginated()``: wraps records in the DRF pagination envelope.
"""

from __future__ import annotations

import pytest
import requests

from conftest import BASE_URL, make_paginated, make_response


# ---------------------------------------------------------------------------#
# Helpers                                                                     #
# ---------------------------------------------------------------------------#


def assert_get_called(mock_session, expected_path: str) -> None:
    """Assert that ``session.get`` was called with the correct full URL."""
    mock_session.get.assert_called_once()
    call_url = mock_session.get.call_args[0][0]
    assert call_url == f"{BASE_URL}{expected_path}"


def assert_get_called_with_params(
    mock_session, expected_path: str, expected_params: dict
) -> None:
    """Assert URL and that every key in *expected_params* was passed."""
    assert_get_called(mock_session, expected_path)
    actual_params = mock_session.get.call_args[1].get(
        "params", mock_session.get.call_args[0][1] if len(mock_session.get.call_args[0]) > 1 else {}
    )
    for key, val in expected_params.items():
        assert actual_params.get(key) == val, (
            f"Param '{key}': expected {val!r}, got {actual_params.get(key)!r}"
        )


# ---------------------------------------------------------------------------#
# URL normalisation (unit tests – no HTTP)                                   #
# ---------------------------------------------------------------------------#


class TestNormalizeApiBaseUrl:
    """_normalize_api_base_url() cleans up URL variants correctly."""

    def test_plain_url_unchanged(self) -> None:
        from client import _normalize_api_base_url

        assert _normalize_api_base_url(BASE_URL) == BASE_URL

    def test_trailing_slash_stripped(self) -> None:
        from client import _normalize_api_base_url

        assert _normalize_api_base_url(f"{BASE_URL}/") == BASE_URL

    def test_docs_suffix_stripped(self) -> None:
        from client import _normalize_api_base_url

        assert _normalize_api_base_url(f"{BASE_URL}/docs") == BASE_URL

    def test_scheme_added_when_missing(self) -> None:
        from client import _normalize_api_base_url

        result = _normalize_api_base_url("streamflowops.3rdplaces.io/api/v1")
        assert result.startswith("https://")

    def test_empty_url_raises(self) -> None:
        from client import _normalize_api_base_url

        with pytest.raises(ValueError, match="cannot be empty"):
            _normalize_api_base_url("")


# ---------------------------------------------------------------------------#
# Analytics                                                                   #
# ---------------------------------------------------------------------------#


class TestAnalyticsComputations:
    def test_list_computations_calls_correct_url(self, client) -> None:
        records = [{"id": 1, "name": "daily_percentiles"}]
        client.session.get.return_value = make_response(make_paginated(records))

        result = client.list_computations()

        assert result == records
        assert_get_called(client.session, "/analytics/computations/")

    def test_list_computations_passes_ordering(self, client) -> None:
        client.session.get.return_value = make_response(make_paginated([]))

        client.list_computations(ordering="-id")

        assert_get_called_with_params(
            client.session, "/analytics/computations/", {"ordering": "-id"}
        )

    def test_get_computation_calls_correct_url(self, client) -> None:
        payload = {"id": 42, "name": "rmse_calc"}
        client.session.get.return_value = make_response(payload)

        result = client.get_computation(42)

        assert result == payload
        assert_get_called(client.session, "/analytics/computations/42/")

    def test_list_analytics_logs_calls_correct_url(self, client) -> None:
        records = [{"id": 1, "status": "success"}]
        client.session.get.return_value = make_response(make_paginated(records))

        result = client.list_analytics_logs()

        assert result == records
        assert_get_called(client.session, "/analytics/logs/")

    def test_get_analytics_log_calls_correct_url(self, client) -> None:
        payload = {"id": 7, "status": "failed"}
        client.session.get.return_value = make_response(payload)

        result = client.get_analytics_log(7)

        assert result == payload
        assert_get_called(client.session, "/analytics/logs/7/")


# ---------------------------------------------------------------------------#
# Configurations                                                              #
# ---------------------------------------------------------------------------#


class TestConfigurations:
    def test_list_configurations_no_filters(self, client) -> None:
        records = [{"id": 1, "name": "usgs_daily"}]
        client.session.get.return_value = make_response(make_paginated(records))

        result = client.list_configurations()

        assert result == records
        assert_get_called(client.session, "/configurations/")

    def test_list_configurations_with_filters(self, client) -> None:
        client.session.get.return_value = make_response(make_paginated([]))

        client.list_configurations(data_type="daily_mean", is_enabled=True)

        assert_get_called_with_params(
            client.session,
            "/configurations/",
            {"data_type": "daily_mean", "is_enabled": True},
        )

    def test_none_filters_not_sent(self, client) -> None:
        """None-valued filter params must be stripped before the request."""
        client.session.get.return_value = make_response(make_paginated([]))

        client.list_configurations(data_type=None, is_enabled=None)

        actual_params = client.session.get.call_args[1].get("params", {})
        assert "data_type" not in actual_params
        assert "is_enabled" not in actual_params

    def test_get_configuration(self, client) -> None:
        payload = {"id": 10, "data_type": "forecast"}
        client.session.get.return_value = make_response(payload)

        result = client.get_configuration(10)

        assert result == payload
        assert_get_called(client.session, "/configurations/10/")

    def test_get_configuration_execution_history(self, client) -> None:
        payload = {"executions": []}
        client.session.get.return_value = make_response(payload)

        result = client.get_configuration_execution_history(10)

        assert result == payload
        assert_get_called(client.session, "/configurations/10/execution_history/")

    def test_get_configuration_statistics(self, client) -> None:
        payload = {"success_rate": 0.98}
        client.session.get.return_value = make_response(payload)

        result = client.get_configuration_statistics(10)

        assert result == payload
        assert_get_called(client.session, "/configurations/10/statistics/")


# ---------------------------------------------------------------------------#
# Forecasts                                                                   #
# ---------------------------------------------------------------------------#


class TestForecasts:
    def test_list_forecasts_no_filters(self, client) -> None:
        records = [{"id": 1, "run_date": "2026-03-15T12:00:00Z"}]
        client.session.get.return_value = make_response(make_paginated(records))

        result = client.list_forecasts()

        assert result == records
        assert_get_called(client.session, "/forecasts/")

    def test_list_forecasts_with_station_number(self, client) -> None:
        client.session.get.return_value = make_response(make_paginated([]))

        client.list_forecasts(station_number="MRGO1", source="NOAA_RFC")

        assert_get_called_with_params(
            client.session,
            "/forecasts/",
            {"station_number": "MRGO1", "source": "NOAA_RFC"},
        )

    def test_get_forecast_returns_payload(self, client) -> None:
        payload = {"id": 99, "data": [{"date": "2026-03-16", "value": 1200.5}]}
        client.session.get.return_value = make_response(payload)

        result = client.get_forecast(99)

        assert result == payload
        assert_get_called(client.session, "/forecasts/99/")

    def test_list_forecasts_by_station(self, client) -> None:
        records = [{"id": 5, "run_date": "2026-03-14T12:00:00Z"}]
        client.session.get.return_value = make_response(make_paginated(records))

        result = client.list_forecasts_by_station("MRGO1")

        assert result == records
        assert_get_called(client.session, "/forecasts/by-station/MRGO1/")

    def test_list_forecasts_by_station_with_source_filter(self, client) -> None:
        client.session.get.return_value = make_response(make_paginated([]))

        client.list_forecasts_by_station("MRGO1", source="NOAA_RFC")

        assert_get_called_with_params(
            client.session,
            "/forecasts/by-station/MRGO1/",
            {"source": "NOAA_RFC"},
        )

    def test_get_latest_forecast(self, client) -> None:
        payload = {"id": 200, "run_date": "2026-03-16T00:00:00Z"}
        client.session.get.return_value = make_response(payload)

        result = client.get_latest_forecast()

        assert result == payload
        assert_get_called(client.session, "/forecasts/latest/")

    def test_get_forecast_statistics_no_filters(self, client) -> None:
        payload = {"total_runs": 500, "avg_rmse": 0.12}
        client.session.get.return_value = make_response(payload)

        result = client.get_forecast_statistics()

        assert result == payload
        assert_get_called(client.session, "/forecasts/statistics/")

    def test_get_forecast_statistics_with_date_range(self, client) -> None:
        client.session.get.return_value = make_response({})

        client.get_forecast_statistics(
            source="NOAA_RFC",
            start_date="2026-01-01T00:00:00Z",
            end_date="2026-03-01T00:00:00Z",
        )

        assert_get_called_with_params(
            client.session,
            "/forecasts/statistics/",
            {
                "source": "NOAA_RFC",
                "start_date": "2026-01-01T00:00:00Z",
                "end_date": "2026-03-01T00:00:00Z",
            },
        )


# ---------------------------------------------------------------------------#
# Logs                                                                        #
# ---------------------------------------------------------------------------#


class TestLogs:
    def test_list_logs_no_filters(self, client) -> None:
        records = [{"id": 1, "status": "success"}]
        client.session.get.return_value = make_response(make_paginated(records))

        result = client.list_logs()

        assert result == records
        assert_get_called(client.session, "/logs/")

    def test_list_logs_filter_by_status(self, client) -> None:
        client.session.get.return_value = make_response(make_paginated([]))

        client.list_logs(status="failed")

        assert_get_called_with_params(
            client.session, "/logs/", {"status": "failed"}
        )

    def test_list_logs_filter_by_configuration(self, client) -> None:
        client.session.get.return_value = make_response(make_paginated([]))

        client.list_logs(configuration=3)

        assert_get_called_with_params(
            client.session, "/logs/", {"configuration": 3}
        )

    def test_get_log(self, client) -> None:
        payload = {"id": 55, "status": "running"}
        client.session.get.return_value = make_response(payload)

        result = client.get_log(55)

        assert result == payload
        assert_get_called(client.session, "/logs/55/")


# ---------------------------------------------------------------------------#
# Observations                                                                #
# ---------------------------------------------------------------------------#


class TestObservations:
    def test_list_observations_no_filters(self, client) -> None:
        records = [{"id": 1, "discharge": 540.0}]
        client.session.get.return_value = make_response(make_paginated(records))

        result = client.list_observations()

        assert result == records
        assert_get_called(client.session, "/observations/discharge/")

    def test_list_observations_with_type_filter(self, client) -> None:
        client.session.get.return_value = make_response(make_paginated([]))

        client.list_observations(type="daily_mean", quality_code="A", unit="cfs")

        assert_get_called_with_params(
            client.session,
            "/observations/discharge/",
            {"type": "daily_mean", "quality_code": "A", "unit": "cfs"},
        )

    def test_get_observation(self, client) -> None:
        payload = {"id": 8, "discharge": 300.5, "quality_code": "P"}
        client.session.get.return_value = make_response(payload)

        result = client.get_observation(8)

        assert result == payload
        assert_get_called(client.session, "/observations/discharge/8/")

    def test_get_percentile_bands_no_params(self, client) -> None:
        payload = {"date": "2026-03-15", "stations": []}
        client.session.get.return_value = make_response(payload)

        result = client.get_percentile_bands()

        assert result == payload
        assert_get_called(client.session, "/observations/discharge/percentile-bands/")

    def test_get_percentile_bands_with_date_and_station(self, client) -> None:
        client.session.get.return_value = make_response({})

        client.get_percentile_bands(date="2026-03-15", station="MRGO1")

        assert_get_called_with_params(
            client.session,
            "/observations/discharge/percentile-bands/",
            {"date": "2026-03-15", "station": "MRGO1"},
        )

    def test_get_percentile_date_range(self, client) -> None:
        payload = {"min_date": "2010-01-01", "max_date": "2026-03-15"}
        client.session.get.return_value = make_response(payload)

        result = client.get_percentile_date_range()

        assert result == payload
        assert_get_called(
            client.session, "/observations/discharge/percentile-date-range/"
        )

    def test_get_observation_statistics_no_filters(self, client) -> None:
        payload = {"count": 12000}
        client.session.get.return_value = make_response(payload)

        result = client.get_observation_statistics()

        assert result == payload
        assert_get_called(
            client.session, "/observations/discharge/statistics/"
        )

    def test_get_observation_statistics_with_filters(self, client) -> None:
        client.session.get.return_value = make_response({})

        client.get_observation_statistics(
            station_number="MRGO1",
            start_date="2025-01-01",
            end_date="2026-01-01",
            type="daily_mean",
        )

        assert_get_called_with_params(
            client.session,
            "/observations/discharge/statistics/",
            {
                "station_number": "MRGO1",
                "start_date": "2025-01-01",
                "end_date": "2026-01-01",
                "type": "daily_mean",
            },
        )


# ---------------------------------------------------------------------------#
# Raster Configurations                                                       #
# ---------------------------------------------------------------------------#


class TestRasterConfigurations:
    def test_list_raster_configurations(self, client) -> None:
        records = [{"id": 1, "name": "precip_snodas"}]
        client.session.get.return_value = make_response(make_paginated(records))

        result = client.list_raster_configurations()

        assert result == records
        assert_get_called(client.session, "/raster-configurations/")

    def test_get_raster_configuration(self, client) -> None:
        payload = {"id": 3, "name": "swe"}
        client.session.get.return_value = make_response(payload)

        result = client.get_raster_configuration(3)

        assert result == payload
        assert_get_called(client.session, "/raster-configurations/3/")

    def test_get_raster_configuration_logs(self, client) -> None:
        payload = {"logs": []}
        client.session.get.return_value = make_response(payload)

        result = client.get_raster_configuration_logs(3)

        assert result == payload
        assert_get_called(client.session, "/raster-configurations/3/logs/")


# ---------------------------------------------------------------------------#
# Raster Datasets                                                             #
# ---------------------------------------------------------------------------#


class TestRasterDatasets:
    def test_list_raster_datasets(self, client) -> None:
        records = [{"id": 1, "name": "SNODAS"}]
        client.session.get.return_value = make_response(make_paginated(records))

        result = client.list_raster_datasets()

        assert result == records
        assert_get_called(client.session, "/raster-datasets/")

    def test_get_raster_dataset(self, client) -> None:
        payload = {"id": 2, "name": "PRISM"}
        client.session.get.return_value = make_response(payload)

        result = client.get_raster_dataset(2)

        assert result == payload
        assert_get_called(client.session, "/raster-datasets/2/")

    def test_get_raster_dataset_coverage(self, client) -> None:
        payload = {"start": "2010-01-01", "end": "2026-03-16"}
        client.session.get.return_value = make_response(payload)

        result = client.get_raster_dataset_coverage(2)

        assert result == payload
        assert_get_called(client.session, "/raster-datasets/2/coverage/")

    def test_get_raster_dataset_variables(self, client) -> None:
        payload = {"variables": ["swe", "precip"]}
        client.session.get.return_value = make_response(payload)

        result = client.get_raster_dataset_variables(2)

        assert result == payload
        assert_get_called(client.session, "/raster-datasets/2/variables/")


# ---------------------------------------------------------------------------#
# Raster Layers                                                               #
# ---------------------------------------------------------------------------#


class TestRasterLayers:
    def test_list_raster_layers(self, client) -> None:
        records = [{"id": 1, "valid_time": "2026-03-15T06:00:00Z"}]
        client.session.get.return_value = make_response(make_paginated(records))

        result = client.list_raster_layers()

        assert result == records
        assert_get_called(client.session, "/raster-layers/")

    def test_list_raster_layers_with_search(self, client) -> None:
        client.session.get.return_value = make_response(make_paginated([]))

        client.list_raster_layers(search="swe")

        assert_get_called_with_params(
            client.session, "/raster-layers/", {"search": "swe"}
        )

    def test_get_raster_layer(self, client) -> None:
        payload = {"id": 11, "file_url": "https://example.com/layer.tif"}
        client.session.get.return_value = make_response(payload)

        result = client.get_raster_layer(11)

        assert result == payload
        assert_get_called(client.session, "/raster-layers/11/")

    def test_get_raster_layer_download(self, client) -> None:
        payload = {"download_url": "https://example.com/download/11"}
        client.session.get.return_value = make_response(payload)

        result = client.get_raster_layer_download(11)

        assert result == payload
        assert_get_called(client.session, "/raster-layers/11/download/")

    def test_get_raster_layer_thumbnail(self, client) -> None:
        payload = {"thumbnail_url": "https://example.com/thumb/11.png"}
        client.session.get.return_value = make_response(payload)

        result = client.get_raster_layer_thumbnail(11)

        assert result == payload
        assert_get_called(client.session, "/raster-layers/11/thumbnail/")

    def test_get_raster_layers_coverage(self, client) -> None:
        payload = {"first": "2020-01-01", "last": "2026-03-16"}
        client.session.get.return_value = make_response(payload)

        result = client.get_raster_layers_coverage()

        assert result == payload
        assert_get_called(client.session, "/raster-layers/coverage/")

    def test_get_raster_layers_statistics(self, client) -> None:
        payload = {"total_layers": 730}
        client.session.get.return_value = make_response(payload)

        result = client.get_raster_layers_statistics()

        assert result == payload
        assert_get_called(client.session, "/raster-layers/statistics/")


# ---------------------------------------------------------------------------#
# Raster Logs                                                                 #
# ---------------------------------------------------------------------------#


class TestRasterLogs:
    def test_list_raster_logs(self, client) -> None:
        records = [{"id": 1, "status": "success"}]
        client.session.get.return_value = make_response(make_paginated(records))

        result = client.list_raster_logs()

        assert result == records
        assert_get_called(client.session, "/raster-logs/")

    def test_get_raster_log(self, client) -> None:
        payload = {"id": 22, "status": "failed", "error": "timeout"}
        client.session.get.return_value = make_response(payload)

        result = client.get_raster_log(22)

        assert result == payload
        assert_get_called(client.session, "/raster-logs/22/")


# ---------------------------------------------------------------------------#
# Raster Variables                                                            #
# ---------------------------------------------------------------------------#


class TestRasterVariables:
    def test_list_raster_variables(self, client) -> None:
        records = [{"id": 1, "name": "swe"}]
        client.session.get.return_value = make_response(make_paginated(records))

        result = client.list_raster_variables()

        assert result == records
        assert_get_called(client.session, "/raster-variables/")

    def test_get_raster_variable(self, client) -> None:
        payload = {"id": 4, "name": "precip", "unit": "mm"}
        client.session.get.return_value = make_response(payload)

        result = client.get_raster_variable(4)

        assert result == payload
        assert_get_called(client.session, "/raster-variables/4/")


# ---------------------------------------------------------------------------#
# Spatial Extents                                                             #
# ---------------------------------------------------------------------------#


class TestSpatialExtents:
    def test_list_spatial_extents(self, client) -> None:
        records = [{"id": 1, "name": "PNW"}]
        client.session.get.return_value = make_response(make_paginated(records))

        result = client.list_spatial_extents()

        assert result == records
        assert_get_called(client.session, "/spatial-extents/")

    def test_get_spatial_extent(self, client) -> None:
        payload = {"id": 2, "name": "Columbia Basin", "bbox": [-125, 45, -110, 50]}
        client.session.get.return_value = make_response(payload)

        result = client.get_spatial_extent(2)

        assert result == payload
        assert_get_called(client.session, "/spatial-extents/2/")


# ---------------------------------------------------------------------------#
# Stations                                                                    #
# ---------------------------------------------------------------------------#


class TestStations:
    def test_list_stations_no_filters(self, client) -> None:
        records = [{"station_number": "MRGO1", "name": "Marys River"}]
        client.session.get.return_value = make_response(make_paginated(records))

        result = client.list_stations()

        assert result == records
        assert_get_called(client.session, "/stations/")

    def test_list_stations_with_agency_filter(self, client) -> None:
        client.session.get.return_value = make_response(make_paginated([]))

        client.list_stations(agency="USGS", state="OR", is_active=True)

        assert_get_called_with_params(
            client.session,
            "/stations/",
            {"agency": "USGS", "state": "OR", "is_active": True},
        )

    def test_list_stations_search(self, client) -> None:
        client.session.get.return_value = make_response(make_paginated([]))

        client.list_stations(search="Willamette")

        assert_get_called_with_params(
            client.session, "/stations/", {"search": "Willamette"}
        )

    def test_get_station(self, client) -> None:
        payload = {"station_number": "MRGO1", "name": "Marys River at Philomath"}
        client.session.get.return_value = make_response(payload)

        result = client.get_station("MRGO1")

        assert result == payload
        assert_get_called(client.session, "/stations/MRGO1/")

    def test_get_station_statistics(self, client) -> None:
        payload = {"station_number": "MRGO1", "observation_count": 4380}
        client.session.get.return_value = make_response(payload)

        result = client.get_station_statistics("MRGO1")

        assert result == payload
        assert_get_called(client.session, "/stations/MRGO1/statistics/")

    def test_get_stations_by_region_no_params(self, client) -> None:
        payload = {"OR": ["MRGO1", "WNNO3"]}
        client.session.get.return_value = make_response(payload)

        result = client.get_stations_by_region()

        assert result == payload
        assert_get_called(client.session, "/stations/by_region/")

    def test_get_stations_by_region_with_group_by(self, client) -> None:
        client.session.get.return_value = make_response({})

        client.get_stations_by_region(group_by="huc")

        assert_get_called_with_params(
            client.session, "/stations/by_region/", {"group_by": "huc"}
        )


# ---------------------------------------------------------------------------#
# Pagination                                                                  #
# ---------------------------------------------------------------------------#


class TestPagination:
    def test_paginate_follows_next_url(self, client) -> None:
        """_paginate() should follow the ``next`` link until exhausted."""
        page1 = make_paginated(
            [{"id": 1}], next_url=f"{BASE_URL}/stations/?page=2"
        )
        page2 = make_paginated([{"id": 2}], next_url=None)

        client.session.get.side_effect = [
            make_response(page1),
            make_response(page2),
        ]

        result = client.list_stations()

        assert result == [{"id": 1}, {"id": 2}]
        assert client.session.get.call_count == 2

    def test_paginate_empty_results(self, client) -> None:
        client.session.get.return_value = make_response(make_paginated([]))

        result = client.list_stations()

        assert result == []


# ---------------------------------------------------------------------------#
# Error handling                                                              #
# ---------------------------------------------------------------------------#


class TestErrorHandling:
    def test_401_raises_informative_error(self, client) -> None:
        client.session.get.return_value = make_response({}, status_code=401)

        with pytest.raises(requests.HTTPError, match="401 Unauthorized"):
            client.get_station("MRGO1")

    def test_404_re_raises_http_error(self, client) -> None:
        client.session.get.return_value = make_response({}, status_code=404)

        with pytest.raises(requests.HTTPError):
            client.get_station("DOES_NOT_EXIST")

    def test_500_re_raises_http_error(self, client) -> None:
        client.session.get.return_value = make_response({}, status_code=500)

        with pytest.raises(requests.HTTPError):
            client.list_stations()


# ---------------------------------------------------------------------------#
# Higher-level helpers                                                        #
# ---------------------------------------------------------------------------#


class TestDownloadForecasts:
    """download_forecasts() integration-style tests (still fully mocked)."""

    def _make_run_list_response(self, run_ids: list[int]) -> object:
        records = [{"id": rid, "run_date": f"2026-03-{rid:02d}T12:00:00Z"} for rid in run_ids]
        return make_response(make_paginated(records))

    def _make_run_detail_response(self, run_id: int) -> object:
        return make_response(
            {
                "id": run_id,
                "run_date": f"2026-03-{run_id:02d}T12:00:00Z",
                "data": [
                    {"date": f"2026-03-{(run_id + d):02d}T00:00:00Z", "value": 1000.0 + d}
                    for d in range(1, 4)
                ],
            }
        )

    def test_returns_empty_dataframe_when_all_cached(self, client) -> None:
        client.session.get.return_value = self._make_run_list_response([1, 2, 3])

        result = client.download_forecasts("MRGO1", known_ids={1, 2, 3})

        import pandas as pd
        assert isinstance(result, pd.DataFrame)
        assert result.empty

    def test_fetches_only_uncached_runs(self, client) -> None:
        list_resp = self._make_run_list_response([1, 2])
        detail_resp = self._make_run_detail_response(2)
        # First call: list endpoint; second call: detail for id=2 only
        client.session.get.side_effect = [list_resp, detail_resp]

        result = client.download_forecasts("MRGO1", known_ids={1})

        import pandas as pd
        assert isinstance(result, pd.DataFrame)
        assert set(result.columns) >= {
            "station_number", "run_date", "lead_date", "lead_day", "forecast_value_cfs"
        }
        assert (result["station_number"] == "MRGO1").all()


class TestFetchForecastForRunDate:
    def test_raises_value_error_when_date_missing(self, client) -> None:
        import pandas as pd

        client.session.get.return_value = make_response(make_paginated([]))

        with pytest.raises(ValueError, match="No forecast run found"):
            client.fetch_forecast_for_run_date("MRGO1", pd.Timestamp("2026-03-01"))

    def test_raises_value_error_when_run_has_no_data(self, client) -> None:
        import pandas as pd

        run_list = make_paginated(
            [{"id": 99, "run_date": "2026-03-01T12:00:00Z"}]
        )
        run_detail = {"id": 99, "run_date": "2026-03-01T12:00:00Z", "data": []}
        client.session.get.side_effect = [
            make_response(run_list),
            make_response(run_detail),
        ]

        with pytest.raises(ValueError, match="returned no data"):
            client.fetch_forecast_for_run_date("MRGO1", pd.Timestamp("2026-03-01"))


class TestDownloadObservations:
    def test_returns_empty_dataframe_when_no_records(self, client) -> None:
        client.session.get.return_value = make_response(make_paginated([]))

        result = client.download_observations("MRGO1")

        import pandas as pd
        assert isinstance(result, pd.DataFrame)
        assert result.empty

    def test_maps_columns_correctly(self, client) -> None:
        import pandas as pd

        records = [
            {"observed_at": "2026-01-01T00:00:00Z", "discharge": 500.0, "quality_code": "A"},
            {"observed_at": "2026-01-02T00:00:00Z", "discharge": 510.5, "quality_code": "P"},
        ]
        client.session.get.return_value = make_response(make_paginated(records))

        result = client.download_observations("MRGO1")

        assert list(result.columns) == [
            "station_number", "usgs_id", "date", "discharge_cfs", "quality_code"
        ]
        assert (result["station_number"] == "MRGO1").all()
        assert result["discharge_cfs"].tolist() == [500.0, 510.5]

    def test_uses_usgs_id_as_query_param_when_provided(self, client) -> None:
        client.session.get.return_value = make_response(make_paginated([]))

        client.download_observations("MRGO1", usgs_id="14171000")

        call_params = client.session.get.call_args[1].get("params", {})
        assert call_params.get("station_number") == "14171000"
