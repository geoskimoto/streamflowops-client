"""StreamflowOps HTTP client.

Handles pagination, parallel forecast-run fetching, and all documented GET
endpoints.  Higher-level helpers (download_forecasts, download_observations)
are preserved for backward compatibility.

NOTE: The individual GET /forecasts/{id}/ calls per run are a current
necessity because the list endpoint omits the ``data`` payload.  Once
StreamflowOps adds a bulk-export endpoint, replace download_forecasts() with
a single streaming request.
"""

from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Iterator
from urllib.parse import urlsplit, urlunsplit

import pandas as pd
import requests
from tqdm import tqdm

from config import cfg

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------#
# Module-level utilities                                                      #
# ---------------------------------------------------------------------------#


def _normalize_api_base_url(raw_url: str) -> str:
    """Return a clean, canonical API root URL.

    Accepts values such as:
    - https://streamflowops.3rdplaces.io/api/v1
    - https://streamflowops.3rdplaces.io/api/v1/docs/
    - streamflowops.3rdplaces.io/api/v1

    Strips query/fragment, trailing slash, and ``/docs`` suffix.
    """
    cleaned = (raw_url or "").strip()
    if not cleaned:
        raise ValueError("STREAMFLOWOPS_URL cannot be empty")

    if "://" not in cleaned:
        cleaned = f"https://{cleaned}"

    parsed = urlsplit(cleaned)
    netloc = parsed.netloc
    path = parsed.path.rstrip("/")

    if path.endswith("/docs"):
        path = path[:-5]

    api_root = "/api/v1"
    if api_root in path:
        path = path[: path.find(api_root) + len(api_root)]

    if not path:
        path = api_root

    scheme = parsed.scheme or "https"
    return urlunsplit((scheme, netloc, path, "", ""))


def _compact(params: dict[str, Any]) -> dict[str, Any]:
    """Drop keys whose value is None so they are never sent as query params."""
    return {k: v for k, v in params.items() if v is not None}


# ---------------------------------------------------------------------------#
# Client                                                                      #
# ---------------------------------------------------------------------------#


class StreamflowOpsClient:
    """HTTP client for the StreamflowOps REST API.

    Parameters
    ----------
    base_url:
        API root URL.  Defaults to ``cfg.api_base_url``.
    api_token:
        Bearer token.  Defaults to ``cfg.api_token``.
    """

    def __init__(
        self,
        base_url: str | None = None,
        api_token: str | None = None,
    ) -> None:
        self.api_base_url = _normalize_api_base_url(base_url or cfg.api_base_url)
        self.session = requests.Session()
        token = api_token if api_token is not None else cfg.api_token
        if token:
            self.session.headers["Authorization"] = f"Token {token}"

    # ----------------------------------------------------------------------- #
    # Low-level request helpers                                                #
    # ----------------------------------------------------------------------- #

    def _url(self, path: str) -> str:
        """Build a full URL from a root-relative *path*."""
        return f"{self.api_base_url}{path}"

    def _get(self, path: str, params: dict[str, Any] | None = None) -> Any:
        """Issue a GET request and return the parsed JSON body."""
        resp = self.session.get(self._url(path), params=params or {})
        self._raise_for_status(resp)
        return resp.json()

    def _raise_for_status(self, resp: requests.Response) -> None:
        try:
            resp.raise_for_status()
        except requests.HTTPError as exc:
            if resp.status_code == 401:
                token_state = "set" if bool(cfg.api_token) else "missing"
                raise requests.HTTPError(
                    f"401 Unauthorized for {resp.url}. "
                    f"Check STREAMFLOWOPS_TOKEN (currently {token_state}) and "
                    f"STREAMFLOWOPS_URL ({self.api_base_url})."
                ) from exc
            raise

    def _paginate(self, url: str, params: dict | None = None) -> Iterator[dict]:
        """Yield every result record from a paginated DRF endpoint."""
        params = dict(params or {})
        params.setdefault("limit", cfg.page_size)
        while url:
            resp = self.session.get(url, params=params)
            self._raise_for_status(resp)
            body = resp.json()
            yield from body["results"]
            url = body.get("next")
            params = None  # subsequent page URLs already encode all params

    def _paginate_path(
        self, path: str, params: dict[str, Any] | None = None
    ) -> list[dict]:
        """Return all records from a paginated endpoint given a root-relative path."""
        return list(self._paginate(self._url(path), params))

    # ----------------------------------------------------------------------- #
    # Analytics                                                                #
    # ----------------------------------------------------------------------- #

    def list_computations(self, *, ordering: str | None = None) -> list[dict]:
        """List all scheduled analytics computations."""
        return self._paginate_path(
            "/analytics/computations/", _compact({"ordering": ordering})
        )

    def get_computation(self, computation_id: int) -> dict:
        """Retrieve a single analytics computation by ID."""
        return self._get(f"/analytics/computations/{computation_id}/")

    def list_analytics_logs(self, *, ordering: str | None = None) -> list[dict]:
        """List all analytics computation run logs."""
        return self._paginate_path(
            "/analytics/logs/", _compact({"ordering": ordering})
        )

    def get_analytics_log(self, log_id: int) -> dict:
        """Retrieve a single analytics log entry by ID."""
        return self._get(f"/analytics/logs/{log_id}/")

    # ----------------------------------------------------------------------- #
    # Configurations                                                           #
    # ----------------------------------------------------------------------- #

    def list_configurations(
        self,
        *,
        data_strategy: str | None = None,
        data_type: str | None = None,
        is_enabled: bool | None = None,
        schedule_type: str | None = None,
        search: str | None = None,
        ordering: str | None = None,
    ) -> list[dict]:
        """List pull configurations with optional filters."""
        params = _compact(
            {
                "data_strategy": data_strategy,
                "data_type": data_type,
                "is_enabled": is_enabled,
                "schedule_type": schedule_type,
                "search": search,
                "ordering": ordering,
            }
        )
        return self._paginate_path("/configurations/", params)

    def get_configuration(self, config_id: int) -> dict:
        """Retrieve a single pull configuration by ID."""
        return self._get(f"/configurations/{config_id}/")

    def get_configuration_execution_history(self, config_id: int) -> dict:
        """Get execution history for a pull configuration."""
        return self._get(f"/configurations/{config_id}/execution_history/")

    def get_configuration_statistics(self, config_id: int) -> dict:
        """Get detailed statistics for a pull configuration."""
        return self._get(f"/configurations/{config_id}/statistics/")

    # ----------------------------------------------------------------------- #
    # Forecasts                                                                #
    # ----------------------------------------------------------------------- #

    def list_forecasts(
        self,
        *,
        station_number: str | None = None,
        source: str | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        run_date: str | None = None,
        search: str | None = None,
        ordering: str | None = None,
    ) -> list[dict]:
        """List forecast runs with optional filters."""
        params = _compact(
            {
                "station_number": station_number,
                "source": source,
                "start_date": start_date,
                "end_date": end_date,
                "run_date": run_date,
                "search": search,
                "ordering": ordering,
            }
        )
        return self._paginate_path("/forecasts/", params)

    def get_forecast(self, forecast_id: int) -> dict:
        """Retrieve a single forecast run with its full data array."""
        return self._get(f"/forecasts/{forecast_id}/")

    def list_forecasts_by_station(
        self,
        station_number: str,
        *,
        source: str | None = None,
        run_date: str | None = None,
        ordering: str | None = None,
    ) -> list[dict]:
        """List all forecast runs for a specific station (most recent first)."""
        params = _compact(
            {
                "station_number": station_number,
                "source": source,
                "run_date": run_date,
                "ordering": ordering,
            }
        )
        return self._paginate_path(
            f"/forecasts/by-station/{station_number}/", params
        )

    def get_latest_forecast(self) -> dict:
        """Retrieve the most recent forecast run across all stations."""
        return self._get("/forecasts/latest/")

    def get_forecast_statistics(
        self,
        *,
        source: str | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> dict:
        """Get aggregate statistics (counts, date ranges, avg RMSE) for forecasts."""
        params = _compact(
            {"source": source, "start_date": start_date, "end_date": end_date}
        )
        return self._get("/forecasts/statistics/", params)

    # ----------------------------------------------------------------------- #
    # Logs (data-pull execution logs)                                          #
    # ----------------------------------------------------------------------- #

    def list_logs(
        self,
        *,
        configuration: int | None = None,
        status: str | None = None,
        ordering: str | None = None,
    ) -> list[dict]:
        """List data-pull execution logs.

        Parameters
        ----------
        status:
            One of ``'running'``, ``'success'``, ``'failed'``.
        """
        params = _compact(
            {"configuration": configuration, "status": status, "ordering": ordering}
        )
        return self._paginate_path("/logs/", params)

    def get_log(self, log_id: int) -> dict:
        """Retrieve a single data-pull log entry."""
        return self._get(f"/logs/{log_id}/")

    # ----------------------------------------------------------------------- #
    # Observations                                                             #
    # ----------------------------------------------------------------------- #

    def list_observations(
        self,
        *,
        station: int | None = None,
        type: str | None = None,
        quality_code: str | None = None,
        unit: str | None = None,
        ordering: str | None = None,
    ) -> list[dict]:
        """List discharge observations with optional filters.

        Parameters
        ----------
        type:
            ``'daily_mean'`` or ``'realtime_15min'``.
        quality_code:
            ``'P'`` (provisional) or ``'A'`` (approved).
        unit:
            ``'cfs'`` or ``'cms'``.
        """
        params = _compact(
            {
                "station": station,
                "type": type,
                "quality_code": quality_code,
                "unit": unit,
                "ordering": ordering,
            }
        )
        return self._paginate_path("/observations/discharge/", params)

    def get_observation(self, observation_id: int) -> dict:
        """Retrieve a single discharge observation."""
        return self._get(f"/observations/discharge/{observation_id}/")

    def get_percentile_bands(
        self,
        *,
        date: str | None = None,
        station: str | None = None,
    ) -> dict:
        """Return precomputed exceedance percentile bands.

        Parameters
        ----------
        date:
            ``YYYY-MM-DD`` string.  Defaults to the latest available date.
        station:
            Filter to a single station number.
        """
        return self._get(
            "/observations/discharge/percentile-bands/",
            _compact({"date": date, "station": station}),
        )

    def get_percentile_date_range(self) -> dict:
        """Return the min/max dates available in daily flow percentiles."""
        return self._get("/observations/discharge/percentile-date-range/")

    def get_observation_statistics(
        self,
        *,
        station_number: str | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        type: str | None = None,
    ) -> dict:
        """Get a statistical summary of observations."""
        params = _compact(
            {
                "station_number": station_number,
                "start_date": start_date,
                "end_date": end_date,
                "type": type,
            }
        )
        return self._get("/observations/discharge/statistics/", params)

    # ----------------------------------------------------------------------- #
    # Raster Configurations                                                    #
    # ----------------------------------------------------------------------- #

    def list_raster_configurations(
        self, *, search: str | None = None, ordering: str | None = None
    ) -> list[dict]:
        """List raster pull configurations."""
        return self._paginate_path(
            "/raster-configurations/",
            _compact({"search": search, "ordering": ordering}),
        )

    def get_raster_configuration(self, config_id: int) -> dict:
        """Retrieve a single raster pull configuration."""
        return self._get(f"/raster-configurations/{config_id}/")

    def get_raster_configuration_logs(self, config_id: int) -> dict:
        """Get pull logs for a raster configuration."""
        return self._get(f"/raster-configurations/{config_id}/logs/")

    # ----------------------------------------------------------------------- #
    # Raster Datasets                                                          #
    # ----------------------------------------------------------------------- #

    def list_raster_datasets(
        self, *, search: str | None = None, ordering: str | None = None
    ) -> list[dict]:
        """List raster datasets."""
        return self._paginate_path(
            "/raster-datasets/",
            _compact({"search": search, "ordering": ordering}),
        )

    def get_raster_dataset(self, dataset_id: int) -> dict:
        """Retrieve a single raster dataset."""
        return self._get(f"/raster-datasets/{dataset_id}/")

    def get_raster_dataset_coverage(self, dataset_id: int) -> dict:
        """Get temporal coverage for a raster dataset."""
        return self._get(f"/raster-datasets/{dataset_id}/coverage/")

    def get_raster_dataset_variables(self, dataset_id: int) -> dict:
        """Get variables associated with a raster dataset."""
        return self._get(f"/raster-datasets/{dataset_id}/variables/")

    # ----------------------------------------------------------------------- #
    # Raster Layers                                                            #
    # ----------------------------------------------------------------------- #

    def list_raster_layers(
        self, *, search: str | None = None, ordering: str | None = None
    ) -> list[dict]:
        """List raster layers."""
        return self._paginate_path(
            "/raster-layers/",
            _compact({"search": search, "ordering": ordering}),
        )

    def get_raster_layer(self, layer_id: int) -> dict:
        """Retrieve a single raster layer."""
        return self._get(f"/raster-layers/{layer_id}/")

    def get_raster_layer_download(self, layer_id: int) -> dict:
        """Get download metadata/URL for a raster layer."""
        return self._get(f"/raster-layers/{layer_id}/download/")

    def get_raster_layer_thumbnail(self, layer_id: int) -> dict:
        """Get thumbnail metadata for a raster layer."""
        return self._get(f"/raster-layers/{layer_id}/thumbnail/")

    def get_raster_layers_coverage(self) -> dict:
        """Get temporal coverage summary across all raster layers."""
        return self._get("/raster-layers/coverage/")

    def get_raster_layers_statistics(self) -> dict:
        """Get aggregated statistics across all raster layers."""
        return self._get("/raster-layers/statistics/")

    # ----------------------------------------------------------------------- #
    # Raster Logs                                                              #
    # ----------------------------------------------------------------------- #

    def list_raster_logs(
        self, *, search: str | None = None, ordering: str | None = None
    ) -> list[dict]:
        """List raster pull logs."""
        return self._paginate_path(
            "/raster-logs/",
            _compact({"search": search, "ordering": ordering}),
        )

    def get_raster_log(self, log_id: int) -> dict:
        """Retrieve a single raster pull log entry."""
        return self._get(f"/raster-logs/{log_id}/")

    # ----------------------------------------------------------------------- #
    # Raster Variables                                                         #
    # ----------------------------------------------------------------------- #

    def list_raster_variables(
        self, *, search: str | None = None, ordering: str | None = None
    ) -> list[dict]:
        """List raster variables."""
        return self._paginate_path(
            "/raster-variables/",
            _compact({"search": search, "ordering": ordering}),
        )

    def get_raster_variable(self, variable_id: int) -> dict:
        """Retrieve a single raster variable."""
        return self._get(f"/raster-variables/{variable_id}/")

    # ----------------------------------------------------------------------- #
    # Spatial Extents                                                          #
    # ----------------------------------------------------------------------- #

    def list_spatial_extents(
        self, *, search: str | None = None, ordering: str | None = None
    ) -> list[dict]:
        """List spatial extents."""
        return self._paginate_path(
            "/spatial-extents/",
            _compact({"search": search, "ordering": ordering}),
        )

    def get_spatial_extent(self, extent_id: int) -> dict:
        """Retrieve a single spatial extent."""
        return self._get(f"/spatial-extents/{extent_id}/")

    # ----------------------------------------------------------------------- #
    # Stations                                                                 #
    # ----------------------------------------------------------------------- #

    def list_stations(
        self,
        *,
        agency: str | None = None,
        huc_code: str | None = None,
        is_active: bool | None = None,
        state: str | None = None,
        search: str | None = None,
        ordering: str | None = None,
    ) -> list[dict]:
        """List stations with optional filters.

        Parameters
        ----------
        agency:
            ``'USGS'``, ``'EC'``, or ``'NOAA_RFC'``.
        """
        params = _compact(
            {
                "agency": agency,
                "huc_code": huc_code,
                "is_active": is_active,
                "state": state,
                "search": search,
                "ordering": ordering,
            }
        )
        return self._paginate_path("/stations/", params)

    def get_station(self, station_number: str) -> dict:
        """Retrieve a single station by its station number."""
        return self._get(f"/stations/{station_number}/")

    def get_station_statistics(self, station_number: str) -> dict:
        """Get statistics (observation counts, date ranges) for a station."""
        return self._get(f"/stations/{station_number}/statistics/")

    def get_stations_by_region(self, *, group_by: str | None = None) -> dict:
        """Get stations grouped by region.

        Parameters
        ----------
        group_by:
            ``'state'`` or ``'huc'``.
        """
        return self._get(
            "/stations/by_region/", _compact({"group_by": group_by})
        )

    # ----------------------------------------------------------------------- #
    # Higher-level helpers                                                     #
    # ----------------------------------------------------------------------- #

    def download_forecasts(
        self,
        station_number: str,
        known_ids: set[int] | None = None,
    ) -> pd.DataFrame:
        """Download NWRFC forecast runs for one station; return a flat DataFrame.

        Columns: station_number, run_date, lead_date, lead_day,
        forecast_value_cfs

        Parameters
        ----------
        station_number:
            NWRFC station identifier.
        known_ids:
            Run IDs already present in the local cache; skipped.
        """
        known_ids = known_ids or set()
        runs = self.list_forecasts_by_station(station_number, source="NOAA_RFC")
        to_fetch = [r for r in runs if r["id"] not in known_ids]

        if not to_fetch:
            logger.info("%s: all %d runs already cached", station_number, len(runs))
            return pd.DataFrame()

        logger.info(
            "%s: fetching %d new runs (skipping %d cached)",
            station_number,
            len(to_fetch),
            len(runs) - len(to_fetch),
        )

        def _fetch_and_flatten(run_meta: dict) -> list[dict]:
            full = self.get_forecast(run_meta["id"])
            run_date = pd.Timestamp(full["run_date"]).normalize()
            rows = []
            for item in full.get("data") or []:
                lead_date = pd.Timestamp(item["date"]).normalize()
                lead_day = (lead_date - run_date).days
                rows.append(
                    {
                        "station_number": station_number,
                        "run_date": run_date,
                        "lead_date": lead_date,
                        "lead_day": lead_day,
                        "forecast_value_cfs": float(item["value"]),
                    }
                )
            return rows

        all_rows: list[dict] = []
        with ThreadPoolExecutor(max_workers=cfg.max_download_workers) as executor:
            futures = {executor.submit(_fetch_and_flatten, r): r for r in to_fetch}
            for future in tqdm(
                as_completed(futures),
                total=len(futures),
                desc=f"Fetching {station_number}",
            ):
                try:
                    all_rows.extend(future.result())
                except Exception as exc:
                    meta = futures[future]
                    logger.warning("Run %s failed: %s", meta["id"], exc)

        if not all_rows:
            return pd.DataFrame()

        df = pd.DataFrame(all_rows)
        df["run_date"] = pd.to_datetime(df["run_date"])
        df["lead_date"] = pd.to_datetime(df["lead_date"])
        return df

    def fetch_forecast_for_run_date(
        self,
        station_number: str,
        run_date: pd.Timestamp,
    ) -> pd.DataFrame:
        """Fetch a single forecast run matching ``run_date`` from the API.

        Does not read or write any local cache.

        Parameters
        ----------
        station_number:
            NWRFC station identifier.
        run_date:
            The run date to retrieve.

        Returns
        -------
        pd.DataFrame
            Columns: station_number, run_date, lead_date, lead_day,
            forecast_value_cfs

        Raises
        ------
        ValueError
            If no run exists for the requested date.
        """
        target = pd.Timestamp(run_date).normalize().date()
        runs = self.list_forecasts_by_station(station_number)
        matching = [
            r
            for r in runs
            if pd.Timestamp(r["run_date"]).normalize().date() == target
        ]
        if not matching:
            available = sorted({pd.Timestamp(r["run_date"]).date() for r in runs})
            raise ValueError(
                f"No forecast run found for {station_number} on {target}. "
                f"Available dates: {available}"
            )

        full = self.get_forecast(matching[0]["id"])
        run_date_ts = pd.Timestamp(target)
        rows = []
        for item in full.get("data") or []:
            lead_date = pd.Timestamp(item["date"]).normalize().tz_localize(None)
            lead_day = (lead_date - run_date_ts).days
            rows.append(
                {
                    "station_number": station_number,
                    "run_date": run_date_ts,
                    "lead_date": lead_date,
                    "lead_day": lead_day,
                    "forecast_value_cfs": float(item["value"]),
                }
            )

        if not rows:
            raise ValueError(
                f"Forecast run for {station_number} on {target} returned no data."
            )

        df = pd.DataFrame(rows)
        df["run_date"] = pd.to_datetime(df["run_date"])
        df["lead_date"] = pd.to_datetime(df["lead_date"])
        return df

    def download_observations(
        self,
        station_number: str,
        usgs_id: str | None = None,
    ) -> pd.DataFrame:
        """Download all daily-mean discharge observations for one station.

        Parameters
        ----------
        station_number:
            NWRFC station identifier used as the join key in returned data.
        usgs_id:
            USGS gage number.  When the observations endpoint requires USGS IDs
            rather than NWSLIs, pass the value obtained from the HADS crosswalk.

        Returns
        -------
        pd.DataFrame
            Columns: station_number, usgs_id, date, discharge_cfs, quality_code
        """
        query_id = usgs_id if usgs_id else station_number
        records = list(
            self._paginate(
                self._url("/observations/discharge/"),
                {"station_number": query_id, "type": "daily_mean"},
            )
        )

        if not records:
            logger.warning(
                "%s (queried as %s): no observations returned",
                station_number,
                query_id,
            )
            return pd.DataFrame()

        df = pd.DataFrame(
            [
                {
                    "station_number": station_number,
                    "usgs_id": usgs_id or "",
                    "date": pd.Timestamp(r["observed_at"]).normalize(),
                    "discharge_cfs": float(r["discharge"]),
                    "quality_code": r.get("quality_code", ""),
                }
                for r in records
            ]
        )
        df["date"] = pd.to_datetime(df["date"])
        return df.sort_values("date").reset_index(drop=True)
