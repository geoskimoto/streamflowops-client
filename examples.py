"""StreamflowOps client usage examples.

Run from the repo root after setting STREAMFLOWOPS_URL and STREAMFLOWOPS_TOKEN:

    export STREAMFLOWOPS_URL="https://streamflowops.3rdplaces.io/api/v1"
    export STREAMFLOWOPS_TOKEN="your-token-here"
    python3 examples.py

Or pass credentials directly to the constructor (shown in example 1).

Station numbers
---------------
NWRFC stations use alphanumeric identifiers like "MRGO1".
USGS gages use numeric strings like "14103000".
When pulling observations for a USGS gage via an NWRFC station, pass
usgs_id to download_observations() so the correct field is queried.
"""

from __future__ import annotations

import os

import pandas as pd

from client import StreamflowOpsClient


# ---------------------------------------------------------------------------#
# Client setup                                                                #
# ---------------------------------------------------------------------------#

# Option A – read from environment variables set in a config module
client = StreamflowOpsClient()

# Option B – pass credentials directly (overrides config)
# client = StreamflowOpsClient(
#     base_url="https://streamflowops.3rdplaces.io/api/v1",
#     api_token="your-token-here",
# )


# ===========================================================================
# Observed discharge data
# ===========================================================================


def example_obs_1_download_daily_mean() -> pd.DataFrame:
    """Download all daily-mean observations for a single USGS gage.

    download_observations() pages through the entire record and returns a
    tidy DataFrame sorted by date.  For a station with 10+ years of data
    this may take a few seconds on the first call.

    Columns returned:
        station_number  – NWRFC identifier passed to the call
        usgs_id         – USGS gage number (empty string when not provided)
        date            – observation date (datetime64, time-normalised to midnight)
        discharge_cfs   – mean daily discharge in cubic feet per second
        quality_code    – 'A' (approved) or 'P' (provisional)
    """
    print("\n=== Example OBS-1: Download all daily-mean observations ===")

    df = client.download_observations("14103000")

    print(f"Retrieved {len(df):,} daily-mean records")
    print(f"Date range : {df['date'].min().date()} → {df['date'].max().date()}")
    print(f"Flow range : {df['discharge_cfs'].min():.1f} – {df['discharge_cfs'].max():.1f} cfs")
    print(df.head())

    return df


def example_obs_2_nwrfc_station_with_usgs_id() -> pd.DataFrame:
    """Download observations for an NWRFC station using its USGS gage ID.

    Some NWRFC station entries are stored in StreamflowOps under the NWRFC
    identifier (e.g. "MRGO1"), but the observations table was populated from
    USGS and indexed by the USGS gage number.  Pass usgs_id so the query
    hits the right records.
    """
    print("\n=== Example OBS-2: NWRFC station with USGS gage crosswalk ===")

    df = client.download_observations("MRGO1", usgs_id="14171000")

    print(f"Station    : MRGO1  (USGS {df['usgs_id'].iloc[0]})")
    print(f"Records    : {len(df):,}")
    print(f"Date range : {df['date'].min().date()} → {df['date'].max().date()}")
    print(df.tail())

    return df


def example_obs_3_filter_approved_only() -> pd.DataFrame:
    """List approved daily-mean observations using the paginated list endpoint.

    list_observations() lets you filter by quality_code, type, and unit
    without downloading the full record.  Useful for quick spot-checks or
    when you only need a subset of the data.
    """
    print("\n=== Example OBS-3: Filter approved daily-mean observations ===")

    records = client.list_observations(
        type="daily_mean",
        quality_code="A",
        ordering="-observed_at",  # most recent first
    )

    df = pd.DataFrame(records)
    print(f"Approved daily-mean records across all stations: {len(df):,}")
    if not df.empty:
        print(df[["station_number", "observed_at", "discharge", "unit"]].head(10))

    return df


def example_obs_4_station_statistics() -> dict:
    """Pull observation statistics for a single station (no DataFrame needed).

    Returns observation counts, date ranges, and the latest value – useful
    for a quick health check without downloading the full record.
    """
    print("\n=== Example OBS-4: Station observation statistics ===")

    stats = client.get_observation_statistics(
        station_number="14103000",
        type="daily_mean",
    )

    print(f"Total records : {stats['count']:,}")
    print(f"Date range    : {stats['start_date']} → {stats['end_date']}")
    print(f"Flow range    : {stats['min_value']:.1f} – {stats['max_value']:.1f} cfs")
    print(f"Mean flow     : {stats['mean_value']:.1f} cfs")
    print(f"Latest value  : {stats['latest_value']:.1f} cfs  @ {stats['latest_timestamp']}")

    return stats


# ===========================================================================
# Forecast data
# ===========================================================================


def example_fc_1_download_all_forecast_runs() -> pd.DataFrame:
    """Download every NWRFC forecast run for one station as a flat DataFrame.

    download_forecasts() fetches the run list, then retrieves each run's full
    data payload in parallel.  Returns a tidy DataFrame with one row per
    (run_date, lead_date) pair.

    Columns returned:
        station_number      – station identifier
        run_date            – date the forecast was issued (datetime64)
        lead_date           – valid date being forecast (datetime64)
        lead_day            – integer days ahead from run_date (0 = same day)
        forecast_value_cfs  – forecast discharge in cfs
    """
    print("\n=== Example FC-1: Download all forecast runs for a station ===")

    df = client.download_forecasts("MRGO1")

    if df.empty:
        print("No forecast data returned.")
        return df

    n_runs = df["run_date"].nunique()
    print(f"Runs retrieved  : {n_runs}")
    print(f"Run date range  : {df['run_date'].min().date()} → {df['run_date'].max().date()}")
    print(f"Max lead day    : {df['lead_day'].max()} days")
    print(df.head(10))

    return df


def example_fc_2_incremental_update(known_ids: set[int] | None = None) -> pd.DataFrame:
    """Fetch only new forecast runs, skipping already-cached IDs.

    Pass the set of run IDs you have locally; the client skips those and
    fetches only what is new.  Returns an empty DataFrame when nothing is new.

    Typical workflow:
        cached_ids = set(local_df["run_id"].unique())
        new_df = example_fc_2_incremental_update(cached_ids)
        combined = pd.concat([local_df, new_df], ignore_index=True)
    """
    print("\n=== Example FC-2: Incremental forecast update ===")

    # Simulate having the three most recent runs already cached.
    if known_ids is None:
        all_runs = client.list_forecasts_by_station("MRGO1", source="NOAA_RFC")
        known_ids = {r["id"] for r in all_runs[:3]}
        print(f"Simulating cache with {len(known_ids)} known run IDs: {known_ids}")

    new_df = client.download_forecasts("MRGO1", known_ids=known_ids)

    if new_df.empty:
        print("No new runs to fetch – already up to date.")
    else:
        print(f"New rows fetched : {len(new_df):,}")
        print(new_df.head())

    return new_df


def example_fc_3_single_run_by_date() -> pd.DataFrame:
    """Fetch a single forecast run matching a specific run date.

    Useful when you want the forecast that was issued on a particular date,
    e.g. to compare what was predicted vs. what was observed.
    """
    print("\n=== Example FC-3: Fetch one forecast run by run date ===")

    # Use the most recent available run date to keep the example runnable.
    runs = client.list_forecasts_by_station("MRGO1", source="NOAA_RFC")
    if not runs:
        print("No forecast runs found for MRGO1.")
        return pd.DataFrame()

    target_date = pd.Timestamp(runs[0]["run_date"]).normalize()
    print(f"Fetching run issued on {target_date.date()}")

    df = client.fetch_forecast_for_run_date("MRGO1", run_date=target_date)

    print(f"Lead days : {df['lead_day'].min()} – {df['lead_day'].max()}")
    print(f"Flow range: {df['forecast_value_cfs'].min():.1f} – {df['forecast_value_cfs'].max():.1f} cfs")
    print(df)

    return df


def example_fc_4_forecast_statistics() -> dict:
    """Pull aggregate forecast statistics without downloading run data.

    Returns total run count, station count, date range, and average RMSE.
    """
    print("\n=== Example FC-4: Forecast aggregate statistics ===")

    stats = client.get_forecast_statistics(source="NOAA_RFC")

    print(f"Total runs  : {stats.get('count', 'N/A')}")
    print(f"Stations    : {stats.get('stations', 'N/A')}")
    print(f"Date range  : {stats.get('start_date')} → {stats.get('end_date')}")
    print(f"Avg RMSE    : {stats.get('avg_rmse')}")

    return stats


# ---------------------------------------------------------------------------#
# Runner                                                                      #
# ---------------------------------------------------------------------------#

if __name__ == "__main__":
    print("StreamflowOps Client – Examples")
    print("=" * 50)

    # Observations
    example_obs_1_download_daily_mean()
    example_obs_2_nwrfc_station_with_usgs_id()
    example_obs_3_filter_approved_only()
    example_obs_4_station_statistics()

    # Forecasts
    example_fc_1_download_all_forecast_runs()
    example_fc_2_incremental_update()
    example_fc_3_single_run_by_date()
    example_fc_4_forecast_statistics()

    print("\n" + "=" * 50)
    print("Done.")
