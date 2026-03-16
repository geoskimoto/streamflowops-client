"""Shared test helpers for the StreamflowOps client test suite."""

from __future__ import annotations

from unittest.mock import MagicMock

import requests

BASE_URL = "https://streamflowops.3rdplaces.io/api/v1"


def make_response(data: object, status_code: int = 200) -> MagicMock:
    """Return a mock ``requests.Response`` whose ``.json()`` yields *data*."""
    resp = MagicMock(spec=requests.Response)
    resp.status_code = status_code
    resp.json.return_value = data
    resp.url = BASE_URL
    if status_code < 400:
        resp.raise_for_status.return_value = None
    else:
        resp.raise_for_status.side_effect = requests.HTTPError(
            response=resp, request=MagicMock()
        )
    return resp


def make_paginated(results: list, next_url: str | None = None) -> dict:
    """Wrap a list of records in the DRF pagination envelope."""
    return {
        "count": len(results),
        "next": next_url,
        "previous": None,
        "results": results,
    }
