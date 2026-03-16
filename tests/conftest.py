"""Shared pytest fixtures and test helpers for the StreamflowOps client suite."""

from __future__ import annotations

import sys
from types import ModuleType
from unittest.mock import MagicMock, patch

import pytest
import requests


# ---------------------------------------------------------------------------#
# Stub the ``config`` module so tests never need a real .env / config file.  #
# ---------------------------------------------------------------------------#

_config_stub = ModuleType("config")
_cfg_stub = MagicMock()
_cfg_stub.api_base_url = "https://streamflowops.3rdplaces.io/api/v1"
_cfg_stub.api_token = "test-token-abc"
_cfg_stub.page_size = 100
_cfg_stub.max_download_workers = 4
_config_stub.cfg = _cfg_stub
sys.modules["config"] = _config_stub

# Import *after* the stub is registered so client.py resolves ``from config import cfg``
from client import StreamflowOpsClient  # noqa: E402  (intentional late import)

BASE_URL = "https://streamflowops.3rdplaces.io/api/v1"


# ---------------------------------------------------------------------------#
# Reusable fixture factory                                                    #
# ---------------------------------------------------------------------------#


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


# ---------------------------------------------------------------------------#
# Pytest fixtures                                                             #
# ---------------------------------------------------------------------------#


@pytest.fixture()
def client() -> StreamflowOpsClient:
    """A fresh ``StreamflowOpsClient`` with a mocked HTTP session."""
    c = StreamflowOpsClient(
        base_url=BASE_URL,
        api_token="test-token-abc",
    )
    c.session = MagicMock(spec=requests.Session)
    return c
