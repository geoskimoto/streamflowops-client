"""Shared pytest fixtures for the StreamflowOps client test suite."""

from __future__ import annotations

import sys
from types import ModuleType
from unittest.mock import MagicMock

import pytest
import requests

from helpers import BASE_URL, make_response  # noqa: F401 – re-exported for convenience


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
