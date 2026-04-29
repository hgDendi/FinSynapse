from __future__ import annotations

from pathlib import Path

import pytest


@pytest.fixture(scope="module")
def vcr_config() -> dict:
    return {
        "filter_query_parameters": ["apikey", "api_key", "token"],
        "filter_headers": ["authorization", "cookie"],
        "decode_compressed_response": True,
    }


@pytest.fixture
def tmp_data_dir(tmp_path, monkeypatch) -> Path:
    """Redirect bronze/silver/gold writes into a tmp dir so tests don't touch real data."""
    monkeypatch.setenv("FINSYNAPSE_DATA_DIR", str(tmp_path))
    # Reload settings to pick up the env override
    from finsynapse import config as cfg

    cfg.settings = cfg.Settings()
    return tmp_path
