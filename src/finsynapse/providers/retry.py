"""Retry + timeout utilities for provider HTTP calls.

Transient upstream failures (DNS blips, 503, connection resets) are common
across free APIs. A lightweight retry loop with exponential backoff covers
the 99th percentile of transient errors without masking real problems.
"""

from __future__ import annotations

from functools import lru_cache

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


@lru_cache(maxsize=1)
def _default_session() -> requests.Session:
    return _create_session()


def _create_session(
    total_retries: int = 3,
    backoff_factor: float = 1.0,
    status_forcelist: tuple[int, ...] = (429, 500, 502, 503, 504),
) -> requests.Session:
    # Restrict retries to idempotent methods. The shared session is a global
    # singleton, so a future caller doing POST/PUT must opt in explicitly.
    retry_strategy = Retry(
        total=total_retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
        allowed_methods=frozenset(["GET", "HEAD", "OPTIONS"]),
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session = requests.Session()
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def requests_session() -> requests.Session:
    """Create/return a shared requests.Session with automatic retry on transient failures.

    Uses urllib3's Retry with exponential backoff (3 retries, 1s factor).
    Retries only idempotent methods (GET/HEAD/OPTIONS).
    """
    return _default_session()
