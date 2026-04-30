"""Retry + timeout utilities for provider HTTP calls.

Transient upstream failures (DNS blips, 503, connection resets) are common
across free APIs. A lightweight retry loop with exponential backoff covers
the 99th percentile of transient errors without masking real problems.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from functools import lru_cache, wraps
from typing import TypeVar

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

F = TypeVar("F", bound=Callable)


@lru_cache(maxsize=1)
def _default_session() -> requests.Session:
    return _create_session()


def _create_session(
    total_retries: int = 3,
    backoff_factor: float = 1.0,
    status_forcelist: tuple[int, ...] = (429, 500, 502, 503, 504),
) -> requests.Session:
    retry_strategy = Retry(
        total=total_retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
        allowed_methods=False,
    )
    adapter = HTTPAdapter(max_retries=retry_strategy, pool_connections=1, pool_maxsize=1)
    session = requests.Session()
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def requests_session() -> requests.Session:
    """Create/return a shared requests.Session with automatic retry on transient failures.

    Uses urllib3's Retry with exponential backoff (3 retries, 1s factor).
    """
    return _default_session()


def with_backoff(
    max_retries: int = 3,
    base_delay: float = 2.0,
    exceptions: tuple[type[Exception], ...] = (
        requests.exceptions.ConnectionError,
        requests.exceptions.Timeout,
        requests.exceptions.HTTPError,
    ),
) -> Callable[[F], F]:
    """Decorator: retry a function with exponential backoff on transient errors."""

    def decorator(func: F) -> F:
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exc = None
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as exc:
                    last_exc = exc
                    if attempt < max_retries:
                        delay = base_delay * (2**attempt)
                        time.sleep(delay)
            raise last_exc  # type: ignore[misc]

        return wrapper  # type: ignore[return-value]

    return decorator
