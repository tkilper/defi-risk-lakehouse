"""
Base GraphQL client for The Graph subgraph APIs.

Features
--------
- Cursor-based pagination (skip / id_gt pattern)
- Exponential-backoff retry on transient errors and 429 rate limits
- Optional API key injection (The Graph decentralised network)
- Configurable endpoint via environment variable or constructor arg
"""

from __future__ import annotations

import logging
import os
import time
from typing import Any

import requests

logger = logging.getLogger(__name__)

# Default page size — The Graph enforces a hard limit of 1000 per page
PAGE_SIZE = 1000

# Maximum total records to fetch per run (safety cap to prevent runaway loops)
MAX_RECORDS = 100_000


class GraphQLError(Exception):
    """Raised when The Graph returns a GraphQL-level error response."""


class GraphQLClient:
    """
    Thin HTTP client for The Graph subgraph APIs with pagination and retry logic.

    Parameters
    ----------
    subgraph_url:
        Full URL of the subgraph endpoint.
        If ``GRAPH_API_KEY`` is set in the environment and the URL contains
        the ``{api_key}`` placeholder, the key is injected automatically.
    max_retries:
        Number of times to retry on server errors / rate limits.
    backoff_base:
        Base seconds for exponential backoff between retries.
    """

    def __init__(
        self,
        subgraph_url: str,
        max_retries: int = 5,
        backoff_base: float = 2.0,
    ) -> None:
        api_key = os.getenv("GRAPH_API_KEY", "")
        self.url = subgraph_url.replace("{api_key}", api_key) if api_key else subgraph_url
        self.max_retries = max_retries
        self.backoff_base = backoff_base
        self.session = requests.Session()
        self.session.headers.update({"Content-Type": "application/json"})

    def execute(self, query: str, variables: dict[str, Any] | None = None) -> dict[str, Any]:
        """
        Execute a single GraphQL query with retry logic.

        Returns the ``data`` field of the response (not the full envelope).

        Raises
        ------
        GraphQLError
            If The Graph returns a non-empty ``errors`` field.
        requests.HTTPError
            If the HTTP response is a non-retryable 4xx error.
        """
        payload: dict[str, Any] = {"query": query}
        if variables:
            payload["variables"] = variables

        last_exception: Exception | None = None
        for attempt in range(1, self.max_retries + 1):
            try:
                response = self.session.post(self.url, json=payload, timeout=30)

                if response.status_code == 429:
                    wait = self.backoff_base**attempt
                    logger.warning(
                        "Rate limited (429). Waiting %.1fs before retry %d.", wait, attempt
                    )
                    time.sleep(wait)
                    continue

                response.raise_for_status()
                body = response.json()

                if errors := body.get("errors"):
                    raise GraphQLError(f"GraphQL errors: {errors}")

                return body.get("data", {})

            except (requests.ConnectionError, requests.Timeout) as exc:
                last_exception = exc
                wait = self.backoff_base**attempt
                logger.warning(
                    "Network error on attempt %d/%d: %s. Retrying in %.1fs.",
                    attempt,
                    self.max_retries,
                    exc,
                    wait,
                )
                time.sleep(wait)

        raise RuntimeError(
            f"All {self.max_retries} retries exhausted for {self.url}"
        ) from last_exception

    def paginate(
        self,
        query_template: str,
        data_key: str,
        extra_vars: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        """
        Paginate through all results using skip-based pagination.

        Parameters
        ----------
        query_template:
            GraphQL query string.  Must accept ``$first: Int!`` and
            ``$skip: Int!`` variables.
        data_key:
            The top-level key inside ``data`` that holds the result list
            (e.g. ``"userReserves"``, ``"vaults"``).
        extra_vars:
            Additional variables merged into each paginated request.

        Returns
        -------
        list[dict]
            All records across all pages.
        """
        all_records: list[dict[str, Any]] = []
        skip = 0

        while len(all_records) < MAX_RECORDS:
            variables: dict[str, Any] = {"first": PAGE_SIZE, "skip": skip, **(extra_vars or {})}
            data = self.execute(query_template, variables)
            batch: list[dict[str, Any]] = data.get(data_key, [])

            if not batch:
                break

            all_records.extend(batch)
            logger.info("Fetched %d records (total so far: %d).", len(batch), len(all_records))

            if len(batch) < PAGE_SIZE:
                # Last page — no more records
                break

            skip += PAGE_SIZE

        logger.info("Pagination complete. Total records: %d.", len(all_records))
        return all_records
