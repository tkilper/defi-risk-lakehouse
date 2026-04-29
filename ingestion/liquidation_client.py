"""
Aave V3 liquidation event ingestion client.

Queries the Aave V3 Ethereum subgraph on The Graph for LiquidationCall
events. Returns raw liquidation dicts that are written to MinIO and
normalised by the Spark bronze/silver jobs.

Subgraph: https://thegraph.com/explorer/subgraphs/Cd2gEDVeqnjBn1hSeqFMitw8Q1iiyV9FYUZkLNRcL87g
"""

from __future__ import annotations

import logging
import os
from typing import Any

from ingestion.graph_client import GraphQLClient

logger = logging.getLogger(__name__)

_SUBGRAPH_ID = "Cd2gEDVeqnjBn1hSeqFMitw8Q1iiyV9FYUZkLNRcL87g"
_HOSTED_URL = "https://api.thegraph.com/subgraphs/name/aave/protocol-v3"
_GATEWAY_URL = f"https://gateway.thegraph.com/api/{{api_key}}/subgraphs/id/{_SUBGRAPH_ID}"

# ---------------------------------------------------------------------------
# GraphQL query — LiquidationCall events
# The Aave V3 subgraph exposes liquidationCalls with on-chain event fields.
# ---------------------------------------------------------------------------
_LIQUIDATION_QUERY = """
query GetLiquidations($first: Int!, $skip: Int!) {
  liquidationCalls(
    first: $first
    skip: $skip
    orderBy: timestamp
    orderDirection: asc
  ) {
    id
    timestamp
    blockNumber
    user {
      id
    }
    collateralReserve {
      symbol
      decimals
      underlyingAsset
    }
    principalReserve {
      symbol
      decimals
      underlyingAsset
    }
    collateralAmount
    principalAmount
    liquidator {
      id
    }
  }
}
"""

# Historical query with timestamp filter for backfill
_LIQUIDATION_RANGE_QUERY = """
query GetLiquidationsInRange($first: Int!, $skip: Int!, $from_ts: Int!, $to_ts: Int!) {
  liquidationCalls(
    first: $first
    skip: $skip
    where: {
      timestamp_gte: $from_ts
      timestamp_lte: $to_ts
    }
    orderBy: timestamp
    orderDirection: asc
  ) {
    id
    timestamp
    blockNumber
    user {
      id
    }
    collateralReserve {
      symbol
      decimals
      underlyingAsset
    }
    principalReserve {
      symbol
      decimals
      underlyingAsset
    }
    collateralAmount
    principalAmount
    liquidator {
      id
    }
  }
}
"""


class AaveLiquidationClient:
    """
    Fetches LiquidationCall events from the Aave V3 Ethereum subgraph.

    If ``GRAPH_API_KEY`` is set, uses the decentralized network endpoint;
    otherwise falls back to the hosted service.
    """

    def __init__(self) -> None:
        api_key = os.getenv("GRAPH_API_KEY", "")
        url = _GATEWAY_URL if api_key else _HOSTED_URL
        self._client = GraphQLClient(subgraph_url=url)

    def fetch_liquidations(self) -> list[dict[str, Any]]:
        """Return all LiquidationCall events (paginated)."""
        logger.info("Fetching Aave V3 liquidation events...")
        records = self._client.paginate(
            query_template=_LIQUIDATION_QUERY,
            data_key="liquidationCalls",
        )
        logger.info("Aave: fetched %d liquidation events.", len(records))
        return records

    def fetch_liquidations_in_range(self, from_ts: int, to_ts: int) -> list[dict[str, Any]]:
        """Fetch liquidation events within a Unix timestamp range (for backfill)."""
        logger.info(
            "Fetching Aave V3 liquidations between %d and %d...", from_ts, to_ts
        )
        records = self._client.paginate(
            query_template=_LIQUIDATION_RANGE_QUERY,
            data_key="liquidationCalls",
            extra_vars={"from_ts": from_ts, "to_ts": to_ts},
        )
        logger.info("Aave: fetched %d liquidation events in range.", len(records))
        return records
