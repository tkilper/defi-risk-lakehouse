"""
MakerDAO liquidation event ingestion client.

Queries the MakerDAO subgraph for Bite events (CDP liquidations). In the
Messari subgraph schema, these appear as ``liquidates`` entities similar to
other lending protocols.

NOTE: The MakerDAO subgraph ID `G1KHEQkA7shnPZahHUmKcerfpqBMNnL9xJzWGAVrj7n7`
was not found on the decentralized network. MakerDAO rebranded to Sky in 2024.
Search The Graph Explorer for "maker" or "sky" to find the current valid subgraph.
This client will be updated once a working subgraph ID is confirmed.
"""

from __future__ import annotations

import logging
import os
from typing import Any

from ingestion.graph_client import GraphQLClient

logger = logging.getLogger(__name__)

# MakerDAO/Sky subgraph — update this ID once a valid one is confirmed on The Graph
_SUBGRAPH_ID = "G1KHEQkA7shnPZahHUmKcerfpqBMNnL9xJzWGAVrj7n7"
_GATEWAY_URL = f"https://gateway.thegraph.com/api/{{api_key}}/subgraphs/id/{_SUBGRAPH_ID}"
_HOSTED_URL = f"https://api.thegraph.com/subgraphs/id/{_SUBGRAPH_ID}"

# ---------------------------------------------------------------------------
# GraphQL query — Bite events (CDP liquidations via Messari schema)
# ---------------------------------------------------------------------------
_LIQUIDATION_QUERY = """
query GetMakerLiquidations($first: Int!, $skip: Int!) {
  liquidates(
    first: $first
    skip: $skip
    orderBy: timestamp
    orderDirection: asc
  ) {
    id
    hash
    timestamp
    blockNumber
    liquidator
    liquidatee
    asset {
      id
      symbol
      decimals
    }
    amount
    amountUSD
    profitUSD
    market {
      id
      name
    }
  }
}
"""

_LIQUIDATION_RANGE_QUERY = """
query GetMakerLiquidationsInRange($first: Int!, $skip: Int!, $from_ts: Int!, $to_ts: Int!) {
  liquidates(
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
    hash
    timestamp
    blockNumber
    liquidator
    liquidatee
    asset {
      id
      symbol
      decimals
    }
    amount
    amountUSD
    profitUSD
    market {
      id
      name
    }
  }
}
"""


class MakerLiquidationClient:
    """
    Fetches Bite (liquidation) events from the MakerDAO subgraph.

    If ``GRAPH_API_KEY`` is set, uses the decentralized network endpoint.
    """

    def __init__(self) -> None:
        api_key = os.getenv("GRAPH_API_KEY", "")
        url = _GATEWAY_URL if api_key else _HOSTED_URL
        self._client = GraphQLClient(subgraph_url=url)

    def fetch_liquidations(self) -> list[dict[str, Any]]:
        """Return all MakerDAO liquidation events (paginated)."""
        logger.info("Fetching MakerDAO liquidation events...")
        records = self._client.paginate(
            query_template=_LIQUIDATION_QUERY,
            data_key="liquidates",
        )
        logger.info("Maker: fetched %d liquidation events.", len(records))
        return records

    def fetch_liquidations_in_range(self, from_ts: int, to_ts: int) -> list[dict[str, Any]]:
        """Fetch liquidation events within a Unix timestamp range (for backfill)."""
        logger.info(
            "Fetching Maker liquidations between %d and %d...", from_ts, to_ts
        )
        records = self._client.paginate(
            query_template=_LIQUIDATION_RANGE_QUERY,
            data_key="liquidates",
            extra_vars={"from_ts": from_ts, "to_ts": to_ts},
        )
        logger.info("Maker: fetched %d liquidation events in range.", len(records))
        return records
