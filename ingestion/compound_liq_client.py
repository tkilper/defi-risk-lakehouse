"""
Compound V3 liquidation event ingestion client.

Queries the Compound V3 Messari subgraph for AbsorbCollateral (liquidation)
events. The Messari subgraph schema differs from the original Compound subgraph;
this client targets the ``liquidates`` entity exposed by Messari.

NOTE: The Compound V3 Messari subgraph (5nwMCSHaTqG3Kd2gHznbTXEnZ9QNWsssQfbHhDqQSQFp)
has a schema that may require a full query rewrite if the subgraph has been updated.
Validate against the current schema on The Graph Explorer before deploying.
"""

from __future__ import annotations

import logging
import os
from typing import Any

from ingestion.graph_client import GraphQLClient

logger = logging.getLogger(__name__)

# Compound V3 Messari subgraph — same as compound_client.py
_SUBGRAPH_ID = "5nwMCSHaTqG3Kd2gHznbTXEnZ9QNWsssQfbHhDqQSQFp"
_GATEWAY_URL = f"https://gateway.thegraph.com/api/{{api_key}}/subgraphs/id/{_SUBGRAPH_ID}"
_HOSTED_URL = f"https://api.thegraph.com/subgraphs/id/{_SUBGRAPH_ID}"

# ---------------------------------------------------------------------------
# GraphQL query — liquidate events (Messari protocol schema)
# Messari uses the `liquidates` entity for liquidation events.
# ---------------------------------------------------------------------------
_LIQUIDATION_QUERY = """
query GetCompoundLiquidations($first: Int!, $skip: Int!) {
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
query GetCompoundLiquidationsInRange($first: Int!, $skip: Int!, $from_ts: Int!, $to_ts: Int!) {
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


class CompoundLiquidationClient:
    """
    Fetches liquidation events from the Compound V3 Messari subgraph.

    If ``GRAPH_API_KEY`` is set, uses the decentralized network endpoint.
    """

    def __init__(self) -> None:
        api_key = os.getenv("GRAPH_API_KEY", "")
        url = _GATEWAY_URL if api_key else _HOSTED_URL
        self._client = GraphQLClient(subgraph_url=url)

    def fetch_liquidations(self) -> list[dict[str, Any]]:
        """Return all Compound V3 liquidation events (paginated)."""
        logger.info("Fetching Compound V3 liquidation events...")
        records = self._client.paginate(
            query_template=_LIQUIDATION_QUERY,
            data_key="liquidates",
        )
        logger.info("Compound: fetched %d liquidation events.", len(records))
        return records

    def fetch_liquidations_in_range(self, from_ts: int, to_ts: int) -> list[dict[str, Any]]:
        """Fetch liquidation events within a Unix timestamp range (for backfill)."""
        logger.info(
            "Fetching Compound V3 liquidations between %d and %d...", from_ts, to_ts
        )
        records = self._client.paginate(
            query_template=_LIQUIDATION_RANGE_QUERY,
            data_key="liquidates",
            extra_vars={"from_ts": from_ts, "to_ts": to_ts},
        )
        logger.info("Compound: fetched %d liquidation events in range.", len(records))
        return records
