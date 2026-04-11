"""
Compound V3 (Comet) ingestion client.

Queries the Compound V3 subgraph for all positions with non-zero borrow
balances.  Compound V3 uses an isolated market model (USDC and ETH markets
on Ethereum mainnet).

Subgraph: https://thegraph.com/explorer/subgraphs/5nwMCSHaTqG3Kd2gHznbTXEnZ9QNWsssQfbHhDqQSQFp
"""

from __future__ import annotations

import logging
import os
from typing import Any

from ingestion.graph_client import GraphQLClient

logger = logging.getLogger(__name__)

_SUBGRAPH_ID = "5nwMCSHaTqG3Kd2gHznbTXEnZ9QNWsssQfbHhDqQSQFp"
_HOSTED_URL = "https://api.thegraph.com/subgraphs/name/messari/compound-v3-ethereum"
_GATEWAY_URL = f"https://gateway.thegraph.com/api/{{api_key}}/subgraphs/id/{_SUBGRAPH_ID}"

# ---------------------------------------------------------------------------
# GraphQL query
# ---------------------------------------------------------------------------
_POSITIONS_QUERY = """
query GetCompoundPositions($first: Int!, $skip: Int!) {
  positions(
    first: $first
    skip: $skip
    where: { borrowBalance_gt: "0" }
    orderBy: id
    orderDirection: asc
  ) {
    id
    account {
      id
    }
    market {
      id
      name
      inputToken {
        id
        symbol
        decimals
        lastPriceUSD
      }
    }
    # Collateral assets posted by this account
    collateralTokens {
      collateralToken {
        token {
          id
          symbol
          decimals
          lastPriceUSD
        }
        liquidationFactor
        collateralFactor
      }
      collateralBalance
    }
    borrowBalance
    depositBalance
    isCollateral
    rates {
      rate
      side
      type
    }
    timestampOpened
    timestampClosed
  }
}
"""


class CompoundClient:
    """
    Fetches open borrow positions from the Compound V3 Ethereum subgraph.
    """

    def __init__(self) -> None:
        api_key = os.getenv("GRAPH_API_KEY", "")
        url = _GATEWAY_URL if api_key else _HOSTED_URL
        self._client = GraphQLClient(subgraph_url=url)

    def fetch_borrow_positions(self) -> list[dict[str, Any]]:
        """
        Return all positions with non-zero borrow balances.

        Each record includes the account address, market info (base token,
        price), collateral breakdown per asset with liquidation factors, and
        total borrow balance.
        """
        logger.info("Fetching Compound V3 borrow positions...")
        records = self._client.paginate(
            query_template=_POSITIONS_QUERY,
            data_key="positions",
        )
        logger.info("Compound V3: fetched %d open positions.", len(records))
        return records
