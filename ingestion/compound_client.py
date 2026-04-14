"""
Compound V3 (Comet) ingestion client.

Queries the Compound V3 subgraph for all positions with non-zero borrow
balances.  Compound V3 uses an isolated market model (USDC and ETH markets
on Ethereum mainnet).

Subgraph: https://thegraph.com/explorer/subgraphs/5nwMCSHaTqG3Kd2gHznbTXEnZ9QNWsssQfbHhDqQSQFp

Schema notes (Messari v2):
- Borrowers have negative accounting.basePrincipal; suppliers are positive.
- Collateral is in accounting.collateralBalances (not a top-level field).
- market.configuration.baseToken replaces the old market.inputToken.
- liquidateCollateralFactor is the liquidation trigger threshold;
  liquidationFactor is the penalty applied during liquidation.
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
    where: { accounting_: { basePrincipal_lt: "0" } }
    orderBy: id
    orderDirection: asc
  ) {
    id
    account {
      id
    }
    market {
      id
      configuration {
        baseToken {
          token {
            id
            symbol
            decimals
            lastPriceUsd
          }
        }
      }
    }
    accounting {
      basePrincipal
      collateralBalances {
        balance
        collateralToken {
          liquidateCollateralFactor
          liquidationFactor
          token {
            id
            symbol
            decimals
            lastPriceUsd
          }
        }
      }
    }
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
