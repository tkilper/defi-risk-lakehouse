"""
MakerDAO ingestion client.

Queries the MakerDAO Ethereum subgraph for all active CDP vaults.
MakerDAO uses a vault/CDP model where users lock collateral to mint DAI.

Subgraph: https://thegraph.com/explorer/subgraphs/8sE6rTNkPhzZXZC6c8UQy2ghFTu5PPdGauwUBm4t7HZ1

Schema notes (Messari):
- Each vault is split into two position records per (account, market):
    side=COLLATERAL: balance = collateral locked
    side=BORROWER:   balance = DAI minted
- Balances are already in human units (no WAD/RAD conversion needed).
- market.liquidationThreshold is already normalised to a 0–1 decimal.
"""

from __future__ import annotations

import logging
import os
from typing import Any

from ingestion.graph_client import GraphQLClient

logger = logging.getLogger(__name__)

_SUBGRAPH_ID = "8sE6rTNkPhzZXZC6c8UQy2ghFTu5PPdGauwUBm4t7HZ1"
_HOSTED_URL = "https://api.thegraph.com/subgraphs/name/protofire/maker-protocol"
_GATEWAY_URL = f"https://gateway.thegraph.com/api/{{api_key}}/subgraphs/id/{_SUBGRAPH_ID}"

# ---------------------------------------------------------------------------
# GraphQL query
# ---------------------------------------------------------------------------
_POSITIONS_QUERY = """
query GetMakerPositions($first: Int!, $skip: Int!) {
  positions(
    first: $first
    skip: $skip
    where: { balance_gt: "0" }
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
      liquidationThreshold
    }
    balance
    side
    isCollateral
  }
}
"""


class MakerClient:
    """
    Fetches active CDP vaults from the MakerDAO Ethereum subgraph.
    """

    def __init__(self) -> None:
        api_key = os.getenv("GRAPH_API_KEY", "")
        url = _GATEWAY_URL if api_key else _HOSTED_URL
        self._client = GraphQLClient(subgraph_url=url)

    def fetch_active_vaults(self) -> list[dict[str, Any]]:
        """
        Return all positions (both COLLATERAL and BORROWER sides) with
        non-zero balances.

        The silver Spark job joins the two sides on (account, market) to
        reconstruct full vault views with collateral and debt.
        """
        logger.info("Fetching MakerDAO positions...")
        records = self._client.paginate(
            query_template=_POSITIONS_QUERY,
            data_key="positions",
        )
        logger.info("MakerDAO: fetched %d position records.", len(records))
        return records
