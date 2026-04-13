"""
MakerDAO ingestion client.

Queries the MakerDAO Ethereum subgraph for all active CDP vaults.
MakerDAO uses a vault/CDP model where users lock collateral to mint DAI.

Subgraph: https://thegraph.com/explorer/subgraphs/G1KHEQkA7shnPZahHUmKcerfpqBMNnL9xJzWGAVrj7n7

Key units
---------
- collateral: WAD (1e18 scaled integer)
- debt (art): DAI normalised debt units; multiply by collateral type ``rate``
  to get actual DAI owed
- liquidationRatio: decimal (e.g. 1.5 = 150% collateralisation required)
- price: USD price of the collateral asset from the MakerDAO oracle
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
_VAULTS_QUERY = """
query GetMakerVaults($first: Int!, $skip: Int!) {
  vaults(
    first: $first
    skip: $skip
    where: {
      collateral_gt: "0"
      debt_gt: "0"
    }
    orderBy: id
    orderDirection: asc
  ) {
    id
    cdpId
    owner {
      id
    }
    collateralType {
      id
      # e.g. "ETH-A", "WBTC-B", "USDC-A"
      liquidationRatio
      # Stability fee accumulator — multiply art by rate to get DAI owed
      rate
      # Current oracle price (MakerDAO Next Price, USD)
      price {
        value
      }
    }
    # Raw collateral locked in the vault (WAD = 1e18 units)
    collateral
    # Normalised debt (DAI, art units). Actual DAI = art * collateralType.rate
    debt
    openedAt
    updatedAt
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
        Return all vaults with non-zero collateral and debt.

        Each record includes the owner address, collateral type (ilk) with
        liquidation ratio and current oracle price, and the raw collateral /
        debt amounts that the silver Spark job converts to USD.
        """
        logger.info("Fetching MakerDAO active vaults...")
        records = self._client.paginate(
            query_template=_VAULTS_QUERY,
            data_key="vaults",
        )
        logger.info("MakerDAO: fetched %d active vaults.", len(records))
        return records
