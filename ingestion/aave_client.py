"""
Aave V3 ingestion client.

Queries the Aave V3 Ethereum subgraph on The Graph for all open borrow
positions (userReserves with non-zero debt).  Returns raw position dicts
that are later written to MinIO and normalised by the Spark silver job.

Subgraph: https://thegraph.com/explorer/subgraphs/Cd2gEDVeqnjBn1hSeqFMitw8Q1iiyV9FYUZkLNRcL87g
"""

from __future__ import annotations

import logging
import os
from typing import Any

from ingestion.graph_client import GraphQLClient

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Subgraph endpoint
# Use the decentralized network (requires GRAPH_API_KEY) or fall back to the
# hosted service for development / zero-config usage.
# ---------------------------------------------------------------------------
_SUBGRAPH_ID = "Cd2gEDVeqnjBn1hSeqFMitw8Q1iiyV9FYUZkLNRcL87g"
_HOSTED_URL = "https://api.thegraph.com/subgraphs/name/aave/protocol-v3"
_GATEWAY_URL = f"https://gateway.thegraph.com/api/{{api_key}}/subgraphs/id/{_SUBGRAPH_ID}"

# ---------------------------------------------------------------------------
# GraphQL query — all userReserves with outstanding debt
# ---------------------------------------------------------------------------
_BORROW_POSITIONS_QUERY = """
query GetBorrowPositions($first: Int!, $skip: Int!) {
  userReserves(
    first: $first
    skip: $skip
    where: {
      or: [
        { currentVariableDebt_gt: "0" }
        { currentStableDebt_gt: "0" }
      ]
    }
    orderBy: id
    orderDirection: asc
  ) {
    id
    user {
      id
    }
    reserve {
      id
      symbol
      name
      decimals
      underlyingAsset
      # Liquidation threshold in basis points (e.g. 8000 = 80.00%)
      reserveLiquidationThreshold
      # Loan-to-value ratio in basis points
      baseLTVasCollateral
      # Liquidation bonus in basis points (e.g. 10500 = 5% bonus)
      reserveLiquidationBonus
      # USD price from Chainlink oracle (scaled by 1e8)
      price {
        priceInEth
      }
      totalLiquidity
      availableLiquidity
      utilizationRate
    }
    # Collateral balance (in underlying asset units, scaled by the aToken index)
    currentATokenBalance
    # Outstanding debt
    currentVariableDebt
    currentStableDebt
    # True only if the user has explicitly enabled this reserve as collateral
    usageAsCollateralEnabledOnUser
    liquidityRate
    variableBorrowRate
    stableBorrowRate
  }
}
"""


class AaveClient:
    """
    Fetches open borrow positions from the Aave V3 Ethereum subgraph.

    If ``GRAPH_API_KEY`` is set, uses the decentralized network endpoint;
    otherwise falls back to the hosted service.
    """

    def __init__(self) -> None:
        api_key = os.getenv("GRAPH_API_KEY", "")
        url = _GATEWAY_URL if api_key else _HOSTED_URL
        self._client = GraphQLClient(subgraph_url=url)

    def fetch_borrow_positions(self) -> list[dict[str, Any]]:
        """
        Return all userReserves with non-zero variable or stable debt.

        Each record contains user address, reserve metadata (symbol, decimals,
        liquidation threshold, price), collateral balance, and debt balances.
        """
        logger.info("Fetching Aave V3 borrow positions...")
        records = self._client.paginate(
            query_template=_BORROW_POSITIONS_QUERY,
            data_key="userReserves",
        )
        logger.info("Aave: fetched %d open positions.", len(records))
        return records
