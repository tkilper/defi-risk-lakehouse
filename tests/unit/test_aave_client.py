"""
Unit tests for the Aave V3 ingestion client.

Tests that the AaveClient constructs the correct URL based on env vars
and that it properly delegates pagination to GraphQLClient.
"""

import responses as resp_mock
from ingestion.aave_client import _GATEWAY_URL, _HOSTED_URL, _SUBGRAPH_ID, AaveClient

FAKE_POSITION = {
    "id": "0xabc-0xdef",
    "user": {"id": "0xabc"},
    "reserve": {
        "id": "0xdef",
        "symbol": "WETH",
        "name": "Wrapped Ether",
        "decimals": 18,
        "underlyingAsset": "0xdef",
        "usageAsCollateralEnabled": True,
        "reserveLiquidationThreshold": "8000",
        "baseLTVasCollateral": "7500",
        "reserveLiquidationBonus": "10500",
        "price": {"priceInEth": "1000000000000000000"},
        "totalLiquidity": "500000000000000000000",
        "availableLiquidity": "200000000000000000000",
        "utilizationRate": "0.6",
    },
    "currentATokenBalance": "2000000000000000000",
    "currentVariableDebt": "1000000000000000000",
    "currentStableDebt": "0",
    "usageAsCollateralEnabledOnUser": True,
    "liquidityRate": "0.03",
    "variableBorrowRate": "0.05",
    "stableBorrowRate": "0.04",
}


class TestAaveClientUrl:
    def test_uses_hosted_url_when_no_api_key(self, monkeymodule):
        monkeymodule.setenv("GRAPH_API_KEY", "")
        client = AaveClient()
        assert client._client.url == _HOSTED_URL

    def test_uses_gateway_url_when_api_key_set(self, monkeymodule):
        monkeymodule.setenv("GRAPH_API_KEY", "test-key-123")
        client = AaveClient()
        expected_url = _GATEWAY_URL.replace("{api_key}", "test-key-123")
        assert client._client.url == expected_url
        assert _SUBGRAPH_ID in expected_url


class TestAaveFetchPositions:
    @resp_mock.activate
    def test_returns_list_of_positions(self, monkeymodule):
        monkeymodule.setenv("GRAPH_API_KEY", "")
        resp_mock.add(
            resp_mock.POST,
            _HOSTED_URL,
            json={"data": {"userReserves": [FAKE_POSITION]}},
            status=200,
        )
        client = AaveClient()
        result = client.fetch_borrow_positions()
        assert len(result) == 1
        assert result[0]["user"]["id"] == "0xabc"
        assert result[0]["reserve"]["symbol"] == "WETH"

    @resp_mock.activate
    def test_returns_empty_list_when_no_positions(self, monkeymodule):
        monkeymodule.setenv("GRAPH_API_KEY", "")
        resp_mock.add(
            resp_mock.POST,
            _HOSTED_URL,
            json={"data": {"userReserves": []}},
            status=200,
        )
        client = AaveClient()
        result = client.fetch_borrow_positions()
        assert result == []

    @resp_mock.activate
    def test_includes_debt_fields(self, monkeymodule):
        monkeymodule.setenv("GRAPH_API_KEY", "")
        resp_mock.add(
            resp_mock.POST,
            _HOSTED_URL,
            json={"data": {"userReserves": [FAKE_POSITION]}},
            status=200,
        )
        client = AaveClient()
        result = client.fetch_borrow_positions()
        pos = result[0]
        assert "currentVariableDebt" in pos
        assert "currentATokenBalance" in pos
        assert pos["reserve"]["reserveLiquidationThreshold"] == "8000"
