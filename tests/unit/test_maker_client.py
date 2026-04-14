"""
Unit tests for the MakerDAO vault ingestion client.
"""

import responses as resp_mock
from ingestion.maker_client import _HOSTED_URL, MakerClient

# Two position records representing a single ETH-A vault (Messari schema).
FAKE_COLLATERAL_POSITION = {
    "id": "1-collateral",
    "account": {"id": "0xowner1"},
    "market": {
        "id": "0xmarket-eth-a",
        "name": "ETH-A",
        "inputToken": {
            "id": "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
            "symbol": "ETH",
            "decimals": 18,
            "lastPriceUsd": "3000.0",
        },
        "liquidationThreshold": "0.6667",
    },
    "balance": "5.0",
    "side": "COLLATERAL",
    "isCollateral": True,
}

FAKE_BORROWER_POSITION = {
    "id": "1-borrower",
    "account": {"id": "0xowner1"},
    "market": {
        "id": "0xmarket-eth-a",
        "name": "ETH-A",
        "inputToken": {
            "id": "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
            "symbol": "ETH",
            "decimals": 18,
            "lastPriceUsd": "3000.0",
        },
        "liquidationThreshold": "0.6667",
    },
    "balance": "10000.0",
    "side": "BORROWER",
    "isCollateral": False,
}


class TestMakerClientUrl:
    def test_uses_hosted_url_when_no_api_key(self, monkeymodule):
        monkeymodule.setenv("GRAPH_API_KEY", "")
        client = MakerClient()
        assert client._client.url == _HOSTED_URL


class TestMakerFetchVaults:
    @resp_mock.activate
    def test_returns_list_of_positions(self, monkeymodule):
        monkeymodule.setenv("GRAPH_API_KEY", "")
        resp_mock.add(
            resp_mock.POST,
            _HOSTED_URL,
            json={"data": {"positions": [FAKE_COLLATERAL_POSITION, FAKE_BORROWER_POSITION]}},
            status=200,
        )
        client = MakerClient()
        result = client.fetch_active_vaults()
        assert len(result) == 2
        sides = {r["side"] for r in result}
        assert sides == {"COLLATERAL", "BORROWER"}

    @resp_mock.activate
    def test_returns_empty_list_when_no_positions(self, monkeymodule):
        monkeymodule.setenv("GRAPH_API_KEY", "")
        resp_mock.add(
            resp_mock.POST,
            _HOSTED_URL,
            json={"data": {"positions": []}},
            status=200,
        )
        client = MakerClient()
        result = client.fetch_active_vaults()
        assert result == []
