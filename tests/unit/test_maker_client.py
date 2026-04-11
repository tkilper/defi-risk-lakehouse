"""
Unit tests for the MakerDAO vault ingestion client and unit conversions.
"""

import pytest
import responses as resp_mock
from ingestion.maker_client import _HOSTED_URL, MakerClient

FAKE_VAULT = {
    "id": "1",
    "cdpId": "1",
    "owner": {"id": "0xowner1"},
    "collateralType": {
        "id": "ETH-A",
        "liquidationRatio": "1.5",
        "rate": "1050000000000000000000000000",  # 1.05 in 1e27 units
        "price": {"value": "3000.0"},
    },
    "collateral": "5000000000000000000",  # 5 ETH in WAD
    "debt": "10000000000000000000",  # 10 DAI in normalized units
    "openedAt": "1620000000",
    "updatedAt": "1650000000",
}


class TestMakerClientUrl:
    def test_uses_hosted_url_when_no_api_key(self, monkeypatch):
        monkeypatch.setenv("GRAPH_API_KEY", "")
        client = MakerClient()
        assert client._client.url == _HOSTED_URL


class TestMakerFetchVaults:
    @resp_mock.activate
    def test_returns_list_of_vaults(self, monkeypatch):
        monkeypatch.setenv("GRAPH_API_KEY", "")
        resp_mock.add(
            resp_mock.POST,
            _HOSTED_URL,
            json={"data": {"vaults": [FAKE_VAULT]}},
            status=200,
        )
        client = MakerClient()
        result = client.fetch_active_vaults()
        assert len(result) == 1
        assert result[0]["cdpId"] == "1"
        assert result[0]["collateralType"]["id"] == "ETH-A"

    @resp_mock.activate
    def test_returns_empty_list_when_no_vaults(self, monkeypatch):
        monkeypatch.setenv("GRAPH_API_KEY", "")
        resp_mock.add(
            resp_mock.POST,
            _HOSTED_URL,
            json={"data": {"vaults": []}},
            status=200,
        )
        client = MakerClient()
        result = client.fetch_active_vaults()
        assert result == []


class TestMakerUnitConversions:
    """
    Test the WAD / RAD / rate unit conversions documented in maker_client.py.
    These formulas are also applied in the Spark silver transformer.
    """

    WAD = 1e18
    RAY = 1e27  # Maker uses 1e27 for rates

    def test_wad_to_human_collateral(self):
        # 5 ETH stored as WAD
        raw = 5_000_000_000_000_000_000  # 5e18
        human = raw / self.WAD
        assert human == pytest.approx(5.0)

    def test_dai_debt_computation(self):
        # art = 10 (normalised debt), rate = 1.05 (in 1e27 units)
        art = 10_000_000_000_000_000_000  # 10 DAI normalised, WAD
        rate = 1_050_000_000_000_000_000_000_000_000  # 1.05 in RAY
        actual_dai = (art / self.WAD) * (rate / self.RAY)
        assert actual_dai == pytest.approx(10.5)  # 10 * 1.05 = 10.5 DAI

    def test_liquidation_threshold_from_ratio(self):
        # ETH-A: liquidationRatio = 1.5 → threshold = 1/1.5 ≈ 0.667
        liq_ratio = 1.5
        threshold = 1.0 / liq_ratio
        assert threshold == pytest.approx(0.6667, abs=0.0001)

    def test_collateral_usd_value(self):
        # 5 ETH at $3000/ETH → $15,000
        collateral_raw = 5_000_000_000_000_000_000  # 5 ETH in WAD
        price_usd = 3_000.0
        usd_value = (collateral_raw / self.WAD) * price_usd
        assert usd_value == pytest.approx(15_000.0)
