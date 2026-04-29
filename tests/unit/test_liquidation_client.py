"""
Unit tests for the liquidation ingestion clients.

Tests are mocked at the HTTP layer — no real subgraph calls are made.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
import requests

from ingestion.liquidation_client import AaveLiquidationClient


class TestGraphQLPagination:
    """Test that the base GraphQL client paginates correctly."""

    def _mock_paginated_responses(self, total_records: int, page_size: int = 1000):
        """Build a list of mock Response objects simulating paginated API calls."""
        responses = []
        remaining = total_records
        while remaining > 0:
            batch_size = min(remaining, page_size)
            page_data = [{"id": f"liq_{i}"} for i in range(batch_size)]
            mock_resp = MagicMock()
            mock_resp.status_code = 200
            mock_resp.json.return_value = {
                "data": {"liquidationCalls": page_data}
            }
            mock_resp.raise_for_status = MagicMock()
            responses.append(mock_resp)
            remaining -= batch_size

        # Final empty page
        empty_resp = MagicMock()
        empty_resp.status_code = 200
        empty_resp.json.return_value = {"data": {"liquidationCalls": []}}
        empty_resp.raise_for_status = MagicMock()
        responses.append(empty_resp)

        return responses

    @patch("ingestion.graph_client.requests.Session.post")
    def test_pagination_yields_all_records(self, mock_post):
        """Pagination should fetch all records across multiple pages."""
        total = 2500
        mock_post.side_effect = self._mock_paginated_responses(total)

        client = AaveLiquidationClient()
        records = client.fetch_liquidations()

        assert len(records) == total

    @patch("ingestion.graph_client.requests.Session.post")
    def test_single_page_result(self, mock_post):
        """When fewer than PAGE_SIZE records exist, only one request should be needed."""
        total = 42
        mock_post.side_effect = self._mock_paginated_responses(total)

        client = AaveLiquidationClient()
        records = client.fetch_liquidations()

        assert len(records) == total
        # The paginator stops after an undersized page (no second request needed)
        assert mock_post.call_count == 1


class TestRetryOnRateLimit:
    """Test that the client retries on 429 responses."""

    @patch("ingestion.graph_client.time.sleep")
    @patch("ingestion.graph_client.requests.Session.post")
    def test_retry_on_429_then_success(self, mock_post, mock_sleep):
        """After a 429, the client should back off and succeed on the next attempt."""
        # First call: 429
        rate_limited_resp = MagicMock()
        rate_limited_resp.status_code = 429

        # Second call: success with empty result (end of pagination)
        ok_resp = MagicMock()
        ok_resp.status_code = 200
        ok_resp.json.return_value = {"data": {"liquidationCalls": [{"id": "liq_0"}]}}
        ok_resp.raise_for_status = MagicMock()

        empty_resp = MagicMock()
        empty_resp.status_code = 200
        empty_resp.json.return_value = {"data": {"liquidationCalls": []}}
        empty_resp.raise_for_status = MagicMock()

        mock_post.side_effect = [rate_limited_resp, ok_resp, empty_resp]

        client = AaveLiquidationClient()
        records = client.fetch_liquidations()

        assert len(records) == 1
        # sleep should have been called once (after the 429)
        assert mock_sleep.call_count >= 1

    @patch("ingestion.graph_client.time.sleep")
    @patch("ingestion.graph_client.requests.Session.post")
    def test_all_retries_exhausted_raises(self, mock_post, mock_sleep):
        """After all retries are exhausted, a RuntimeError should be raised."""
        rate_limited_resp = MagicMock()
        rate_limited_resp.status_code = 429
        # Always return 429
        mock_post.return_value = rate_limited_resp

        client = AaveLiquidationClient()

        with pytest.raises(RuntimeError, match="retries exhausted"):
            client.fetch_liquidations()


class TestRangeQuery:
    """Test the historical backfill query with timestamp filters."""

    @patch("ingestion.graph_client.requests.Session.post")
    def test_range_query_passes_timestamps(self, mock_post):
        """The range query should pass from_ts and to_ts in the variables."""
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {"data": {"liquidationCalls": []}}
        mock_resp.raise_for_status = MagicMock()
        mock_post.return_value = mock_resp

        client = AaveLiquidationClient()
        client.fetch_liquidations_in_range(from_ts=1700000000, to_ts=1700086400)

        call_payload = mock_post.call_args.kwargs["json"]
        assert call_payload["variables"]["from_ts"] == 1700000000
        assert call_payload["variables"]["to_ts"] == 1700086400
