"""
Unit tests for the GraphQL base client.

All HTTP calls are mocked with the ``responses`` library so no network
access is required.
"""

import json

import pytest
import responses as resp_mock

from ingestion.graph_client import GraphQLClient, GraphQLError, PAGE_SIZE

FAKE_URL = "https://api.thegraph.com/subgraphs/name/test/protocol"

SIMPLE_QUERY = "{ things(first: $first, skip: $skip) { id } }"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_response(data: dict, status: int = 200) -> dict:
    return {"data": data, "errors": None}


def register_page(
    data_key: str,
    items: list[dict],
    status: int = 200,
):
    """Register a single mocked page response."""
    resp_mock.add(
        resp_mock.POST,
        FAKE_URL,
        json=make_response({data_key: items}),
        status=status,
        content_type="application/json",
    )


# ---------------------------------------------------------------------------
# Tests: execute()
# ---------------------------------------------------------------------------

class TestExecute:
    @resp_mock.activate
    def test_returns_data_field(self):
        resp_mock.add(
            resp_mock.POST,
            FAKE_URL,
            json={"data": {"things": [{"id": "1"}]}},
            status=200,
        )
        client = GraphQLClient(FAKE_URL)
        result = client.execute("{ things { id } }")
        assert result == {"things": [{"id": "1"}]}

    @resp_mock.activate
    def test_raises_on_graphql_errors(self):
        resp_mock.add(
            resp_mock.POST,
            FAKE_URL,
            json={"data": None, "errors": [{"message": "Subgraph error"}]},
            status=200,
        )
        client = GraphQLClient(FAKE_URL)
        with pytest.raises(GraphQLError, match="Subgraph error"):
            client.execute("{ things { id } }")

    @resp_mock.activate
    def test_retries_on_connection_error_then_succeeds(self):
        # First call raises ConnectionError, second succeeds
        resp_mock.add(
            resp_mock.POST,
            FAKE_URL,
            body=ConnectionError("Network unreachable"),
        )
        resp_mock.add(
            resp_mock.POST,
            FAKE_URL,
            json={"data": {"things": [{"id": "abc"}]}},
            status=200,
        )
        client = GraphQLClient(FAKE_URL, max_retries=3, backoff_base=0.01)
        result = client.execute("{ things { id } }")
        assert result["things"][0]["id"] == "abc"

    @resp_mock.activate
    def test_raises_after_all_retries_exhausted(self):
        for _ in range(3):
            resp_mock.add(
                resp_mock.POST,
                FAKE_URL,
                body=ConnectionError("Always failing"),
            )
        client = GraphQLClient(FAKE_URL, max_retries=3, backoff_base=0.01)
        with pytest.raises(RuntimeError, match="retries exhausted"):
            client.execute("{ things { id } }")

    @resp_mock.activate
    def test_raises_on_non_retryable_4xx(self):
        resp_mock.add(
            resp_mock.POST,
            FAKE_URL,
            json={"error": "Forbidden"},
            status=403,
        )
        client = GraphQLClient(FAKE_URL, max_retries=1, backoff_base=0.01)
        with pytest.raises(Exception):
            client.execute("{ things { id } }")

    @resp_mock.activate
    def test_injects_api_key_into_url(self, monkeypatch):
        monkeypatch.setenv("GRAPH_API_KEY", "my-secret-key")
        url_template = "https://gateway.thegraph.com/api/{api_key}/subgraphs/id/abc"
        expected_url = "https://gateway.thegraph.com/api/my-secret-key/subgraphs/id/abc"
        resp_mock.add(
            resp_mock.POST,
            expected_url,
            json={"data": {"things": []}},
            status=200,
        )
        client = GraphQLClient(url_template)
        result = client.execute("{ things { id } }")
        assert result == {"things": []}


# ---------------------------------------------------------------------------
# Tests: paginate()
# ---------------------------------------------------------------------------

class TestPaginate:
    @resp_mock.activate
    def test_single_page_returned_fully(self):
        items = [{"id": str(i)} for i in range(5)]
        register_page("things", items)
        client = GraphQLClient(FAKE_URL)
        result = client.paginate(SIMPLE_QUERY, "things")
        assert len(result) == 5
        assert result[0]["id"] == "0"

    @resp_mock.activate
    def test_empty_result_returns_empty_list(self):
        register_page("things", [])
        client = GraphQLClient(FAKE_URL)
        result = client.paginate(SIMPLE_QUERY, "things")
        assert result == []

    @resp_mock.activate
    def test_multi_page_pagination(self):
        # First page: full PAGE_SIZE records
        page1 = [{"id": str(i)} for i in range(PAGE_SIZE)]
        # Second page: 3 records (less than PAGE_SIZE → last page)
        page2 = [{"id": str(i)} for i in range(PAGE_SIZE, PAGE_SIZE + 3)]
        register_page("things", page1)
        register_page("things", page2)

        client = GraphQLClient(FAKE_URL)
        result = client.paginate(SIMPLE_QUERY, "things")
        assert len(result) == PAGE_SIZE + 3

    @resp_mock.activate
    def test_stops_at_empty_second_page(self):
        page1 = [{"id": str(i)} for i in range(PAGE_SIZE)]
        register_page("things", page1)
        register_page("things", [])

        client = GraphQLClient(FAKE_URL)
        result = client.paginate(SIMPLE_QUERY, "things")
        assert len(result) == PAGE_SIZE

    @resp_mock.activate
    def test_extra_vars_are_passed(self):
        """Extra variables should be merged into the GraphQL request."""
        register_page("userReserves", [{"id": "1"}])
        client = GraphQLClient(FAKE_URL)
        result = client.paginate(SIMPLE_QUERY, "userReserves", extra_vars={"where": {"debt_gt": "0"}})
        assert len(result) == 1
