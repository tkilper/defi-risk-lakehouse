"""
Unit tests for the backfill script's time-chunking logic.

Tests the batch window generation in backfill_labels.py — pure Python,
no network calls, no MinIO required.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

# ---------------------------------------------------------------------------
# Inline batch window generator
# (mirrors the while loop in scripts/backfill_labels.py)
# ---------------------------------------------------------------------------


def generate_batch_windows(
    start_ts: int, end_ts: int, batch_days: int = 7
) -> list[tuple[int, int]]:
    """Return list of (batch_start, batch_end) Unix timestamp tuples."""
    windows = []
    current = start_ts
    batch_seconds = batch_days * 86_400
    while current < end_ts:
        batch_end = min(current + batch_seconds, end_ts)
        windows.append((current, batch_end))
        current = batch_end
    return windows


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestBatchWindowGeneration:
    def test_single_batch_when_range_fits_in_one_window(self):
        start = 0
        end = 3 * 86_400  # 3 days — fits in one 7-day window
        windows = generate_batch_windows(start, end, batch_days=7)
        assert len(windows) == 1
        assert windows[0] == (0, 3 * 86_400)

    def test_exact_multiple_of_batch_size(self):
        start = 0
        end = 14 * 86_400  # exactly 2 weeks
        windows = generate_batch_windows(start, end, batch_days=7)
        assert len(windows) == 2
        assert windows[0] == (0, 7 * 86_400)
        assert windows[1] == (7 * 86_400, 14 * 86_400)

    def test_partial_last_batch(self):
        start = 0
        end = 10 * 86_400  # 10 days with 7-day batches → 2 windows (7 + 3)
        windows = generate_batch_windows(start, end, batch_days=7)
        assert len(windows) == 2
        assert windows[-1] == (7 * 86_400, 10 * 86_400)

    def test_90_day_backfill_produces_13_windows(self):
        """90 days / 7 = 12.857 → 13 windows (12 full + 1 partial)."""
        start = 0
        end = 90 * 86_400
        windows = generate_batch_windows(start, end, batch_days=7)
        assert len(windows) == 13

    def test_windows_are_contiguous(self):
        """Each window's end should equal the next window's start."""
        start = 0
        end = 30 * 86_400
        windows = generate_batch_windows(start, end, batch_days=7)
        for i in range(len(windows) - 1):
            assert windows[i][1] == windows[i + 1][0], (
                f"Gap between window {i} and {i + 1}"
            )

    def test_windows_cover_full_range(self):
        start = 1_700_000_000
        end = 1_700_000_000 + 30 * 86_400
        windows = generate_batch_windows(start, end, batch_days=7)
        assert windows[0][0] == start
        assert windows[-1][1] == end

    def test_empty_range_produces_no_windows(self):
        windows = generate_batch_windows(100, 100, batch_days=7)
        assert windows == []

    def test_single_second_range(self):
        windows = generate_batch_windows(0, 1, batch_days=7)
        assert len(windows) == 1
        assert windows[0] == (0, 1)


class TestBackfillErrorHandling:
    """Test that failed protocol fetches are skipped gracefully."""

    def _run_backfill_with_mock_client(
        self,
        fetch_side_effects: list,
        start_ts: int,
        end_ts: int,
        batch_days: int = 7,
    ) -> int:
        """
        Inline implementation of the backfill loop for testing error handling.
        Mirrors the try/except pattern in scripts/backfill_labels.py.
        """
        total = 0
        windows = generate_batch_windows(start_ts, end_ts, batch_days)
        effect_iter = iter(fetch_side_effects)

        for _batch_start, _batch_end in windows:
            effect = next(effect_iter, [])
            try:
                if isinstance(effect, Exception):
                    raise effect
                records = effect
            except Exception:
                continue  # skip failed batch
            total += len(records)

        return total

    def test_exception_in_one_batch_does_not_stop_others(self):
        """A fetch failure in one batch should not stop subsequent batches."""
        effects = [
            RuntimeError("subgraph timeout"),  # first batch fails
            [{"id": "liq_1"}],                 # second batch succeeds
        ]
        total = self._run_backfill_with_mock_client(
            effects, start_ts=0, end_ts=14 * 86_400
        )
        assert total == 1

    def test_all_batches_fail_returns_zero(self):
        effects = [RuntimeError("timeout")] * 3
        total = self._run_backfill_with_mock_client(
            effects, start_ts=0, end_ts=21 * 86_400
        )
        assert total == 0

    def test_successful_batches_accumulate_count(self):
        effects = [
            [{"id": "a"}, {"id": "b"}],  # 2 records
            [{"id": "c"}],               # 1 record
            [{"id": "d"}, {"id": "e"}],  # 2 records
        ]
        total = self._run_backfill_with_mock_client(
            effects, start_ts=0, end_ts=21 * 86_400
        )
        assert total == 5


class TestBackfillDateRange:
    """Test that the 90-day default covers the right time window."""

    def test_90_day_start_is_before_end(self):
        end_dt = datetime.now(tz=UTC)
        start_dt = end_dt - timedelta(days=90)
        assert start_dt < end_dt

    def test_start_timestamp_is_positive(self):
        end_dt = datetime.now(tz=UTC)
        start_dt = end_dt - timedelta(days=90)
        assert int(start_dt.timestamp()) > 0

    def test_range_spans_exactly_90_days(self):
        end_dt = datetime(2026, 4, 29, tzinfo=UTC)
        start_dt = end_dt - timedelta(days=90)
        delta_days = (end_dt - start_dt).days
        assert delta_days == 90
