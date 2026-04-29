"""
Backfill 90 days of liquidation events and position snapshots.

The Graph allows historical queries via block numbers / timestamps.
This script fetches events from 90 days ago to today in weekly batches
and writes them to MinIO, then triggers the Spark bronze/silver jobs.

Usage:
    python scripts/backfill_labels.py [--days 90] [--protocol aave]

After running, trigger the Spark jobs manually:
    make spark-bronze-liq
    make spark-silver-liq
    make spark-snapshots
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import UTC, datetime, timedelta

sys.path.insert(0, ".")

from ingestion.compound_liq_client import CompoundLiquidationClient
from ingestion.liquidation_client import AaveLiquidationClient
from ingestion.maker_liq_client import MakerLiquidationClient
from ingestion.s3_writer import write_raw_records

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s — %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)

# Batch size in days — queries The Graph in weekly windows to avoid timeouts
_BATCH_DAYS = 7


def backfill_protocol(
    protocol: str,
    client,
    start_ts: int,
    end_ts: int,
) -> int:
    """Fetch and store all liquidations for a protocol in weekly batches."""
    total = 0
    current = start_ts

    while current < end_ts:
        batch_end = min(current + _BATCH_DAYS * 86_400, end_ts)
        logger.info(
            "[%s] Fetching %s → %s",
            protocol,
            datetime.fromtimestamp(current, tz=UTC).date(),
            datetime.fromtimestamp(batch_end, tz=UTC).date(),
        )

        try:
            records = client.fetch_liquidations_in_range(current, batch_end)
        except Exception:
            logger.exception("[%s] Failed to fetch batch — skipping.", protocol)
            current = batch_end
            continue

        if records:
            snapshot_ts = datetime.fromtimestamp(current, tz=UTC)
            write_raw_records(f"liquidations/{protocol}", records, snapshot_ts=snapshot_ts)
            total += len(records)
            logger.info("[%s] Wrote %d records (total so far: %d).", protocol, len(records), total)

        current = batch_end

    logger.info("[%s] Backfill complete — %d total records.", protocol, total)
    return total


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill DeFi liquidation events")
    parser.add_argument("--days", type=int, default=90, help="Number of days to backfill")
    parser.add_argument(
        "--protocol",
        choices=["aave", "compound", "maker", "all"],
        default="all",
        help="Protocol to backfill",
    )
    args = parser.parse_args()

    end_dt = datetime.now(tz=UTC)
    start_dt = end_dt - timedelta(days=args.days)
    start_ts = int(start_dt.timestamp())
    end_ts = int(end_dt.timestamp())

    logger.info(
        "Backfilling %d days: %s → %s",
        args.days,
        start_dt.date(),
        end_dt.date(),
    )

    if args.protocol in ("aave", "all"):
        backfill_protocol("aave", AaveLiquidationClient(), start_ts, end_ts)

    if args.protocol in ("compound", "all"):
        try:
            backfill_protocol("compound", CompoundLiquidationClient(), start_ts, end_ts)
        except Exception:
            logger.warning("Compound backfill failed — subgraph may be unavailable.")

    if args.protocol in ("maker", "all"):
        try:
            backfill_protocol("maker", MakerLiquidationClient(), start_ts, end_ts)
        except Exception:
            logger.warning("Maker backfill failed — subgraph may be unavailable.")

    logger.info("Backfill script complete.")
    logger.info("Next steps:")
    logger.info("  make spark-bronze-liq   # load raw → nessie.bronze")
    logger.info("  make spark-silver-liq   # bronze → nessie.silver.liquidation_events")
    logger.info("  make spark-snapshots    # silver positions → position_snapshots")


if __name__ == "__main__":
    main()
