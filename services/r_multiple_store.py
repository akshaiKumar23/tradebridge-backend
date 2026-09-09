from decimal import Decimal
import logging
from db.dynamodb import get_r_multiple_table
from services import bulk

logger = logging.getLogger(__name__)


def save_r_multiples(user_id: str, trades: list):

    table = get_r_multiple_table()

    existing_keys, _ = bulk.collect_existing_keys(table, user_id, "timestamp")

    if not trades:
        logger.warning(f"No r-multiples to save for user_id={user_id}")
        bulk.delete_keys(table, existing_keys)
        return

    # Several trades can close in the same second, and timestamp is the sort key,
    # so the last one written wins -- keep that explicit rather than leaving it to
    # batch ordering.
    by_timestamp = {}

    for trade in trades:
        by_timestamp[int(trade["timestamp"])] = {
            "user_id": user_id,
            "timestamp": int(trade["timestamp"]),
            "position_id": trade["position_id"],
            "symbol": trade["symbol"],
            "r_multiple": Decimal(str(trade["r_multiple"])),
            "pnl": Decimal(str(trade["pnl"])),
            "risk_amount": Decimal(str(trade["risk_amount"])),
        }

    bulk.replace_partition(
        table, "timestamp", existing_keys, list(by_timestamp.values()),
        label=f"r_multiples user_id={user_id}",
    )

    logger.info(f"Successfully saved {len(by_timestamp)} r-multiples for user_id={user_id}")
