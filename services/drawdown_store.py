from decimal import Decimal
from datetime import datetime
import logging
from db.dynamodb import get_drawdown_curve_table
from services import bulk

logger = logging.getLogger(__name__)


def save_drawdown_curve(user_id: str, equity_curve: list):
    table = get_drawdown_curve_table()

    if not equity_curve:
        logger.warning(f"No equity curve for user_id={user_id}")
        return

    # The running peak has to see EVERY point. The previous version skipped
    # points sharing a timestamp before updating the peak, so on an account where
    # most trades close within the same second the peak -- and therefore the
    # drawdown -- was computed from a small fraction of the curve and understated
    # the real worst case. Track the peak across the full curve, then collapse to
    # one row per timestamp keeping the latest, since equity is cumulative.
    peak = 0
    by_timestamp = {}
    duplicate_count = 0
    created_at = datetime.utcnow().isoformat()

    for point in equity_curve:
        try:
            timestamp = int(point["timestamp"])
            equity = float(point["equity"])
        except (KeyError, ValueError, TypeError) as e:
            logger.error(f"Invalid equity point for user_id={user_id}: {point}, error={e}")
            continue

        if equity > peak:
            peak = equity

        if timestamp in by_timestamp:
            duplicate_count += 1

        by_timestamp[timestamp] = {
            "user_id": user_id,
            "timestamp": timestamp,
            "equity": Decimal(str(equity)),
            "peak_equity": Decimal(str(peak)),
            "drawdown": Decimal(str(round(peak - equity, 2))),
            "created_at": created_at,
        }

    try:
        existing_keys, _ = bulk.collect_existing_keys(table, user_id, "timestamp")

        bulk.replace_partition(
            table, "timestamp", existing_keys, list(by_timestamp.values()),
            label=f"drawdown user_id={user_id}",
        )

        if duplicate_count > 0:
            logger.warning(
                f"Collapsed {duplicate_count} drawdown points sharing a timestamp "
                f"for user_id={user_id} (kept the latest of each)"
            )

        logger.info(f"Saved {len(by_timestamp)} drawdown points for user_id={user_id}")
    except Exception as e:
        logger.exception(f"Failed saving drawdown curve: {str(e)}")
        raise
