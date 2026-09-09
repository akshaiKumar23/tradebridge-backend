from decimal import Decimal
import logging
from db.dynamodb import get_equity_curve_table
from services import bulk

logger = logging.getLogger(__name__)


def save_equity_curve(user_id: str, equity_curve: list):
    if not equity_curve:
        logger.warning(f"No equity curve data to save for user_id={user_id}")
        return

    table = get_equity_curve_table()

    # Many trades close within the same second, and timestamp is the sort key.
    # The previous version kept the FIRST point for each second and discarded the
    # rest, which made the stored curve lag the real one and -- when the final
    # trade shared its second with an earlier one -- stopped it from ever
    # reaching the account's true closing balance. Equity is cumulative, so the
    # LAST point in a second is the correct one to keep.
    by_timestamp = {}
    duplicate_count = 0

    for point in equity_curve:
        try:
            timestamp = int(point["timestamp"])
            equity = Decimal(str(point["equity"]))
        except (KeyError, ValueError, TypeError) as e:
            logger.error(f"Invalid equity point for user_id={user_id}: {point}, error={e}")
            continue

        if timestamp in by_timestamp:
            duplicate_count += 1

        by_timestamp[timestamp] = {
            "user_id": user_id,
            "timestamp": timestamp,
            "equity": equity,
        }

    try:
        existing_keys, _ = bulk.collect_existing_keys(table, user_id, "timestamp")

        bulk.replace_partition(
            table, "timestamp", existing_keys, list(by_timestamp.values()),
            label=f"equity user_id={user_id}",
        )

        if duplicate_count > 0:
            logger.warning(
                f"Collapsed {duplicate_count} equity points sharing a timestamp "
                f"for user_id={user_id} (kept the latest of each)"
            )

        logger.info(f"Successfully saved {len(by_timestamp)} equity points for user_id={user_id}")
    except Exception as e:
        logger.exception(f"Failed to save equity curve for user_id={user_id}: {str(e)}")
        raise
