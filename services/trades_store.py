from decimal import Decimal
import logging
from db.dynamodb import get_trades_table
from services import bulk

logger = logging.getLogger(__name__)

MIN_VALID_TIMESTAMP = 1577836800


def save_user_trades(user_id: str, trades: list):
    table = get_trades_table()

    # Step 1: Read the existing partition once -- both to know which rows to
    # clear and to carry the user's own tags across the rewrite. Tags are keyed
    # by position_id because the timestamp can shift when duplicates are
    # de-collided below, so it is not a stable handle.
    try:
        existing_keys, existing_items = bulk.collect_existing_keys(
            table, user_id, "timestamp", extra_fields=["position_id", "tags"]
        )

        existing_tags = {
            int(item["position_id"]): item["tags"]
            for item in existing_items
            if item.get("position_id") is not None and item.get("tags")
        }

    except Exception as e:
        logger.error(f"Failed to read existing trades for user_id={user_id}: {e}")
        raise

    # Step 2: If no new trades, clear the partition and stop -- same behaviour as
    # the delete-first version this replaced.
    if not trades:
        logger.warning(f"No trades to save for user_id={user_id}")
        bulk.delete_keys(table, existing_keys)
        return

    # Step 3: Build the fresh rows.
    #
    # timestamp is the sort key, so every row needs a distinct one. Trades close
    # in bursts within the same second, and the previous scheme counted
    # collisions per original timestamp -- five trades at T became T..T+4, which
    # then collided with the genuine trades at T+1 and T+2. Those duplicates used
    # to overwrite each other silently, one update_item at a time, quietly losing
    # trades; a batch write rejects them outright.
    #
    # Walk the trades in time order and hand out the next free second instead.
    # That is unique by construction and keeps the original ordering.
    ordered = sorted(
        trades, key=lambda t: (int(t["timestamp"]), int(t["position_id"]))
    )

    last_assigned = None
    items = []
    shifted_count = 0
    skipped_invalid = 0

    for trade in ordered:
        try:
            timestamp = int(trade["timestamp"])
            position_id = int(trade["position_id"])

            if timestamp < MIN_VALID_TIMESTAMP:
                logger.error(
                    f"Skipping trade with suspicious timestamp {timestamp} "
                    f"(position_id={position_id}) for user_id={user_id}"
                )
                skipped_invalid += 1
                continue

            # The real close time, kept as its own attribute so that shifting
            # the sort key below can never move a trade onto the wrong day.
            close_time = timestamp

            if last_assigned is not None and timestamp <= last_assigned:
                timestamp = last_assigned + 1
                shifted_count += 1

            last_assigned = timestamp

            items.append({
                "user_id": user_id,
                "timestamp": timestamp,
                "close_time": close_time,
                "position_id": position_id,
                "symbol": trade["symbol"],
                "direction": trade.get("direction", "LONG"),
                "entry_price": Decimal(str(
                    trade.get("entry_price") or trade.get("entry") or 0
                )),
                "exit_price": Decimal(str(
                    trade.get("exit_price") or trade.get("exit") or 0
                )),
                "volume": Decimal(str(trade["volume"])),
                "pnl": Decimal(str(trade["pnl"])),
                "r_multiple": Decimal(str(trade["r_multiple"])),
                "risk_amount": Decimal(str(trade["risk_amount"])),
                "tags": existing_tags.get(position_id, ["unreviewed"]),
            })

        except Exception as e:
            logger.error(
                f"Invalid trade data for user_id={user_id}, trade={trade}, error={e}"
            )

    # Step 4: Rewrite the partition. Rows whose key is being overwritten are left
    # in place rather than deleted and immediately rewritten; on a re-sync that
    # is nearly all of them.
    bulk.replace_partition(
        table, "timestamp", existing_keys, items, label=f"trades user_id={user_id}"
    )

    if skipped_invalid > 0:
        logger.warning(f"Skipped {skipped_invalid} trades with invalid timestamps for user_id={user_id}")

    if shifted_count > 0:
        logger.info(
            f"Shifted {shifted_count} trades onto free timestamps for "
            f"user_id={user_id} (trades closing within the same second)"
        )

    logger.info(f"Successfully saved {len(items)} trades for user_id={user_id}")
