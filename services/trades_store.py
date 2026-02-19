from decimal import Decimal
import logging

from db.dynamodb import get_trades_table


logger = logging.getLogger(__name__)


def save_user_trades(user_id: str, trades: list):


    if not trades:
        logger.warning(f"No trades to save for user_id={user_id}")
        return

    table = get_trades_table()

    seen_keys = set()
    duplicate_count = 0
    saved_count = 0

    try:
        with table.batch_writer(
            overwrite_by_pkeys=["user_id", "timestamp"]
        ) as batch:

            for trade in trades:

                try:
                    timestamp = int(trade["timestamp"])
                    position_id = int(trade["position_id"])

                    # Create unique key per batch
                    unique_key = (user_id, timestamp)

                    if unique_key in seen_keys:
                        duplicate_count += 1
                        continue

                    seen_keys.add(unique_key)

                    item = {
                        "user_id": user_id,
                        "timestamp": timestamp,
                        "position_id": position_id,
                        "symbol": trade["symbol"],
                        "direction": "LONG" if Decimal(str(trade["pnl"])) >= 0 else "SHORT",
                        "entry_price": Decimal("0"),
                        "exit_price": Decimal("0"),
                        "volume": Decimal(str(trade["volume"])),
                        "pnl": Decimal(str(trade["pnl"])),
                        "r_multiple": Decimal(str(trade["r_multiple"])),
                        "risk_amount": Decimal(str(trade["risk_amount"])),
                        "tags": ["MT5 Trade"],
                    }

                    batch.put_item(Item=item)
                    saved_count += 1

                except Exception as e:
                    logger.error(
                        f"Invalid trade data for user_id={user_id}, trade={trade}, error={e}"
                    )

        if duplicate_count > 0:
            logger.warning(
                f"Skipped {duplicate_count} duplicate trades for user_id={user_id}"
            )

        logger.info(
            f"Successfully saved {saved_count} trades for user_id={user_id}"
        )

    except Exception as e:
        logger.exception(
            f"Failed to save trades for user_id={user_id}: {str(e)}"
        )
        raise
