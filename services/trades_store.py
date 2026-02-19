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

    for trade in trades:
        try:
            timestamp = int(trade["timestamp"])
            position_id = int(trade["position_id"])

            unique_key = (user_id, timestamp)
            if unique_key in seen_keys:
                duplicate_count += 1
                continue
            seen_keys.add(unique_key)

            table.update_item(
                Key={
                    "user_id": user_id,
                    "timestamp": timestamp,
                },
                UpdateExpression="""
                    SET position_id = :pid,
                        symbol = :sym,
                        direction = :dir,
                        entry_price = :entry,
                        exit_price = :exit,
                        volume = :vol,
                        pnl = :pnl,
                        r_multiple = :r,
                        risk_amount = :risk,
                        tags = if_not_exists(tags, :default_tags)
                """,
                ExpressionAttributeValues={
                    ":pid": position_id,
                    ":sym": trade["symbol"],
                    ":dir": "LONG" if Decimal(str(trade["pnl"])) >= 0 else "SHORT",
                    ":entry": Decimal("0"),
                    ":exit": Decimal("0"),
                    ":vol": Decimal(str(trade["volume"])),
                    ":pnl": Decimal(str(trade["pnl"])),
                    ":r": Decimal(str(trade["r_multiple"])),
                    ":risk": Decimal(str(trade["risk_amount"])),
                    ":default_tags": ["MT5 Trade"],
                }
            )
            saved_count += 1

        except Exception as e:
            logger.error(
                f"Invalid trade data for user_id={user_id}, trade={trade}, error={e}"
            )

    if duplicate_count > 0:
        logger.warning(f"Skipped {duplicate_count} duplicate trades for user_id={user_id}")

    logger.info(f"Successfully saved {saved_count} trades for user_id={user_id}")