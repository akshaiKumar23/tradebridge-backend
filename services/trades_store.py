from decimal import Decimal
import logging
from db.dynamodb import get_trades_table

logger = logging.getLogger(__name__)

# Minimum valid timestamp: Jan 1, 2020 00:00:00 UTC
MIN_VALID_TIMESTAMP = 1577836800


def save_user_trades(user_id: str, trades: list):
    if not trades:
        logger.warning(f"No trades to save for user_id={user_id}")
        return

    table = get_trades_table()
    seen_keys = set()
    duplicate_count = 0
    saved_count = 0
    skipped_invalid = 0

    for trade in trades:
        try:
            timestamp = int(trade["timestamp"])
            position_id = int(trade["position_id"])

            # Sanity check: reject timestamps that are clearly wrong
            # (e.g. position_id accidentally used as timestamp)
            if timestamp < MIN_VALID_TIMESTAMP:
                logger.error(
                    f"Skipping trade with suspicious timestamp {timestamp} "
                    f"(position_id={position_id}) for user_id={user_id}"
                )
                skipped_invalid += 1
                continue

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
                    ":dir": trade.get("direction", "LONG"),
                    ":entry": Decimal(str(
                        trade.get("entry_price") or trade.get("entry") or 0
                    )),
                    ":exit": Decimal(str(
                        trade.get("exit_price") or trade.get("exit") or 0
                    )),
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

    if skipped_invalid > 0:
        logger.warning(f"Skipped {skipped_invalid} trades with invalid timestamps for user_id={user_id}")

    if duplicate_count > 0:
        logger.warning(f"Skipped {duplicate_count} duplicate trades for user_id={user_id}")

    logger.info(f"Successfully saved {saved_count} trades for user_id={user_id}")