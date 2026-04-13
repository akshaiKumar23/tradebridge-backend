from decimal import Decimal
from datetime import datetime
from collections import defaultdict
import logging
from boto3.dynamodb.conditions import Key
from db.dynamodb import get_report_win_rate_table

logger = logging.getLogger(__name__)


def save_user_report_win_rate(user_id: str, trades: list):
    table = get_report_win_rate_table()

    # Delete all existing rows for this user
    items_to_delete = []
    last_key = None
    while True:
        kwargs = {
            "KeyConditionExpression": Key("user_id").eq(user_id),
            "ProjectionExpression": "user_id, period_key",
        }
        if last_key:
            kwargs["ExclusiveStartKey"] = last_key
        response = table.query(**kwargs)
        items_to_delete.extend(response.get("Items", []))
        last_key = response.get("LastEvaluatedKey")
        if not last_key:
            break

    with table.batch_writer() as batch:
        for item in items_to_delete:
            batch.delete_item(
                Key={"user_id": item["user_id"], "period_key": item["period_key"]})

    grouped = defaultdict(lambda: {"wins": 0, "losses": 0, "trades": 0})

    for trade in trades:
        symbol = trade["symbol"]
        dt = datetime.fromtimestamp(trade["close_time"])
        week_start = dt.strftime("%Y-W%U")
        key = f"{week_start}#{symbol}"

        grouped[key]["symbol"] = symbol
        grouped[key]["period_start"] = week_start
        grouped[key]["trades"] += 1

        if trade["pnl"] > 0:
            grouped[key]["wins"] += 1
        else:
            grouped[key]["losses"] += 1

    try:
        with table.batch_writer() as batch:
            for period_key, data in grouped.items():
                trade_count = data["trades"]
                wins = data["wins"]
                win_rate = (wins / trade_count) * 100 if trade_count > 0 else 0

                batch.put_item(Item={
                    "user_id": user_id,
                    "period_key": period_key,
                    "symbol": data["symbol"],
                    "period_start": data["period_start"],
                    "period_type": "weekly",
                    "wins": wins,           # fixed: was incorrectly set to `trades`
                    "losses": data["losses"],
                    "trades": trade_count,
                    "win_rate": Decimal(str(round(win_rate, 2))),
                    "created_at": datetime.utcnow().isoformat()
                })

        logger.info(f"Saved win rate chart data for user_id={user_id}")
    except Exception as e:
        logger.exception(f"Failed saving win rate chart: {str(e)}")
        raise
