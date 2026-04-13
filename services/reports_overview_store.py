from decimal import Decimal
from datetime import datetime, timedelta
from collections import defaultdict
import logging
from boto3.dynamodb.conditions import Key
from db.dynamodb import get_report_overview_table

logger = logging.getLogger(__name__)


def save_user_report_overview(user_id: str, trades: list):
    table = get_report_overview_table()

    # Delete all existing rows for this user
    items_to_delete = []
    last_key = None
    while True:
        kwargs = {
            "KeyConditionExpression": Key("user_id").eq(user_id),
            "ProjectionExpression": "user_id, week_start",
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
                Key={"user_id": item["user_id"], "week_start": item["week_start"]})

    weekly = defaultdict(lambda: {"net_pnl": 0, "trade_count": 0})

    for trade in trades:
        pnl = float(trade["pnl"])
        dt = datetime.fromtimestamp(trade["close_time"])
        week_start = (dt - timedelta(days=dt.weekday())).strftime("%Y-%m-%d")
        weekly[week_start]["net_pnl"] += pnl
        weekly[week_start]["trade_count"] += 1

    try:
        with table.batch_writer() as batch:
            for week_start, data in weekly.items():
                batch.put_item(Item={
                    "user_id": user_id,
                    "week_start": week_start,
                    "net_pnl": Decimal(str(round(data["net_pnl"], 2))),
                    "trade_count": int(data["trade_count"]),
                    "created_at": datetime.utcnow().isoformat()
                })

        logger.info(f"Saved report overview for user_id={user_id}")
    except Exception as e:
        logger.exception(f"Failed saving report overview: {str(e)}")
        raise
