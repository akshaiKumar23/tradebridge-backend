from decimal import Decimal
from datetime import datetime
from collections import defaultdict
from boto3.dynamodb.conditions import Key
import logging
from db.dynamodb import get_dashboard_daily_pnl_table

logger = logging.getLogger(__name__)

BASELINE = 30000


def save_dashboard_daily_pnl(user_id: str, trades: list):
    table = get_dashboard_daily_pnl_table()

    # Delete all existing rows for this user
    items_to_delete = []
    last_key = None
    while True:
        kwargs = {
            "KeyConditionExpression": Key("user_id").eq(user_id),
            "ProjectionExpression": "user_id, #d",
            "ExpressionAttributeNames": {"#d": "date"},
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
            batch.delete_item(Key={"user_id": item["user_id"], "date": item["date"]})

    if not trades:
        logger.warning(f"No trades to save dashboard daily pnl for user_id={user_id}")
        return

    daily = defaultdict(float)
    for trade in trades:
        date = datetime.fromtimestamp(trade["close_time"]).strftime("%Y-%m-%d")
        daily[date] += float(trade["pnl"])

    try:
        with table.batch_writer() as batch:
            for date, pnl in daily.items():
                batch.put_item(Item={
                    "user_id": user_id,
                    "date": date,
                    "base": Decimal(str(BASELINE)),
                    "profit": Decimal(str(pnl)) if pnl > 0 else Decimal("0"),
                    "loss": Decimal(str(pnl)) if pnl < 0 else Decimal("0"),
                    "created_at": datetime.utcnow().isoformat()
                })
        logger.info(f"Saved dashboard daily pnl for user_id={user_id}")
    except Exception as e:
        logger.exception(f"Failed saving dashboard daily pnl: {str(e)}")
        raise