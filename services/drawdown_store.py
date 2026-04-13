from decimal import Decimal
from datetime import datetime
import logging
from boto3.dynamodb.conditions import Key
from db.dynamodb import get_drawdown_curve_table

logger = logging.getLogger(__name__)


def save_drawdown_curve(user_id: str, equity_curve: list):
    table = get_drawdown_curve_table()

    if not equity_curve:
        logger.warning(f"No equity curve for user_id={user_id}")
        return

    # Delete all existing rows for this user
    items_to_delete = []
    last_key = None
    while True:
        kwargs = {
            "KeyConditionExpression": Key("user_id").eq(user_id),
            "ProjectionExpression": "user_id, #ts",
            "ExpressionAttributeNames": {"#ts": "timestamp"},
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
            batch.delete_item(Key={"user_id": item["user_id"], "timestamp": item["timestamp"]})

    peak = 0
    seen_timestamps = set()
    duplicate_count = 0

    try:
        with table.batch_writer() as batch:
            for point in equity_curve:
                timestamp = int(point["timestamp"])
                equity = float(point["equity"])

                if timestamp in seen_timestamps:
                    duplicate_count += 1
                    continue

                seen_timestamps.add(timestamp)

                if equity > peak:
                    peak = equity

                drawdown = peak - equity

                batch.put_item(Item={
                    "user_id": user_id,
                    "timestamp": timestamp,
                    "equity": Decimal(str(equity)),
                    "peak_equity": Decimal(str(peak)),
                    "drawdown": Decimal(str(round(drawdown, 2))),
                    "created_at": datetime.utcnow().isoformat()
                })

        if duplicate_count > 0:
            logger.warning(f"Removed {duplicate_count} duplicate drawdown points for user_id={user_id}")

        logger.info(f"Saved drawdown curve for user_id={user_id}")
    except Exception as e:
        logger.exception(f"Failed saving drawdown curve: {str(e)}")
        raise