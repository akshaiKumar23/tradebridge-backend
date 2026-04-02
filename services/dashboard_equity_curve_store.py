from decimal import Decimal
from datetime import datetime
from boto3.dynamodb.conditions import Key
import logging
from db.dynamodb import get_dashboard_equity_curve_table

logger = logging.getLogger(__name__)


def save_dashboard_equity_curve(user_id: str, equity_curve: list):
    table = get_dashboard_equity_curve_table()

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

    if not equity_curve:
        logger.warning(f"No equity curve for user_id={user_id}")
        return

    # Deduplicate by date — keep the last equity value for each date
    # (equity_curve is already sorted by timestamp from mt5_logic)
    deduped = {}
    for point in equity_curve:
        date = datetime.fromtimestamp(point["timestamp"]).strftime("%Y-%m-%d")
        deduped[date] = point["equity"]  # later points overwrite earlier ones

    try:
        with table.batch_writer() as batch:
            for date, equity in deduped.items():
                batch.put_item(Item={
                    "user_id": user_id,
                    "date": date,
                    "equity": Decimal(str(round(equity, 2))),
                    "created_at": datetime.utcnow().isoformat()
                })
        logger.info(f"Saved dashboard equity curve for user_id={user_id}")
    except Exception as e:
        logger.exception(f"Failed saving dashboard equity curve: {str(e)}")
        raise