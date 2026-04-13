from decimal import Decimal
import logging
from boto3.dynamodb.conditions import Key
from db.dynamodb import get_equity_curve_table

logger = logging.getLogger(__name__)


def save_equity_curve(user_id: str, equity_curve: list):
    if not equity_curve:
        logger.warning(f"No equity curve data to save for user_id={user_id}")
        return

    table = get_equity_curve_table()

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

    seen_timestamps = set()
    duplicate_count = 0

    try:
        with table.batch_writer() as batch:
            for point in equity_curve:
                try:
                    timestamp = int(point["timestamp"])
                    equity = Decimal(str(point["equity"]))
                except (KeyError, ValueError, TypeError) as e:
                    logger.error(f"Invalid equity point for user_id={user_id}: {point}, error={e}")
                    continue

                if timestamp in seen_timestamps:
                    duplicate_count += 1
                    continue

                seen_timestamps.add(timestamp)
                batch.put_item(Item={
                    "user_id": user_id,
                    "timestamp": timestamp,
                    "equity": equity,
                })

        if duplicate_count > 0:
            logger.warning(f"Removed {duplicate_count} duplicate equity points for user_id={user_id}")

        logger.info(f"Successfully saved {len(seen_timestamps)} equity points for user_id={user_id}")
    except Exception as e:
        logger.exception(f"Failed to save equity curve for user_id={user_id}: {str(e)}")
        raise