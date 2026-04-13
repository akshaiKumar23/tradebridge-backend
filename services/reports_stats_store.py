from decimal import Decimal
from datetime import datetime
import logging
from boto3.dynamodb.conditions import Key
from db.dynamodb import get_report_stats_table

logger = logging.getLogger(__name__)


def save_user_report_stats(user_id: str, snapshot_date: str, analytics: dict):
    table = get_report_stats_table()

    # Delete all existing rows for this user
    items_to_delete = []
    last_key = None
    while True:
        kwargs = {
            "KeyConditionExpression": Key("user_id").eq(user_id),
            "ProjectionExpression": "user_id, snapshot_date",
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
                Key={"user_id": item["user_id"], "snapshot_date": item["snapshot_date"]})

    try:
        table.put_item(Item={
            "user_id": user_id,
            "snapshot_date": snapshot_date,
            "total_pnl": Decimal(str(analytics.get("total_pnl", 0))),
            "total_trades": int(analytics.get("total_trades", 0)),
            "win_rate": Decimal(str(analytics.get("win_rate", 0))),
            "profit_factor": Decimal(str(analytics.get("profit_factor", 0))),
            "expectancy": Decimal(str(analytics.get("expectancy", 0))),
            "created_at": datetime.utcnow().isoformat()
        })

        logger.info(
            f"Report stats saved for user_id={user_id}, snapshot={snapshot_date}")
    except Exception as e:
        logger.exception(
            f"Failed to save report stats for user_id={user_id}: {str(e)}")
        raise
