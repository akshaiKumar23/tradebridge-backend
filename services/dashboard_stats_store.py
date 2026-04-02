from decimal import Decimal
from datetime import datetime
from boto3.dynamodb.conditions import Key
from db.dynamodb import get_dashboard_stats_table
import logging

logger = logging.getLogger(__name__)


def save_dashboard_stats(user_id: str, snapshot_date: str, analytics: dict):
    table = get_dashboard_stats_table()

    # Delete all existing snapshots for this user
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
            batch.delete_item(Key={
                "user_id": item["user_id"],
                "snapshot_date": item["snapshot_date"]
            })

    wins = analytics.get("wins", 0)
    losses = analytics.get("losses", 0)
    avg_rr = round(analytics["avg_win"] / analytics["avg_loss"], 2) if losses > 0 else 0

    current_time = datetime.utcnow().isoformat()
    item = {
        "user_id": user_id,
        "snapshot_date": snapshot_date,
        "total_pnl": Decimal(str(analytics["total_pnl"])),
        "avg_rr": Decimal(str(avg_rr)),
        "profit_factor": Decimal(str(analytics["profit_factor"])),
        "avg_win": Decimal(str(analytics["avg_win"])),
        "avg_loss": Decimal(str(analytics["avg_loss"])),
        "win_rate": Decimal(str(analytics["win_rate"])),
        "created_at": current_time,
        "updated_at": current_time,
    }

    table.put_item(Item=item)
    logger.info(f"Saved dashboard stats for user_id={user_id}, snapshot={snapshot_date}")