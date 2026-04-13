from datetime import datetime
from decimal import Decimal
from boto3.dynamodb.conditions import Key
from db.dynamodb import get_performance_snapshots_table


def save_user_performance_snapshot(user_id: str, snapshot_date: str, data: dict):
    table = get_performance_snapshots_table()

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
            batch.delete_item(Key={"user_id": item["user_id"], "snapshot_date": item["snapshot_date"]})

    table.put_item(Item={
        "user_id": user_id,
        "snapshot_date": snapshot_date,
        "symbols": {
            k: {
                "pnl": Decimal(str(v["pnl"])),
                "trades": Decimal(v["trades"]),
                "wins": Decimal(v["wins"]),
                "losses": Decimal(v["losses"]),
            }
            for k, v in data["symbols"].items()
        },
        "total_trades": Decimal(data["total_trades"]),
        "total_pnl": Decimal(str(data["total_pnl"])),
        "created_at": datetime.utcnow().isoformat()
    })