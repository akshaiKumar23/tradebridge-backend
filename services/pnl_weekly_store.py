from decimal import Decimal
from boto3.dynamodb.conditions import Key
from db.dynamodb import get_pnl_weekly_table


def save_weekly_pnl(user_id: str, weekly_pnl: dict):
    table = get_pnl_weekly_table()

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
            batch.delete_item(Key={"user_id": item["user_id"], "week_start": item["week_start"]})

    with table.batch_writer() as batch:
        for week_start, pnl in weekly_pnl.items():
            batch.put_item(Item={
                "user_id": user_id,
                "week_start": week_start,
                "pnl": Decimal(str(pnl))
            })