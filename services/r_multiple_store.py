from decimal import Decimal
import logging
from boto3.dynamodb.conditions import Key
from db.dynamodb import get_r_multiple_table

logger = logging.getLogger(__name__)


def save_r_multiples(user_id: str, trades: list):

    table = get_r_multiple_table()

    # Delete all existing rows for this user. Without this, narrowing the sync
    # window left the previous window's R-multiples behind -- overwrite_by_pkeys
    # only de-duplicates inside the batch, never against what is already stored.
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
            batch.delete_item(Key={
                "user_id": item["user_id"],
                "timestamp": item["timestamp"],
            })

    logger.info(
        f"Deleted {len(items_to_delete)} existing r-multiples for user_id={user_id}"
    )

    if not trades:
        logger.warning(f"No r-multiples to save for user_id={user_id}")
        return

    with table.batch_writer(
        overwrite_by_pkeys=["user_id", "timestamp"]
    ) as batch:

        for trade in trades:

            batch.put_item(

                Item={

                    "user_id": user_id,

                    "timestamp": trade["timestamp"],

                    "position_id": trade["position_id"],

                    "symbol": trade["symbol"],

                    "r_multiple":
                        Decimal(str(trade["r_multiple"])),

                    "pnl":
                        Decimal(str(trade["pnl"])),

                    "risk_amount":
                        Decimal(str(trade["risk_amount"])),
                }
            )

    logger.info(f"Successfully saved {len(trades)} r-multiples for user_id={user_id}")
