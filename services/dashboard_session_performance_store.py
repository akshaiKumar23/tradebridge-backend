from decimal import Decimal
from datetime import datetime
from collections import defaultdict
from boto3.dynamodb.conditions import Key
import logging
from db.dynamodb import get_dashboard_session_performance_table

logger = logging.getLogger(__name__)


def get_session_from_timestamp(timestamp):
    hour = datetime.fromtimestamp(timestamp).hour
    if 0 <= hour < 8:
        return "Asia"
    elif 8 <= hour < 16:
        return "London"
    else:
        return "New York"


def save_dashboard_session_performance(user_id: str, trades: list):
    table = get_dashboard_session_performance_table()

    # Delete all existing rows for this user
    items_to_delete = []
    last_key = None
    while True:
        kwargs = {
            "KeyConditionExpression": Key("user_id").eq(user_id),
            "ProjectionExpression": "user_id, session_period",
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
                "session_period": item["session_period"]
            })

    if not trades:
        logger.warning(
            f"No trades for dashboard session performance user_id={user_id}")
        return

    # Aggregate total pnl, trade count, wins, losses per session
    sessions = defaultdict(lambda: {
        "pnl": 0.0,
        "trades": 0,
        "wins": 0,
        "losses": 0
    })

    for trade in trades:
        session = get_session_from_timestamp(trade["close_time"])
        pnl = float(trade["pnl"])
        sessions[session]["pnl"] += pnl
        sessions[session]["trades"] += 1
        if pnl > 0:
            sessions[session]["wins"] += 1
        else:
            sessions[session]["losses"] += 1

    try:
        with table.batch_writer() as batch:
            for session, data in sessions.items():
                batch.put_item(Item={
                    "user_id": user_id,
                    "session_period": session,
                    "session": session,
                    "period_index": 0,
                    "pnl": Decimal(str(round(data["pnl"], 2))),
                    "trades": data["trades"],
                    "wins": data["wins"],
                    "losses": data["losses"],
                    "created_at": datetime.utcnow().isoformat()
                })
        logger.info(
            f"Saved dashboard session performance for user_id={user_id}")
    except Exception as e:
        logger.exception(
            f"Failed saving dashboard session performance: {str(e)}")
        raise
