from decimal import Decimal
from datetime import datetime
from collections import defaultdict
import logging
from boto3.dynamodb.conditions import Key
from db.dynamodb import get_session_performance_table

logger = logging.getLogger(__name__)


def get_session_from_timestamp(timestamp):
    hour = datetime.fromtimestamp(timestamp).hour
    if 0 <= hour < 8:
        return "Asia"
    elif 8 <= hour < 16:
        return "London"
    else:
        return "New York"


def save_session_performance(user_id: str, trades: list):
    table = get_session_performance_table()

    # Delete all existing rows for this user
    items_to_delete = []
    last_key = None
    while True:
        kwargs = {
            "KeyConditionExpression": Key("user_id").eq(user_id),
            "ProjectionExpression": "user_id, #s",
            "ExpressionAttributeNames": {"#s": "session"},
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
                Key={"user_id": item["user_id"], "session": item["session"]})

    sessions = defaultdict(lambda: {
        "total_pnl": 0, "trade_count": 0, "wins": 0,
        "losses": 0, "peak": 0, "equity": 0, "drawdown": 0
    })

    for trade in trades:
        session = get_session_from_timestamp(trade["close_time"])
        pnl = float(trade["pnl"])

        sessions[session]["total_pnl"] += pnl
        sessions[session]["trade_count"] += 1

        if pnl > 0:
            sessions[session]["wins"] += 1
        else:
            sessions[session]["losses"] += 1

        equity = sessions[session]["equity"] + pnl
        sessions[session]["equity"] = equity

        if equity > sessions[session]["peak"]:
            sessions[session]["peak"] = equity

        drawdown = sessions[session]["peak"] - equity
        if drawdown > sessions[session]["drawdown"]:
            sessions[session]["drawdown"] = drawdown

    try:
        with table.batch_writer() as batch:
            for session, data in sessions.items():
                batch.put_item(Item={
                    "user_id": user_id,
                    "session": session,
                    "total_pnl": Decimal(str(round(data["total_pnl"], 2))),
                    "total_drawdown": Decimal(str(round(data["drawdown"], 2))),
                    "trade_count": data["trade_count"],
                    "win_count": data["wins"],
                    "loss_count": data["losses"],
                    "created_at": datetime.utcnow().isoformat()
                })

        logger.info(f"Saved session performance for user_id={user_id}")
    except Exception as e:
        logger.exception(f"Failed saving session performance: {str(e)}")
        raise
