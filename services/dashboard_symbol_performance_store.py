from decimal import Decimal
from datetime import datetime
from collections import defaultdict
from boto3.dynamodb.conditions import Key
import logging
from db.dynamodb import get_dashboard_symbol_performance_table

logger = logging.getLogger(__name__)


def save_dashboard_symbol_performance(user_id: str, trades: list):
    table = get_dashboard_symbol_performance_table()

    # Delete all existing rows for this user
    items_to_delete = []
    last_key = None
    while True:
        kwargs = {
            "KeyConditionExpression": Key("user_id").eq(user_id),
            "ProjectionExpression": "user_id, symbol",
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
                "symbol": item["symbol"]
            })

    if not trades:
        logger.warning(f"No trades for dashboard symbol performance user_id={user_id}")
        return

    symbols = defaultdict(lambda: {"net_pnl": 0, "trade_count": 0})
    for trade in trades:
        symbols[trade["symbol"]]["net_pnl"] += float(trade["pnl"])
        symbols[trade["symbol"]]["trade_count"] += 1

    max_pnl = max(abs(d["net_pnl"]) for d in symbols.values()) if symbols else 1

    try:
        with table.batch_writer() as batch:
            for symbol, data in symbols.items():
                net_pnl = data["net_pnl"]
                percent = max(min((net_pnl / max_pnl) * 100 if max_pnl > 0 else 0, 100), -100)
                batch.put_item(Item={
                    "user_id": user_id,
                    "symbol": symbol,
                    "net_pnl": Decimal(str(round(net_pnl, 2))),
                    "performance_percent": Decimal(str(round(abs(percent), 2))),
                    "trade_count": data["trade_count"],
                    "created_at": datetime.utcnow().isoformat()
                })
        logger.info(f"Saved dashboard symbol performance for user_id={user_id}")
    except Exception as e:
        logger.exception(f"Failed saving dashboard symbol performance: {str(e)}")
        raise