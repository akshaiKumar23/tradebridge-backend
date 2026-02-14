from fastapi import APIRouter, Depends
from boto3.dynamodb.conditions import Key
from decimal import Decimal

from auth_dependency import get_current_user
from db.dynamodb import get_analytics_stats_table


router = APIRouter(
    prefix="/analytics",
    tags=["Analytics"]
)


def decimal_to_float(value):
    if isinstance(value, Decimal):
        return float(value)
    return value


@router.get("/page")
async def get_analytics_page(
    current_user: dict = Depends(get_current_user)
):
  

    user_id = current_user["user_id"]

    table = get_analytics_stats_table()

    response = table.query(
        KeyConditionExpression=Key("user_id").eq(user_id),
        ScanIndexForward=False,
        Limit=1
    )

    items = response.get("Items", [])

    if not items:

        return {
            "status": "success",
            "data": {
                "stats": None,
                "behavior": None
            }
        }

    item = items[0]

  
    stats = {

        "total_pnl":
            decimal_to_float(item.get("total_pnl", 0)),

        "total_trades":
            decimal_to_float(item.get("total_trades", 0)),

        "wins":
            decimal_to_float(item.get("wins", 0)),

        "losses":
            decimal_to_float(item.get("losses", 0)),

        "win_rate":
            decimal_to_float(item.get("win_rate", 0)),

        "profit_factor":
            decimal_to_float(item.get("profit_factor", 0)),

        "expectancy":
            decimal_to_float(item.get("expectancy", 0)),

        "avg_win":
            decimal_to_float(item.get("avg_win", 0)),

        "avg_loss":
            decimal_to_float(item.get("avg_loss", 0)),

        "snapshot_date":
            item.get("snapshot_date"),
    }

  
    behavior = {

        "avg_hold_time_minutes":
            decimal_to_float(
                item.get("avg_hold_time_minutes", 0)
            ),

        "max_consecutive_losses":
            decimal_to_float(
                item.get("max_consecutive_losses", 0)
            ),

        "best_trading_hour":
            item.get("best_trading_hour"),

        "overtrading_signals":
            item.get("overtrading_signals"),

        "revenge_trading_count":
            decimal_to_float(
                item.get("revenge_trading_count", 0)
            ),

        "avg_volume":
            decimal_to_float(
                item.get("avg_volume", 0)
            ),
    }

    return {

        "status": "success",

        "data": {

            "stats": stats,

            "behavior": behavior

        }
    }
