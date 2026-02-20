from fastapi import APIRouter, Depends
from boto3.dynamodb.conditions import Key
from decimal import Decimal
from db.dynamodb import get_r_multiple_table
from db.dynamodb import get_drawdown_curve_table
from db.dynamodb import get_session_performance_table
from db.dynamodb import get_strategies_table
from db.dynamodb import get_trades_table


from auth_dependency import get_current_user

from db.dynamodb import (
    get_analytics_stats_table,
    get_equity_curve_table,
    get_pnl_weekly_table
)


router = APIRouter(
    prefix="/analytics",
    tags=["Analytics"]
)


def decimal_to_float(v):

    if isinstance(v, Decimal):
        return float(v)

    return v


@router.get("/page")
async def get_analytics_page(
    current_user: dict = Depends(get_current_user)
):

    user_id = current_user["user_id"]

  
    analytics_table = get_analytics_stats_table()

    analytics_res = analytics_table.query(

        KeyConditionExpression=
            Key("user_id").eq(user_id),

        ScanIndexForward=False,

        Limit=1
    )

    stats = None
    behavior = None

    if analytics_res.get("Items"):

        item = analytics_res["Items"][0]

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

    symbols_raw = item.get("symbols", {})

    performance_by_symbol = []

    for symbol, data in symbols_raw.items():

        pnl = decimal_to_float(data.get("pnl", 0))

        performance_by_symbol.append({

            "symbol": symbol,

            "profit": pnl if pnl > 0 else 0,

            "loss": pnl if pnl < 0 else 0,

            "trades":
                decimal_to_float(
                    data.get("trades", 0)
                ),

            "wins":
                decimal_to_float(
                    data.get("wins", 0)
                ),

            "losses":
                decimal_to_float(
                    data.get("losses", 0)
                )
        })

    equity_table = get_equity_curve_table()

    equity_res = equity_table.query(

        KeyConditionExpression=
            Key("user_id").eq(user_id),

        ScanIndexForward=True
    )

    equity_curve = [

        {

            "timestamp": item["timestamp"],

            "equity": decimal_to_float(item["equity"])
        }

        for item in equity_res.get("Items", [])
    ]

    pnl_table = get_pnl_weekly_table()

    pnl_res = pnl_table.query(

        KeyConditionExpression=
            Key("user_id").eq(user_id),

        ScanIndexForward=True
    )

    pnl_by_week = [

        {

            "week": item["week_start"],

            "pnl": decimal_to_float(item["pnl"])
        }

        for item in pnl_res.get("Items", [])
    ]

    r_table = get_r_multiple_table()

    r_res = r_table.query(

        KeyConditionExpression=
            Key("user_id").eq(user_id),

        ScanIndexForward=True
    )

    r_multiple_distribution = [

        {

            "timestamp": item["timestamp"],

            "value": float(item["r_multiple"]),

            "baseline": 0
        }

        for item in r_res.get("Items", [])
    ]

    drawdown_table = get_drawdown_curve_table()

    drawdown_res = drawdown_table.query(

        KeyConditionExpression=
            Key("user_id").eq(user_id),

        ScanIndexForward=True
    )

    drawdown_curve = [

        {

            "timestamp": item["timestamp"],

            "value":
                decimal_to_float(item["drawdown"])
        }

        for item in drawdown_res.get("Items", [])
    ]

    
    session_table = get_session_performance_table()

    session_res = session_table.query(

        KeyConditionExpression=
            Key("user_id").eq(user_id),

        ScanIndexForward=True
    )

    performance_by_session = [

        {

            "session": item["session"],

            "pnl":
                decimal_to_float(item["total_pnl"]),

            "dd":
                decimal_to_float(item["total_drawdown"]),

            "trades":
                item["trade_count"]
        }

        for item in session_res.get("Items", [])
    ]



    strategies_table = get_strategies_table()
    trades_table = get_trades_table()

    strategies_res = strategies_table.query(
        KeyConditionExpression=Key("user_id").eq(user_id)
    )

    strategies = strategies_res.get("Items", [])

    trades_res = trades_table.query(
        KeyConditionExpression=Key("user_id").eq(user_id)
    )

    all_trades = trades_res.get("Items", [])

    strategy_trades_map = {}

    for trade in all_trades:
        for tag in trade.get("tags", []):
            if tag.startswith("strategy#"):
                sid = tag.replace("strategy#", "")
                if sid not in strategy_trades_map:
                    strategy_trades_map[sid] = []
                strategy_trades_map[sid].append(trade)

    strategy_distribution = []
    total_trades_count = 0

    for strategy in strategies:

        sid = strategy["strategy_id"]

        trades = strategy_trades_map.get(sid, [])

        count = len(trades)

        total_trades_count += count

        strategy_distribution.append({
            "strategy_id": sid,
            "name": strategy.get("title") or "Unnamed",
            "trades": count
        })

   
    for s in strategy_distribution:

        s["percentage"] = (
            round((s["trades"] / total_trades_count) * 100, 2)
            if total_trades_count else 0
        )

    strategy_distribution.sort(
        key=lambda x: x["trades"],
        reverse=True
    )


    return {

        "status": "success",

        "data": {

            "stats": stats,

            "behavior": behavior,

            "equity_curve": equity_curve,

            "pnl_by_week": pnl_by_week,
            "performance_by_symbol": performance_by_symbol,
            "r_multiple_distribution": r_multiple_distribution,
             "drawdown_curve": drawdown_curve,
            "performance_by_session": performance_by_session,
            "strategy_distribution": strategy_distribution
        }
    }
