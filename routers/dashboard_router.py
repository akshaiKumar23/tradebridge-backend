from fastapi import APIRouter, Depends
from boto3.dynamodb.conditions import Key
from decimal import Decimal

from auth_dependency import get_current_user
from db.dynamodb import (
    get_daily_pnl_table,
    get_dashboard_stats_table,
    get_dashboard_session_performance_table,
    get_dashboard_symbol_performance_table,
    get_dashboard_daily_pnl_table,
    get_dashboard_equity_curve_table
)


router = APIRouter(
    prefix="/dashboard",
    tags=["Dashboard"]
)


def decimal_to_float(v):
    if isinstance(v, Decimal):
        return float(v)
    return v


@router.get("/page")
async def get_dashboard_page(
    current_user: dict = Depends(get_current_user)
):

    user_id = current_user["user_id"]


    pnl_table = get_daily_pnl_table()

    pnl_res = pnl_table.query(

        KeyConditionExpression=
            Key("user_id").eq(user_id),

        ScanIndexForward=False,

        Limit=30
    )

    daily_pnl = []

    for item in pnl_res.get("Items", []):

        pnl = decimal_to_float(item["pnl"])

        if pnl > 0:
            type_ = "profit"
            value = f"+${abs(pnl):.2f}"

        elif pnl < 0:
            type_ = "loss"
            value = f"-${abs(pnl):.2f}"

        else:
            type_ = "neutral"
            value = "$0"

        daily_pnl.append({

            "date": item["date"],

            "value": value,

            "type": type_
        })


    stats_table = get_dashboard_stats_table()

    stats_res = stats_table.query(

        KeyConditionExpression=
            Key("user_id").eq(user_id),

        ScanIndexForward=False,

        Limit=1
    )

    stats_overview = None

    if stats_res.get("Items"):

        item = stats_res["Items"][0]

        stats_overview = {

            "net_pnl":
                decimal_to_float(item["total_pnl"]),

            "avg_rr":
                decimal_to_float(item["avg_rr"]),

            "profit_factor":
                decimal_to_float(item["profit_factor"]),

            "avg_win":
                decimal_to_float(item["avg_win"]),

            "avg_loss":
                decimal_to_float(item["avg_loss"]),

            "win_rate":
                decimal_to_float(item["win_rate"]),
        }

    session_table = get_dashboard_session_performance_table()

    session_res = session_table.query(

        KeyConditionExpression=
            Key("user_id").eq(user_id),

        ScanIndexForward=True
    )

    session_map = {}

    for item in session_res.get("Items", []):

        session = item["session"]

        period_index = item["period_index"]

        pnl = decimal_to_float(item["pnl"])

        if session not in session_map:

            session_map[session] = {

                "session": session,

                "v1": 0,
                "v2": 0,
                "v3": 0
            }

        if period_index == 0:
            session_map[session]["v1"] = pnl

        elif period_index == 1:
            session_map[session]["v2"] = pnl

        elif period_index == 2:
            session_map[session]["v3"] = pnl


    session_performance = list(session_map.values())

    symbol_table = get_dashboard_symbol_performance_table()

    symbol_res = symbol_table.query(

        KeyConditionExpression=
            Key("user_id").eq(user_id),

        ScanIndexForward=True
    )

    symbol_performance = [

    {

        "symbol": item["symbol"],

        "value":
            decimal_to_float(item["net_pnl"]),

        "percent":
            decimal_to_float(
                item["performance_percent"]
            )
    }

    for item in symbol_res.get("Items", [])
    ]


    dashboard_pnl_table = get_dashboard_daily_pnl_table()

    dashboard_pnl_res = dashboard_pnl_table.query(

        KeyConditionExpression=
            Key("user_id").eq(user_id),

        ScanIndexForward=True
    )

    dashboard_daily_pnl = [

        {

            "date": item["date"],

            "base":
                decimal_to_float(item["base"]),

            "profit":
                decimal_to_float(item["profit"]),

            "loss":
                decimal_to_float(item["loss"])
        }

        for item in dashboard_pnl_res.get("Items", [])
    ]
    
    equity_table = get_dashboard_equity_curve_table()

    equity_res = equity_table.query(

        KeyConditionExpression=
            Key("user_id").eq(user_id),

        ScanIndexForward=True
    )

    equity_curve = [

        {

            "date": item["date"],

            "equity":
                decimal_to_float(item["equity"])
        }

        for item in equity_res.get("Items", [])
    ]


    
    dashboard_equity = []

    pivot_index = len(equity_curve) - 1

    pivot_value = (
        equity_curve[pivot_index]["equity"]
        if equity_curve else 0
    )

    for i, item in enumerate(equity_curve):

        if i <= pivot_index:

            dashboard_equity.append({

                "date": item["date"],

                "history": item["equity"],

                "projection":
                    pivot_value if i >= pivot_index else None
            })

        else:

            dashboard_equity.append({

                "date": item["date"],

                "history": None,

                "projection": pivot_value
            })





    

    return {

        "status": "success",

        "data": {

            "daily_pnl": daily_pnl,

            "stats_overview": stats_overview,

            "session_performance": session_performance,
            "symbol_performance": symbol_performance,
            "dashboard_daily_pnl": dashboard_daily_pnl,
            "dashboard_equity_curve": dashboard_equity
        }
    }
