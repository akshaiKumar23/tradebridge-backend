from fastapi import APIRouter, Depends
from boto3.dynamodb.conditions import Key
from decimal import Decimal

from auth_dependency import get_current_user
from db.dynamodb import get_report_stats_table
from db.dynamodb import get_report_symbol_summary_table
from db.dynamodb import get_report_win_rate_table
from db.dynamodb import get_report_overview_table


router = APIRouter(
    prefix="/reports",
    tags=["Reports"]
)


def decimal_to_float(v):

    if isinstance(v, Decimal):
        return float(v)

    return v


@router.get("/page")
async def get_report_stats(
    current_user: dict = Depends(get_current_user)
):

    user_id = current_user["user_id"]

    table = get_report_stats_table()

    res = table.query(

        KeyConditionExpression=
            Key("user_id").eq(user_id),

        ScanIndexForward=False,

        Limit=1
    )

    if not res.get("Items"):

        return {
            "status": "success",
            "data": []
        }

    item = res["Items"][0]

    total_pnl = decimal_to_float(item.get("total_pnl", 0))

    total_trades =decimal_to_float(item.get("total_trades", 0))

    win_rate = decimal_to_float(item.get("win_rate", 0))

    profit_factor = decimal_to_float(item.get("profit_factor", 0))

    expectancy = decimal_to_float(item.get("expectancy", 0))

    data = [

        {
            "label": "Net PnL",

            "value":
                f"+${total_pnl:.2f}"
                if total_pnl >= 0
                else f"-${abs(total_pnl):.2f}",

            "subLabel":
                "Profitable"
                if total_pnl >= 0
                else "Loss",

            "accent":
                "green"
                if total_pnl >= 0
                else "red"
        },

        {
            "label": "Win Rate",

            "value":
                f"{win_rate:.2f}%",

            "subLabel":
                f"{int(total_trades)} trades",

            "accent":
                "blue"
        },

        {
            "label": "Profit Factor",

            "value":
                f"{profit_factor:.2f}",

            "subLabel":
                "Strong"
                if profit_factor > 1
                else "Weak",

            "accent":
                "green"
                if profit_factor > 1
                else "red"
        },

        {
            "label": "Expectancy",

            "value":
                f"${expectancy:.2f}",

            "subLabel":
                "Per trade avg",

            "accent":
                "blue"
        }
    ]

    summary_table = get_report_symbol_summary_table()

    summary_res = summary_table.query(

        KeyConditionExpression=
            Key("user_id").eq(user_id),

        ScanIndexForward=True
    )

    summary = [

        {

            "symbol": item["symbol"],

            "avg_volume":
                decimal_to_float(item.get("avg_volume", 0)),

            "avg_loss":
                decimal_to_float(item.get("avg_loss", 0)),

            "avg_win":
                decimal_to_float(item.get("avg_win", 0)),

            "net_pnl":
                decimal_to_float(item.get("net_pnl", 0)),

            "trades":
                int(item.get("trades", 0)),

            "win_rate":
                decimal_to_float(item.get("win_rate", 0))
        }

        for item in summary_res.get("Items", [])
    ]

    winrate_table = get_report_win_rate_table()

    winrate_res = winrate_table.query(

        KeyConditionExpression=
            Key("user_id").eq(user_id),

        ScanIndexForward=True
    )

    win_rate_chart = [

        {

            "symbol": item["symbol"],

            "period_start": item["period_start"],

            "win_rate":
                decimal_to_float(item["win_rate"]),

            "trades":
                item["trades"]
        }

        for item in winrate_res.get("Items", [])
    ]
    
    overview_table = get_report_overview_table()

    overview_res = overview_table.query(

        KeyConditionExpression=
            Key("user_id").eq(user_id),

        ScanIndexForward=True
    )

    overview_chart = [

        {

            "week": item["week_start"],

            "pnl":
                decimal_to_float(item["net_pnl"]),

            "trades":
                item["trade_count"]
        }

        for item in overview_res.get("Items", [])
    ]



    return {

        "status": "success",

        "data": {
            "stats":data,
            "summary":summary,
            "win_rate_chart":win_rate_chart,
            "overview_chart": overview_chart
        }
    }
