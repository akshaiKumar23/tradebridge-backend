from fastapi import APIRouter, Depends
from boto3.dynamodb.conditions import Key
from decimal import Decimal

from auth_dependency import get_current_user
from db.dynamodb import (
    get_daily_pnl_table,
    get_dashboard_stats_table
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

 

    return {

        "status": "success",

        "data": {

            "daily_pnl": daily_pnl,

            "stats_overview": stats_overview
        }
    }
