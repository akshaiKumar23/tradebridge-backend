from fastapi import APIRouter, Depends
from boto3.dynamodb.conditions import Key
from decimal import Decimal
from datetime import datetime

from auth_dependency import get_current_user
from db.dynamodb import get_trades_table


router = APIRouter(
    prefix="/trades",
    tags=["Trades"]
)


def decimal_to_native(v):
    if isinstance(v, Decimal):
        if v % 1 == 0:
            return int(v)
        return float(v)
    return v



@router.get("/")
async def get_trades(
    current_user: dict = Depends(get_current_user)
):

    user_id = current_user["user_id"]

    table = get_trades_table()

    response = table.query(

        KeyConditionExpression=
            Key("user_id").eq(user_id),

        ScanIndexForward=False
    )

    items = response.get("Items", [])

    trades = []

    for item in items:

        trade = {

            "trade_id":
                item.get("position_id"),

            "date": datetime.fromtimestamp(
                decimal_to_native(item["timestamp"])
            ).strftime("%Y-%m-%d"),


            "symbol":
                item.get("symbol"),

            "direction":
                item.get("direction"),

            "entry":
                decimal_to_native(
                    item.get("entry_price", 0)
                ),

            "exit":
                decimal_to_native(
                    item.get("exit_price", 0)
                ),

            "size":
                decimal_to_native(
                    item.get("volume", 0)
                ),

            "pnl":
                decimal_to_native(
                    item.get("pnl", 0)
                ),

            "r":
                decimal_to_native(
                    item.get("r_multiple", 0)
                ),

            "tags":
                item.get("tags", [])

        }

        trades.append(trade)

    return {

        "status": "success",

        "data": trades

    }
