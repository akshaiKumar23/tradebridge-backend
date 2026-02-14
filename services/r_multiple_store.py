from decimal import Decimal
from db.dynamodb import get_r_multiple_table


def save_r_multiples(user_id: str, trades: list):

    table = get_r_multiple_table()

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