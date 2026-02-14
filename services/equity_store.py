from decimal import Decimal
from db.dynamodb import get_equity_curve_table


def save_equity_curve(user_id: str, equity_curve: list):

    table = get_equity_curve_table()

    with table.batch_writer() as batch:

        for point in equity_curve:

            batch.put_item(

                Item={

                    "user_id": user_id,

                    "timestamp": int(
                        point["timestamp"]
                    ),

                    "equity": Decimal(
                        str(point["equity"])
                    )
                }
            )
