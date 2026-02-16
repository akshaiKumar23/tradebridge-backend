from decimal import Decimal
from db.dynamodb import get_pnl_weekly_table


def save_weekly_pnl(user_id: str, weekly_pnl: dict):

    table = get_pnl_weekly_table()

    with table.batch_writer(
        overwrite_by_pkeys=["user_id", "week_start"]
    ) as batch:

        for week_start, pnl in weekly_pnl.items():

            batch.put_item(

                Item={

                    "user_id": user_id,

                    "week_start": week_start,

                    "pnl": Decimal(str(pnl))
                }
            )
