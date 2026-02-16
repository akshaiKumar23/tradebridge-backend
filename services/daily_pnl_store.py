from decimal import Decimal
from datetime import datetime
from db.dynamodb import get_daily_pnl_table


def save_daily_pnl(user_id: str, trades: list):

    table = get_daily_pnl_table()

    daily = {}

    for trade in trades:

        date = datetime.fromtimestamp(
            trade["timestamp"]
        ).strftime("%Y-%m-%d")

        if date not in daily:
            daily[date] = {
                "pnl": 0,
                "trades": 0,
                "wins": 0,
                "losses": 0
            }

        pnl = float(trade["pnl"])

        daily[date]["pnl"] += pnl
        daily[date]["trades"] += 1

        if pnl > 0:
            daily[date]["wins"] += 1
        else:
            daily[date]["losses"] += 1

    with table.batch_writer() as batch:

        for date, data in daily.items():

            batch.put_item(
                Item={
                    "user_id": user_id,
                    "date": date,
                    "pnl": Decimal(str(round(data["pnl"], 2))),
                    "trades": data["trades"],
                    "wins": data["wins"],
                    "losses": data["losses"],
                    "created_at":
                        datetime.utcnow().isoformat()
                }
            )
