from decimal import Decimal
from datetime import datetime
from collections import defaultdict
import logging

from db.dynamodb import get_report_win_rate_table


logger = logging.getLogger(__name__)


def save_user_report_win_rate(
    user_id: str,
    trades: list
):

    table = get_report_win_rate_table()

    grouped = defaultdict(lambda: {
        "wins": 0,
        "losses": 0,
        "trades": 0
    })

    for trade in trades:

        symbol = trade["symbol"]

        close_time = trade["close_time"]

        dt = datetime.fromtimestamp(close_time)

        week_start = (
            dt.strftime("%Y-W%U")
        )

        key = f"{week_start}#{symbol}"

        grouped[key]["symbol"] = symbol
        grouped[key]["period_start"] = week_start

        grouped[key]["trades"] += 1

        if trade["pnl"] > 0:
            grouped[key]["wins"] += 1
        else:
            grouped[key]["losses"] += 1

    try:

        with table.batch_writer(
            overwrite_by_pkeys=["user_id", "period_key"]
        ) as batch:

            for period_key, data in grouped.items():

                trades = data["trades"]
                wins = data["wins"]

                win_rate = (
                    (wins / trades) * 100
                    if trades > 0 else 0
                )

                item = {

                    "user_id": user_id,

                    "period_key": period_key,

                    "symbol": data["symbol"],

                    "period_start": data["period_start"],

                    "period_type": "weekly",

                    "wins": trades,

                    "losses": data["losses"],

                    "trades": trades,

                    "win_rate":
                        Decimal(str(round(win_rate, 2))),

                    "created_at":
                        datetime.utcnow().isoformat()
                }

                batch.put_item(Item=item)

        logger.info(
            f"Saved win rate chart data for user_id={user_id}"
        )

    except Exception as e:

        logger.exception(
            f"Failed saving win rate chart: {str(e)}"
        )

        raise
