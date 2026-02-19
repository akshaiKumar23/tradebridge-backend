from decimal import Decimal
from datetime import datetime, timedelta
from collections import defaultdict
import logging

from db.dynamodb import get_report_overview_table


logger = logging.getLogger(__name__)


def save_user_report_overview(
    user_id: str,
    trades: list
):

    table = get_report_overview_table()

    weekly = defaultdict(lambda: {
        "net_pnl": 0,
        "trade_count": 0
    })

    for trade in trades:

        pnl = float(trade["pnl"])

        close_time = trade["close_time"]

        dt = datetime.fromtimestamp(close_time)

        week_start = (
            dt - timedelta(days=dt.weekday())
        ).strftime("%Y-%m-%d")

        weekly[week_start]["net_pnl"] += pnl
        weekly[week_start]["trade_count"] += 1

    try:

        with table.batch_writer(
            overwrite_by_pkeys=["user_id", "week_start"]
        ) as batch:

            for week_start, data in weekly.items():

                item = {

                    "user_id": user_id,

                    "week_start": week_start,

                    "net_pnl":
                        Decimal(str(round(data["net_pnl"], 2))),

                    "trade_count":
                        int(data["trade_count"]),

                    "created_at":
                        datetime.utcnow().isoformat()
                }

                batch.put_item(Item=item)

        logger.info(
            f"Saved report overview for user_id={user_id}"
        )

    except Exception as e:

        logger.exception(
            f"Failed saving report overview: {str(e)}"
        )

        raise
