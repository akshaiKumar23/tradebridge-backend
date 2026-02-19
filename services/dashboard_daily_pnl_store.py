from decimal import Decimal
from datetime import datetime
from collections import defaultdict
import logging

from db.dynamodb import get_dashboard_daily_pnl_table


logger = logging.getLogger(__name__)


BASELINE = 30000


def save_dashboard_daily_pnl(
    user_id: str,
    trades: list
):

    table = get_dashboard_daily_pnl_table()

    daily = defaultdict(float)

    for trade in trades:

        close_time = datetime.fromtimestamp(
            trade["close_time"]
        )

        date = close_time.strftime("%Y-%m-%d")

        pnl = float(trade["pnl"])

        daily[date] += pnl

    try:

        with table.batch_writer(
            overwrite_by_pkeys=["user_id", "date"]
        ) as batch:

            for date, pnl in daily.items():

                item = {

                    "user_id": user_id,

                    "date": date,

                    "base": Decimal(str(BASELINE)),

                    "profit":
                        Decimal(str(pnl)) if pnl > 0 else Decimal("0"),

                    "loss":
                        Decimal(str(pnl)) if pnl < 0 else Decimal("0"),

                    "created_at":
                        datetime.utcnow().isoformat()
                }

                batch.put_item(Item=item)

        logger.info(
            f"Saved dashboard daily pnl for user_id={user_id}"
        )

    except Exception as e:

        logger.exception(
            f"Failed saving dashboard daily pnl: {str(e)}"
        )

        raise
