from decimal import Decimal
from datetime import datetime
import logging

from db.dynamodb import get_report_symbol_summary_table


logger = logging.getLogger(__name__)


def save_user_report_symbol_summary(
    user_id: str,
    snapshot_date: str,
    analytics: dict
):

    table = get_report_symbol_summary_table()

    symbols = analytics.get("symbols", {})

    if not symbols:
        logger.warning(f"No symbol summary for user_id={user_id}")
        return

    try:

        with table.batch_writer(
            overwrite_by_pkeys=["user_id", "symbol"]
        ) as batch:

            for symbol, data in symbols.items():

                trades = data.get("trades", 0)
                wins = data.get("wins", 0)
                losses = data.get("losses", 0)
                pnl = data.get("pnl", 0)

                avg_win = pnl / wins if wins > 0 else 0
                avg_loss = pnl / losses if losses > 0 else 0

                win_rate = (
                    (wins / trades) * 100
                    if trades > 0 else 0
                )

                item = {

                    "user_id": user_id,

                    "symbol": symbol,

                    "avg_volume":
                        Decimal(str(analytics.get("avg_volume", 0))),

                    "avg_loss":
                        Decimal(str(avg_loss)),

                    "avg_win":
                        Decimal(str(avg_win)),

                    "net_pnl":
                        Decimal(str(pnl)),

                    "trades":
                        int(trades),

                    "win_rate":
                        Decimal(str(round(win_rate, 2))),

                    "snapshot_date":
                        snapshot_date,

                    "created_at":
                        datetime.utcnow().isoformat()
                }

                batch.put_item(Item=item)

        logger.info(
            f"Saved report symbol summary for user_id={user_id}"
        )

    except Exception as e:

        logger.exception(
            f"Failed saving symbol summary for user_id={user_id}: {str(e)}"
        )

        raise
