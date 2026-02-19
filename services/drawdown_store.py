from decimal import Decimal
from datetime import datetime
import logging

from db.dynamodb import get_drawdown_curve_table


logger = logging.getLogger(__name__)


def save_drawdown_curve(
    user_id: str,
    equity_curve: list
):

    table = get_drawdown_curve_table()

    if not equity_curve:
        logger.warning(f"No equity curve for user_id={user_id}")
        return

    peak = 0

    try:

        with table.batch_writer(
            overwrite_by_pkeys=["user_id", "timestamp"]
        ) as batch:

            for point in equity_curve:

                timestamp = int(point["timestamp"])
                equity = float(point["equity"])

                if equity > peak:
                    peak = equity

                drawdown = peak - equity

                item = {

                    "user_id": user_id,

                    "timestamp": timestamp,

                    "equity":
                        Decimal(str(equity)),

                    "peak_equity":
                        Decimal(str(peak)),

                    "drawdown":
                        Decimal(str(round(drawdown, 2))),

                    "created_at":
                        datetime.utcnow().isoformat()
                }

                batch.put_item(Item=item)

        logger.info(
            f"Saved drawdown curve for user_id={user_id}"
        )

    except Exception as e:

        logger.exception(
            f"Failed saving drawdown curve: {str(e)}"
        )

        raise
