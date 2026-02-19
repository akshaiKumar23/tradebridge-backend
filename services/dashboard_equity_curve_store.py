from decimal import Decimal
from datetime import datetime
import logging

from db.dynamodb import get_dashboard_equity_curve_table


logger = logging.getLogger(__name__)


def save_dashboard_equity_curve(
    user_id: str,
    equity_curve: list
):

    table = get_dashboard_equity_curve_table()

    try:

        with table.batch_writer(
            overwrite_by_pkeys=["user_id", "date"]
        ) as batch:

            for point in equity_curve:

                timestamp = point["timestamp"]

                equity = point["equity"]

                date = datetime.fromtimestamp(
                    timestamp
                ).strftime("%Y-%m-%d")

                item = {

                    "user_id": user_id,

                    "date": date,

                    "equity":
                        Decimal(str(round(equity, 2))),

                    "created_at":
                        datetime.utcnow().isoformat()
                }

                batch.put_item(Item=item)

        logger.info(
            f"Saved dashboard equity curve for user_id={user_id}"
        )

    except Exception as e:

        logger.exception(
            f"Failed saving dashboard equity curve: {str(e)}"
        )

        raise
