from decimal import Decimal
from datetime import datetime
import logging

from db.dynamodb import get_report_stats_table

logger = logging.getLogger(__name__)


def save_user_report_stats(
    user_id: str,
    snapshot_date: str,
    analytics: dict
):

    table = get_report_stats_table()

    try:

        item = {

            "user_id": user_id,

            "snapshot_date": snapshot_date,

            "total_pnl":
                Decimal(str(analytics.get("total_pnl", 0))),

            "total_trades":
                int(analytics.get("total_trades", 0)),

            "win_rate":
                Decimal(str(analytics.get("win_rate", 0))),

            "profit_factor":
                Decimal(str(analytics.get("profit_factor", 0))),

            "expectancy":
                Decimal(str(analytics.get("expectancy", 0))),

            "created_at":
                datetime.utcnow().isoformat()
        }

        table.put_item(Item=item)

        logger.info(
            f"Report stats saved for user_id={user_id}, snapshot={snapshot_date}"
        )

    except Exception as e:

        logger.exception(
            f"Failed to save report stats for user_id={user_id}: {str(e)}"
        )

        raise
