from decimal import Decimal
from datetime import datetime, timedelta
from collections import defaultdict
import logging

from db.dynamodb import get_dashboard_session_performance_table


logger = logging.getLogger(__name__)


def get_session_from_timestamp(timestamp):

    dt = datetime.fromtimestamp(timestamp)

    hour = dt.hour

    if 0 <= hour < 8:
        return "Asia"

    elif 8 <= hour < 16:
        return "London"

    else:
        return "New York"


def save_dashboard_session_performance(
    user_id: str,
    trades: list
):

    table = get_dashboard_session_performance_table()

    sessions = defaultdict(lambda: defaultdict(float))

    now = datetime.utcnow()

    for trade in trades:

        pnl = float(trade["pnl"])

        close_time = datetime.fromtimestamp(
            trade["close_time"]
        )

        session = get_session_from_timestamp(
            trade["close_time"]
        )

        delta_days = (now - close_time).days

        period_index = delta_days // 7

        if period_index > 2:
            continue

        sessions[session][period_index] += pnl

    try:

        with table.batch_writer(
            overwrite_by_pkeys=["user_id", "session_period"]
        ) as batch:

            for session, periods in sessions.items():

                for period_index, pnl in periods.items():

                    session_period = f"{session}#{period_index}"

                    item = {

                        "user_id": user_id,

                        "session_period": session_period,

                        "session": session,

                        "period_index": period_index,

                        "pnl":
                            Decimal(str(round(pnl, 2))),

                        "created_at":
                            datetime.utcnow().isoformat()
                    }

                    batch.put_item(Item=item)

        logger.info(
            f"Saved dashboard session performance for user_id={user_id}"
        )

    except Exception as e:

        logger.exception(
            f"Failed saving dashboard session performance: {str(e)}"
        )

        raise
