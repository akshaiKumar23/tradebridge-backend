from decimal import Decimal
from datetime import datetime
from collections import defaultdict
import logging

from db.dynamodb import get_dashboard_symbol_performance_table


logger = logging.getLogger(__name__)


def save_dashboard_symbol_performance(
    user_id: str,
    trades: list
):

    table = get_dashboard_symbol_performance_table()

    symbols = defaultdict(lambda: {

        "net_pnl": 0,

        "trade_count": 0
    })

    for trade in trades:

        symbol = trade["symbol"]

        pnl = float(trade["pnl"])

        symbols[symbol]["net_pnl"] += pnl

        symbols[symbol]["trade_count"] += 1

    # find max pnl for normalization
    max_pnl = max(
        abs(data["net_pnl"])
        for data in symbols.values()
    ) if symbols else 1

    try:

        with table.batch_writer(
            overwrite_by_pkeys=["user_id", "symbol"]
        ) as batch:

            for symbol, data in symbols.items():

                net_pnl = data["net_pnl"]

                percent = (
                    (net_pnl / max_pnl) * 100
                ) if max_pnl > 0 else 0

                percent = max(
                    min(percent, 100),
                    -100
                )

                item = {

                    "user_id": user_id,

                    "symbol": symbol,

                    "net_pnl":
                        Decimal(str(round(net_pnl, 2))),

                    "performance_percent":
                        Decimal(str(round(abs(percent), 2))),

                    "trade_count":
                        data["trade_count"],

                    "created_at":
                        datetime.utcnow().isoformat()
                }

                batch.put_item(Item=item)

        logger.info(
            f"Saved dashboard symbol performance for user_id={user_id}"
        )

    except Exception as e:

        logger.exception(
            f"Failed saving dashboard symbol performance: {str(e)}"
        )

        raise
