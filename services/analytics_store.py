from datetime import datetime
from decimal import Decimal
from db.dynamodb import get_analytics_stats_table


def save_user_analytics_stats(
    user_id: str,
    snapshot_date: str,
    analytics: dict
):

    table = get_analytics_stats_table()

    symbols = analytics.get("symbols", {})

    symbols_decimal = {

        symbol: {

            "pnl":
                Decimal(str(data.get("pnl", 0))),

            "trades":
                Decimal(data.get("trades", 0)),

            "wins":
                Decimal(data.get("wins", 0)),

            "losses":
                Decimal(data.get("losses", 0))
        }

        for symbol, data in symbols.items()
    }

    item = {

        "user_id": user_id,

        "snapshot_date": snapshot_date,

        
        "total_pnl":
            Decimal(str(analytics.get("total_pnl", 0))),

        "total_trades":
            Decimal(analytics.get("total_trades", 0)),

        "wins":
            Decimal(analytics.get("wins", 0)),

        "losses":
            Decimal(analytics.get("losses", 0)),

        "win_rate":
            Decimal(str(analytics.get("win_rate", 0))),

        "profit_factor":
            Decimal(str(analytics.get("profit_factor", 0))),

        "expectancy":
            Decimal(str(analytics.get("expectancy", 0))),

        "avg_win":
            Decimal(str(analytics.get("avg_win", 0))),

        "avg_loss":
            Decimal(str(analytics.get("avg_loss", 0))),

      
        "avg_hold_time_minutes":
            Decimal(str(
                analytics.get(
                    "avg_hold_time_minutes", 0
                )
            )),

        "max_consecutive_losses":
            Decimal(
                analytics.get(
                    "max_consecutive_losses", 0
                )
            ),

        "best_trading_hour":
            analytics.get("best_trading_hour"),

        "overtrading_signals":
            analytics.get("overtrading_signals"),

        "revenge_trading_count":
            Decimal(
                analytics.get(
                    "revenge_trading_count", 0
                )
            ),

        "avg_volume":
            Decimal(str(
                analytics.get("avg_volume", 0)
            )),

        
        "symbols":
            symbols_decimal,

        "created_at":
            datetime.utcnow().isoformat()
    }

    table.put_item(Item=item)
