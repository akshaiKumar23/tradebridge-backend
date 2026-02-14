import MetaTrader5 as mt5
from datetime import datetime, timedelta
from collections import defaultdict


def fetch_mt5_analytics(server, login, password):

    try:

        if not mt5.initialize():
            return {
                "status": "error",
                "message": f"MT5 initialize failed: {mt5.last_error()}"
            }

        if not mt5.login(login=login, password=password, server=server):
            mt5.shutdown()
            return {
                "status": "error",
                "message": f"MT5 login failed: {mt5.last_error()}"
            }

        account = mt5.account_info()

        if account is None:
            mt5.shutdown()
            return {
                "status": "error",
                "message": "Unable to fetch account info"
            }

        account_data = {

            "login": account.login,

            "balance": str(account.balance),

            "equity": str(account.equity),

            "profit": str(account.profit),

            "margin_level":
                str((account.equity / account.margin) * 100)
                if account.margin > 0 else "0",

            "company":
                getattr(account, "company", "N/A"),
        }

        positions_data = []

        positions = mt5.positions_get()

        if positions:

            for pos in positions:

                positions_data.append({

                    "ticket": int(pos.ticket),

                    "symbol": pos.symbol,

                    "profit": str(pos.profit),

                    "opened_at":
                        datetime.fromtimestamp(pos.time).isoformat(),
                })

        date_from = datetime.now() - timedelta(days=30)

        deals = mt5.history_deals_get(date_from, datetime.now())

        trades_list = []

        wins = []
        losses = []

        daily_pnl_map = defaultdict(float)

        equity_curve = []

        entry_deals = {}

        current_equity = account.balance - account.profit

        net_pnl = 0.0

        if deals:

            for deal in sorted(deals, key=lambda x: x.time):

                if deal.entry == mt5.DEAL_ENTRY_IN:

                    entry_deals[deal.position_id] = deal

               
                elif deal.entry in (
                    mt5.DEAL_ENTRY_OUT,
                    mt5.DEAL_ENTRY_INOUT
                ):

                    entry = entry_deals.get(deal.position_id)

                    if not entry:
                        continue

                    trade_net = (
                        (deal.profit or 0)
                        + (deal.swap or 0)
                        + (deal.commission or 0)
                    )

                    net_pnl += trade_net

                    current_equity += trade_net

                    open_time = entry.time

                    close_time = deal.time

                    hold_time_minutes = (
                        close_time - open_time
                    ) / 60

                    iso_time = datetime.fromtimestamp(
                        close_time
                    ).isoformat()

                    equity_curve.append({

                        "timestamp": close_time,

                        "equity": round(current_equity, 2)
                    })


                    daily_pnl_map[
                        datetime.fromtimestamp(close_time)
                        .strftime('%Y-%m-%d')
                    ] += trade_net

                    if trade_net > 0:
                        wins.append(trade_net)
                    else:
                        losses.append(abs(trade_net))

                    trades_list.append({

                        "ticket": int(deal.ticket),

                        "position_id":
                            int(deal.position_id),

                        "symbol": str(deal.symbol),

                        "pnl": float(round(trade_net, 2)),

                        "open_time": open_time,

                        "close_time": close_time,

                        "hold_time_minutes":
                            hold_time_minutes,

                        "volume": float(deal.volume),
                    })

        total_t = len(wins) + len(losses)

        avg_win = sum(wins) / len(wins) if wins else 0

        avg_loss = sum(losses) / len(losses) if losses else 0

        expectancy = (
            net_pnl / total_t if total_t > 0 else 0
        )

        metrics = {

            "net_pnl_30d":
                str(round(net_pnl, 2)),

            "total_trades": total_t,

            "wins": len(wins),

            "losses": len(losses),

            "win_percentage":
                round((len(wins) / total_t * 100), 2)
                if total_t > 0 else 0,

            "profit_factor":
                round(sum(wins) / sum(losses), 2)
                if losses else 0,

            "expectancy":
                round(expectancy, 2),

            "avg_win":
                round(avg_win, 2),

            "avg_loss":
                round(avg_loss, 2),
        }

        return {

            "status": "success",

            "data": {

                "account": account_data,

                "open_positions": positions_data,

                "performance_metrics": metrics,

                "equity_vs_time": equity_curve,

                "daily_pnl": [

                    {
                        "date": d,
                        "pnl": str(round(p, 2))
                    }

                    for d, p in
                    sorted(daily_pnl_map.items())
                ],

                "trades": trades_list
            }
        }

    finally:

        mt5.shutdown()
