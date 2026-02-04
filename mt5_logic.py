import MetaTrader5 as mt5
from datetime import datetime, timedelta
from collections import defaultdict

def fetch_mt5_analytics(server, login, password):
    """Connects to MT5 and calculates all required performance metrics."""
    try:
        if not mt5.initialize():
            return {"status": "error", "message": f"MT5 initialize failed: {mt5.last_error()}"}

        if not mt5.login(login=login, password=password, server=server):
            mt5.shutdown()
            return {"status": "error", "message": f"MT5 login failed: {mt5.last_error()}"}

        account = mt5.account_info()
        if account is None:
            mt5.shutdown()
            return {"status": "error", "message": "Unable to fetch account info"}

        # 1. Process Account Info
        account_data = {
            "login": account.login,
            "balance": str(account.balance),
            "equity": str(account.equity),
            "profit": str(account.profit),
            "margin_level": str((account.equity / account.margin) * 100) if account.margin > 0 else "0",
            "company": getattr(account, "company", "N/A"),
        }

        # 2. Process Open Positions
        positions_data = []
        positions = mt5.positions_get()
        if positions:
            for pos in positions:
                positions_data.append({
                    "ticket": int(pos.ticket),
                    "symbol": pos.symbol,
                    "profit": str(pos.profit),
                    "opened_at": datetime.fromtimestamp(pos.time).isoformat(),
                })

        # 3. Process History (Last 30 Days)
        date_from = datetime.now() - timedelta(days=30)
        deals = mt5.history_deals_get(date_from, datetime.now())
        
        trades_list, wins, losses = [], [], []
        daily_pnl_map = defaultdict(float)
        equity_curve = []
        current_equity = account.balance - account.profit
        net_pnl = 0.0

        if deals:
            for deal in sorted(deals, key=lambda x: x.time):
                if deal.entry in [mt5.DEAL_ENTRY_OUT, mt5.DEAL_ENTRY_INOUT]:
                    trade_net = (deal.profit or 0) + (deal.swap or 0) + (deal.commission or 0)
                    net_pnl += trade_net
                    current_equity += trade_net
                    
                    iso_time = datetime.fromtimestamp(deal.time).isoformat()
                    equity_curve.append({"time": iso_time, "equity": str(round(current_equity, 2))})
                    daily_pnl_map[datetime.fromtimestamp(deal.time).strftime('%Y-%m-%d')] += trade_net

                    if trade_net > 0: wins.append(trade_net)
                    else: losses.append(abs(trade_net))

                    trades_list.append({
                        "ticket": int(deal.ticket),
                        "symbol": str(deal.symbol),
                        "net_profit": str(round(trade_net, 2))
                    })

        # 4. Final Aggregation
        total_t = len(wins) + len(losses)
        metrics = {
            "profit_factor": str(round(sum(wins) / sum(losses), 2)) if losses and sum(losses) > 0 else "0",
            "win_percentage": f"{round((len(wins)/total_t*100), 2) if total_t > 0 else 0}%",
            "net_pnl_30d": str(round(net_pnl, 2))
        }

        return {
            "status": "success",
            "data": {
                "account": account_data,
                "open_positions": positions_data,
                "performance_metrics": metrics,
                "equity_vs_time": equity_curve,
                "daily_pnl": [{"date": d, "pnl": str(round(p, 2))} for d, p in sorted(daily_pnl_map.items())],
                "recent_trades": trades_list
            }
        }
    finally:
        mt5.shutdown()