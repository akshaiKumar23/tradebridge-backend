import MetaTrader5 as mt5
from datetime import datetime
from collections import defaultdict
import time


def fetch_mt5_analytics(server, login, password):

    try:

        # ---------------- INITIALIZE ----------------

        # Shutdown first to clear any lingering state from previous task
        mt5.shutdown()

        if not mt5.initialize():
            time.sleep(2)
            if not mt5.initialize():
                return {
                    "status": "error",
                    "message": str(mt5.last_error())
                }

        if not mt5.login(login=login, password=password, server=server):
            mt5.shutdown()
            return {
                "status": "error",
                "message": str(mt5.last_error())
            }

        account = mt5.account_info()

        if account is None:
            mt5.shutdown()
            return {
                "status": "error",
                "message": "Account info failed"
            }

        # ---------------- ACCOUNT ----------------

        account_data = {
            "login": account.login,
            "balance": str(account.balance),
            "equity": str(account.equity),
            "profit": str(account.profit),
            "margin_level":
                str((account.equity / account.margin) * 100)
                if account.margin > 0 else "0",
            "company":
                getattr(account, "company", "N/A")
        }

        # ---------------- OPEN POSITIONS ----------------

        positions_data = []
        positions = mt5.positions_get()

        if positions:
            for pos in positions:
                positions_data.append({
                    "ticket": int(pos.ticket),
                    "symbol": pos.symbol,
                    "profit": str(pos.profit),
                    "volume": float(pos.volume),
                    "opened_at":
                        datetime.fromtimestamp(pos.time).isoformat()
                })

        # ---------------- FETCH ALL DEALS ----------------

        deals = mt5.history_deals_get(0, datetime.now()) or []
        deals = sorted(deals, key=lambda d: d.time)

        print(f"\n=== FETCHING ALL HISTORY ===")
        print(f"Total deals fetched: {len(deals)}")

        # ---------------- GROUP BY POSITION ----------------

        positions_map = defaultdict(list)

        for deal in deals:
            # Skip balance operations
            if deal.type == mt5.DEAL_TYPE_BALANCE:
                continue
            positions_map[deal.position_id].append(deal)

        print(f"Unique positions: {len(positions_map)}")

        # ---------------- ANALYTICS ----------------

        trades_list = []
        equity_curve = []
        daily_pnl_map = defaultdict(float)
        wins = []
        losses = []
        net_pnl = 0.0
        current_equity = account.balance - account.profit

        # ---------------- PROCESS EACH POSITION ----------------

        for position_id, position_deals in positions_map.items():

            entry_deal = None
            exit_deal = None
            
            total_profit = 0.0
            total_swap = 0.0
            total_commission = 0.0
            total_volume = 0.0
            
            symbol = None
            open_time = None
            close_time = None

            # Aggregate all deals for this position
            for deal in position_deals:
                symbol = deal.symbol

                if deal.entry == mt5.DEAL_ENTRY_IN:
                    if entry_deal is None:
                        entry_deal = deal
                        open_time = deal.time

                if deal.entry in (mt5.DEAL_ENTRY_OUT, mt5.DEAL_ENTRY_INOUT):
                    exit_deal = deal
                    close_time = deal.time

                total_profit += deal.profit or 0.0
                total_swap += deal.swap or 0.0
                total_commission += deal.commission or 0.0
                total_volume += deal.volume

            # Skip if position not closed
            if close_time is None:
                continue

            # Calculate net P&L for entire position
            trade_net = total_profit + total_swap + total_commission
            net_pnl += trade_net

            # Classify win/loss
            if trade_net > 0:
                wins.append(trade_net)
            elif trade_net < 0:
                losses.append(abs(trade_net))

            # Calculate hold time
            hold_time_minutes = 0
            if open_time:
                hold_time_minutes = (close_time - open_time) / 60

            # Update equity curve
            current_equity += trade_net
            equity_curve.append({
                "timestamp": close_time,
                "equity": round(current_equity, 2)
            })

            # Daily P&L
            date_str = datetime.fromtimestamp(close_time).strftime('%Y-%m-%d')
            daily_pnl_map[date_str] += trade_net

            # Risk calculation
            risk_amount = None
            position_history = mt5.history_orders_get(position=position_id)
            
            if position_history:
                for order in position_history:
                    if hasattr(order, "sl") and order.sl > 0 and entry_deal:
                        risk_amount = abs(
                            order.price_open - order.sl
                        ) * order.volume_initial
                        break

            # Fallback risk
            if not risk_amount or risk_amount == 0:
                if losses:
                    risk_amount = sum(losses) / len(losses)
                else:
                    risk_amount = abs(trade_net) if trade_net != 0 else 1

            if risk_amount == 0:
                risk_amount = 1

            r_multiple = round(trade_net / risk_amount, 2)

            # Save complete trade
            trades_list.append({
                "ticket": int(exit_deal.ticket) if exit_deal else 0,
                "position_id": int(position_id),
                "symbol": symbol,
                "pnl": float(round(trade_net, 2)),
                "open_time": open_time,
                "close_time": close_time,
                "hold_time_minutes": round(hold_time_minutes, 2),
                "volume": round(total_volume, 2),
                "r_multiple": r_multiple,
                "risk_amount": round(risk_amount, 2),
                "timestamp": close_time,
                "trade_id": int(position_id),
                "entry_price": float(entry_deal.price) if entry_deal else 0,
                "exit_price": float(exit_deal.price) if exit_deal else 0,
                "direction": "LONG" if entry_deal.type == mt5.DEAL_TYPE_BUY else "SHORT",
            })

        # ---------------- METRICS ----------------

        total_trades = len(trades_list)
        avg_win = sum(wins) / len(wins) if wins else 0
        avg_loss = sum(losses) / len(losses) if losses else 0
        expectancy = net_pnl / total_trades if total_trades else 0
        profit_factor = sum(wins) / sum(losses) if losses else 0

        metrics = {
            "net_pnl_30d": str(round(net_pnl, 2)),
            "total_trades": total_trades,
            "wins": len(wins),
            "losses": len(losses),
            "win_percentage":
                round((len(wins)/total_trades)*100, 2)
                if total_trades else 0,
            "profit_factor": round(profit_factor, 2),
            "expectancy": round(expectancy, 2),
            "avg_win": round(avg_win, 2),
            "avg_loss": round(avg_loss, 2)
        }

        print("\n=== FINAL SUMMARY ===")
        print(f"Closed positions: {len(trades_list)}")
        print(f"Wins: {len(wins)}, Losses: {len(losses)}")
        print(f"Equity points: {len(equity_curve)}")
        
        if trades_list:
            print(f"\n=== SAMPLE TRADES ===")
            for i, t in enumerate(sorted(trades_list, key=lambda x: x["close_time"])[:3]):
                dt = datetime.fromtimestamp(t['close_time']).strftime('%Y-%m-%d %H:%M')
                print(f"{i+1}. {t['symbol']} PnL={t['pnl']} @ {dt}")

        # ---------------- RETURN ----------------

        return {
            "status": "success",
            "data": {
                "account": account_data,
                "open_positions": positions_data,
                "performance_metrics": metrics,
                "equity_vs_time": equity_curve,
                "daily_pnl": [
                    {"date": d, "pnl": str(round(p, 2))}
                    for d, p in sorted(daily_pnl_map.items())
                ],
                "trades": sorted(trades_list, key=lambda x: x["close_time"])
            }
        }

    finally:
        mt5.shutdown()