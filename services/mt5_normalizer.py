from datetime import datetime
from collections import defaultdict


def normalize_mt5_data(mt5_data: dict):

    symbols = defaultdict(lambda: {
        "pnl": 0.0,
        "trades": 0,
        "wins": 0,
        "losses": 0
    })

    total_pnl = 0.0
    wins = []
    losses = []

    hold_times = []
    consecutive_losses = 0
    max_consecutive_losses = 0

    trading_hours = defaultdict(int)

    revenge_trades = 0
    last_trade_loss = False

    volumes = []

    trades = mt5_data.get("trades", [])

    for trade in trades:

        pnl = float(trade["pnl"])
        symbol = trade["symbol"]

        symbols[symbol]["pnl"] += pnl
        symbols[symbol]["trades"] += 1

        total_pnl += pnl

        volumes.append(trade.get("volume", 0))

       
        if pnl > 0:
            symbols[symbol]["wins"] += 1
            wins.append(pnl)
            consecutive_losses = 0
            last_trade_loss = False
        else:
            symbols[symbol]["losses"] += 1
            losses.append(abs(pnl))

            consecutive_losses += 1
            max_consecutive_losses = max(
                max_consecutive_losses,
                consecutive_losses
            )

            if last_trade_loss:
                revenge_trades += 1

            last_trade_loss = True

        
        if "close_time" in trade:
            hour = datetime.fromtimestamp(
                trade["close_time"]
            ).hour

            trading_hours[hour] += 1

    total_trades = len(trades)

    sum_wins = sum(wins)
    sum_losses = sum(losses)

    win_rate = (
        (len(wins) / total_trades) * 100
        if total_trades > 0 else 0
    )

    profit_factor = (
        sum_wins / sum_losses
        if sum_losses > 0 else 0
    )

    expectancy = (
        total_pnl / total_trades
        if total_trades > 0 else 0
    )

    avg_win = sum_wins / len(wins) if wins else 0
    avg_loss = sum_losses / len(losses) if losses else 0

   
    best_hour = None

    if trading_hours:
        best_hour = max(
            trading_hours.items(),
            key=lambda x: x[1]
        )[0]

   
    avg_volume = sum(volumes) / len(volumes) if volumes else 0

    return {

        
        "total_trades": total_trades,
        "total_pnl": round(total_pnl, 2),
        "wins": len(wins),
        "losses": len(losses),
        "win_rate": round(win_rate, 2),
        "profit_factor": round(profit_factor, 2),
        "expectancy": round(expectancy, 2),
        "avg_win": round(avg_win, 2),
        "avg_loss": round(avg_loss, 2),

        
        "avg_hold_time_minutes": 0,  
        "max_consecutive_losses": max_consecutive_losses,
        "best_trading_hour": best_hour,
        "overtrading_signals": (
            "Detected"
            if total_trades > 20 else
            "None Detected"
        ),
        "revenge_trading_count": revenge_trades,
        "avg_volume": round(avg_volume, 2),

        "symbols": dict(symbols),
    }
