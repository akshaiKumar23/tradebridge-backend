from collections import defaultdict

def normalize_mt5_data(mt5_data: dict):
    symbols = defaultdict(lambda: {
        "pnl": 0,
        "trades": 0,
        "wins": 0,
        "losses": 0
    })

    for trade in mt5_data.get("trades", []):
        symbol = trade["symbol"]
        pnl = float(trade["pnl"])

        symbols[symbol]["pnl"] += pnl
        symbols[symbol]["trades"] += 1

        if pnl > 0:
            symbols[symbol]["wins"] += 1
        else:
            symbols[symbol]["losses"] += 1

    return {
        "symbols": dict(symbols),
        "total_trades": sum(v["trades"] for v in symbols.values()),
        "total_pnl": sum(v["pnl"] for v in symbols.values())
    }
