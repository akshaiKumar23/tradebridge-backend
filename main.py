from fastapi import FastAPI, HTTPException
import MetaTrader5 as mt5
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv

load_dotenv()

SERVER = os.getenv("MT5_SERVER", "")
LOGIN = int(os.getenv("MT5_LOGIN", "60906"))
PASSWORD = os.getenv("MT5_PASSWORD", "")

app = FastAPI(title="MT5 Account API")


@app.get("/account/summary")
def account_summary():
    # Initialize MT5
    if not mt5.initialize():
        raise HTTPException(
            status_code=500,
            detail=f"MT5 initialize failed: {mt5.last_error()}"
        )

    # Login
    if not mt5.login(login=LOGIN, password=PASSWORD, server=SERVER):
        mt5.shutdown()
        raise HTTPException(
            status_code=401,
            detail=f"MT5 login failed: {mt5.last_error()}"
        )

    response = {}

    # Account Info
    account = mt5.account_info()
    if account is None:
        mt5.shutdown()
        raise HTTPException(status_code=500, detail="Unable to fetch account info")

    response["account"] = {
        "login": account.login,
        "name": account.name,
        "server": account.server,
        "currency": account.currency,
        "leverage": account.leverage,
        "balance": account.balance,
        "equity": account.equity,
        "profit": account.profit,
        "margin": account.margin,
        "margin_free": account.margin_free,
        "margin_level": (account.equity / account.margin) * 100 if account.margin > 0 else None,
    }

    # Open Positions
    positions_data = []
    positions = mt5.positions_get()
    if positions:
        for pos in positions:
            positions_data.append({
                "ticket": pos.ticket,
                "symbol": pos.symbol,
                "type": "BUY" if pos.type == 0 else "SELL",
                "volume": pos.volume,
                "price_open": pos.price_open,
                "price_current": pos.price_current,
                "profit": pos.profit,
                "swap": pos.swap,
                "sl": pos.sl,
                "tp": pos.tp,
                "opened_at": datetime.fromtimestamp(pos.time),
            })

    response["open_positions"] = positions_data

    # Pending Orders
    orders_data = []
    orders = mt5.orders_get()
    if orders:
        for order in orders:
            orders_data.append({
                "ticket": order.ticket,
                "symbol": order.symbol,
                "type": order.type,
                "volume": order.volume_current,
                "price": order.price_open,
                "sl": order.sl,
                "tp": order.tp,
            })

    response["pending_orders"] = orders_data

    # Recent Trades (7 days)
    date_from = datetime.now() - timedelta(days=7)
    date_to = datetime.now()

    trades_data = []
    deals = mt5.history_deals_get(date_from, date_to)
    if deals:
        for deal in deals:
            if deal.type in (0, 1):  # BUY / SELL
                trades_data.append({
                    "time": datetime.fromtimestamp(deal.time),
                    "symbol": deal.symbol,
                    "type": "BUY" if deal.type == 0 else "SELL",
                    "volume": deal.volume,
                    "price": deal.price,
                    "profit": deal.profit,
                })

    response["recent_trades"] = trades_data[-5:]

    mt5.shutdown()
    return response
