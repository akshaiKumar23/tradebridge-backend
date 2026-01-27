import MetaTrader5 as mt5
from datetime import datetime, timedelta
from celery_app import celery_app


@celery_app.task(name="tasks.get_account_summary", bind=True)
def get_account_summary(self, server: str, login: int, password: str):

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

        response = {"status": "success", "data": {}}

        
        account = mt5.account_info()
        if account is None:
            mt5.shutdown()
            return {
                "status": "error",
                "message": "Unable to fetch account info"
            }

        response["data"]["account"] = {
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
                    "opened_at": datetime.fromtimestamp(pos.time).isoformat(),
                })

        response["data"]["open_positions"] = positions_data

       
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

        response["data"]["pending_orders"] = orders_data

        
        date_from = datetime.now() - timedelta(days=7)
        date_to = datetime.now()

        trades_data = []
        deals = mt5.history_deals_get(date_from, date_to)

        trades_data = []
        if deals:
            for deal in deals:
                
                if deal.entry == mt5.DEAL_ENTRY_OUT or deal.entry == mt5.DEAL_ENTRY_INOUT:
                    trades_data.append({
                        "ticket": deal.ticket,
                        "order": deal.order,
                        "time": datetime.fromtimestamp(deal.time).isoformat(),
                        "symbol": deal.symbol,
                        "type": "BUY" if deal.type == mt5.DEAL_TYPE_BUY else "SELL",
                        "volume": deal.volume,
                        "price": deal.price,
                        "profit": deal.profit,
                        "swap": deal.swap,
                        "commission": deal.commission,
                    })

        response["data"]["recent_trades"] = trades_data[-5:]


        return response

    except Exception as e:
        return {
            "status": "error",
            "message": str(e)
        }
    finally:
       
        mt5.shutdown()
