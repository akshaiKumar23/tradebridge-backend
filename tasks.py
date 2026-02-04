# # # import MetaTrader5 as mt5
# # # from datetime import datetime, timedelta
# # # from celery_app import celery_app


# # # @celery_app.task(name="tasks.get_account_summary", bind=True)
# # # def get_account_summary(self, server: str, login: int, password: str):

# # #     try:
      
# # #         if not mt5.initialize():
# # #             return {
# # #                 "status": "error",
# # #                 "message": f"MT5 initialize failed: {mt5.last_error()}"
# # #             }

      
# # #         if not mt5.login(login=login, password=password, server=server):
# # #             mt5.shutdown()
# # #             return {
# # #                 "status": "error",
# # #                 "message": f"MT5 login failed: {mt5.last_error()}"
# # #             }

# # #         response = {"status": "success", "data": {}}

        
# # #         account = mt5.account_info()
# # #         if account is None:
# # #             mt5.shutdown()
# # #             return {
# # #                 "status": "error",
# # #                 "message": "Unable to fetch account info"
# # #             }

# # #         response["data"]["account"] = {
# # #             "login": account.login,
# # #             "name": account.name,
# # #             "server": account.server,
# # #             "currency": account.currency,
# # #             "leverage": account.leverage,
# # #             "balance": account.balance,
# # #             "equity": account.equity,
# # #             "profit": account.profit,
# # #             "margin": account.margin,
# # #             "margin_free": account.margin_free,
# # #             "margin_level": (account.equity / account.margin) * 100 if account.margin > 0 else None,
# # #         }

        
# # #         positions_data = []
# # #         positions = mt5.positions_get()
# # #         if positions:
# # #             for pos in positions:
# # #                 positions_data.append({
# # #                     "ticket": pos.ticket,
# # #                     "symbol": pos.symbol,
# # #                     "type": "BUY" if pos.type == 0 else "SELL",
# # #                     "volume": pos.volume,
# # #                     "price_open": pos.price_open,
# # #                     "price_current": pos.price_current,
# # #                     "profit": pos.profit,
# # #                     "swap": pos.swap,
# # #                     "sl": pos.sl,
# # #                     "tp": pos.tp,
# # #                     "opened_at": datetime.fromtimestamp(pos.time).isoformat(),
# # #                 })

# # #         response["data"]["open_positions"] = positions_data

       
# # #         orders_data = []
# # #         orders = mt5.orders_get()
# # #         if orders:
# # #             for order in orders:
# # #                 orders_data.append({
# # #                     "ticket": order.ticket,
# # #                     "symbol": order.symbol,
# # #                     "type": order.type,
# # #                     "volume": order.volume_current,
# # #                     "price": order.price_open,
# # #                     "sl": order.sl,
# # #                     "tp": order.tp,
# # #                 })

# # #         response["data"]["pending_orders"] = orders_data

        
# # #         date_from = datetime.now() - timedelta(days=7)
# # #         date_to = datetime.now()

# # #         trades_data = []
# # #         deals = mt5.history_deals_get(date_from, date_to)

# # #         trades_data = []
# # #         if deals:
# # #             for deal in deals:
                
# # #                 if deal.entry == mt5.DEAL_ENTRY_OUT or deal.entry == mt5.DEAL_ENTRY_INOUT:
# # #                     trades_data.append({
# # #                         "ticket": deal.ticket,
# # #                         "order": deal.order,
# # #                         "time": datetime.fromtimestamp(deal.time).isoformat(),
# # #                         "symbol": deal.symbol,
# # #                         "type": "BUY" if deal.type == mt5.DEAL_TYPE_BUY else "SELL",
# # #                         "volume": deal.volume,
# # #                         "price": deal.price,
# # #                         "profit": deal.profit,
# # #                         "swap": deal.swap,
# # #                         "commission": deal.commission,
# # #                     })

# # #         response["data"]["recent_trades"] = trades_data


# # #         return response

# # #     except Exception as e:
# # #         return {
# # #             "status": "error",
# # #             "message": str(e)
# # #         }
# # #     finally:
       
# # #         mt5.shutdown()




# # import MetaTrader5 as mt5
# # from datetime import datetime, timedelta
# # from celery_app import celery_app



# # # import boto3

# # # dynamodb = boto3.resource(
# # #     "dynamodb",
# # #     region_name="ap-south-1"
# # # )

# # # table = dynamodb.Table("Users")
# # # table.put_item(Item={"id": "1", "name": "Aman"})





# # @celery_app.task(name="tasks.get_account_summary", bind=True)
# # def get_account_summary(self, server: str, login: int, password: str):

# #     try:
# #         if not mt5.initialize():
# #             return {
# #                 "status": "error",
# #                 "message": f"MT5 initialize failed: {mt5.last_error()}"
# #             }

# #         if not mt5.login(login=login, password=password, server=server):
# #             mt5.shutdown()
# #             return {
# #                 "status": "error",
# #                 "message": f"MT5 login failed: {mt5.last_error()}"
# #             }

# #         response = {"status": "success", "data": {}}

# #         # ---------------- ACCOUNT ----------------
# #         account = mt5.account_info()
# #         if account is None:
# #             mt5.shutdown()
# #             return {
# #                 "status": "error",
# #                 "message": "Unable to fetch account info"
# #             }

# #         response["data"]["account"] = {
# #             "login": account.login,
# #             "name": account.name,
# #             "server": account.server,
# #             "currency": account.currency,
# #             "leverage": account.leverage,
# #             "balance": account.balance,
# #             "equity": account.equity,
# #             "profit": account.profit,
# #             "margin": account.margin,
# #             "margin_free": account.margin_free,
# #             "margin_level": (account.equity / account.margin) * 100 if account.margin > 0 else None,
# #         }

# #         # ---------------- OPEN POSITIONS ----------------
# #         positions_data = []
# #         positions = mt5.positions_get()

# #         if positions:
# #             for pos in positions:
# #                 positions_data.append({
# #                     "ticket": pos.ticket,
# #                     "symbol": pos.symbol,
# #                     "type": "BUY" if pos.type == 0 else "SELL",
# #                     "volume": pos.volume,
# #                     "price_open": pos.price_open,
# #                     "price_current": pos.price_current,
# #                     "profit": pos.profit,
# #                     "swap": pos.swap,
# #                     "sl": pos.sl,
# #                     "tp": pos.tp,
# #                     "opened_at": datetime.fromtimestamp(pos.time).isoformat(),
# #                 })

# #         response["data"]["open_positions"] = positions_data

# #         # ---------------- PENDING ORDERS ----------------
# #         orders_data = []
# #         orders = mt5.orders_get()

# #         if orders:
# #             for order in orders:
# #                 orders_data.append({
# #                     "ticket": order.ticket,
# #                     "symbol": order.symbol,
# #                     "type": order.type,
# #                     "volume": order.volume_current,
# #                     "price": order.price_open,
# #                     "sl": order.sl,
# #                     "tp": order.tp,
# #                 })

# #         response["data"]["pending_orders"] = orders_data

# #         # ---------------- RECENT CLOSED TRADES ----------------
# #         date_from = datetime.now() - timedelta(days=7)
# #         date_to = datetime.now()

# #         trades_data = []
# #         net_pnl = 0.0   # ✅ Always initialized

# #         deals = mt5.history_deals_get(date_from, date_to)

# #         if deals:
# #             for deal in deals:
# #                 if deal.entry == mt5.DEAL_ENTRY_OUT or deal.entry == mt5.DEAL_ENTRY_INOUT:

# #                     profit = deal.profit or 0
# #                     swap = deal.swap or 0
# #                     commission = deal.commission or 0

# #                     trade_net = profit + swap + commission
# #                     net_pnl += trade_net

# #                     trades_data.append({
# #                         "ticket": deal.ticket,
# #                         "time": datetime.fromtimestamp(deal.time).isoformat(),
# #                         "symbol": deal.symbol,
# #                         "type": "BUY" if deal.type == mt5.DEAL_TYPE_BUY else "SELL",
# #                         "volume": deal.volume,
# #                         "price": deal.price,
# #                         "profit": profit,
# #                         "swap": swap,
# #                         "commission": commission,
# #                         "net_profit": trade_net
# #                     })

# #         # ✅ Always return keys even if no trades
# #         response["data"]["recent_trades"] = trades_data
# #         #response["data"]["recent_trades_net_pnl"] = round(net_pnl, 2)

# #         response["data"]["testing"] = "TESTINGGGGGGG"

# #         return response

# #     except Exception as e:
# #         return {
# #             "status": "error",
# #             "message": str(e)
# #         }

# #     finally:
# #         mt5.shutdown()



# ###########################################################################
# import MetaTrader5 as mt5
# from datetime import datetime, timedelta
# from celery_app import celery_app
# from collections import defaultdict
# import boto3
# from boto3.dynamodb.conditions import Key

# # Initialize DynamoDB Resource
# # Using the resource interface is the standard way to avoid the "M" and "S" nesting
# dynamodb = boto3.resource(
#     "dynamodb",
#     region_name="ap-south-1"
# )
# table = dynamodb.Table("Users")

# @celery_app.task(name="tasks.get_account_summary", bind=True)
# def get_account_summary(self, server: str, login: int, password: str):
#     try:
#         if not mt5.initialize():
#             return {"status": "error", "message": f"MT5 initialize failed: {mt5.last_error()}"}

#         if not mt5.login(login=login, password=password, server=server):
#             mt5.shutdown()
#             return {"status": "error", "message": f"MT5 login failed: {mt5.last_error()}"}

#         response = {"status": "success", "data": {}}

#         # ---------------- ACCOUNT INFORMATION ----------------
#         account = mt5.account_info()
#         if account is None:
#             mt5.shutdown()
#             return {"status": "error", "message": "Unable to fetch account info"}

#         # Note: We use strings for numbers because DynamoDB does not support Python floats well
#         response["data"]["account"] = {
#             "login": account.login,
#             "name": account.name,
#             "server": account.server,
#             "currency": account.currency,
#             "leverage": account.leverage,
#             "balance": str(account.balance),
#             "equity": str(account.equity),
#             "profit": str(account.profit),
#             "margin": str(account.margin),
#             "margin_free": str(account.margin_free),
#             "margin_level": str((account.equity / account.margin) * 100) if account.margin > 0 else "0",
#             "company": getattr(account, "company", "N/A"),
#             "trade_mode": getattr(account, "trade_mode", "N/A"),
#             "limit_orders": getattr(account, "limit_orders", 0)
#         }

#         # ---------------- OPEN POSITIONS ----------------
#         positions_data = []
#         positions = mt5.positions_get()
#         if positions:
#             for pos in positions:
#                 positions_data.append({
#                     "ticket": int(pos.ticket),
#                     "symbol": pos.symbol,
#                     "type": "BUY" if pos.type == 0 else "SELL",
#                     "volume": str(pos.volume),
#                     "price_open": str(pos.price_open),
#                     "price_current": str(pos.price_current),
#                     "profit": str(pos.profit),
#                     "swap": str(pos.swap),
#                     "sl": str(pos.sl),
#                     "tp": str(pos.tp),
#                     "opened_at": datetime.fromtimestamp(pos.time).isoformat(),
#                 })
#         response["data"]["open_positions"] = positions_data

#         # ---------------- TRADING HISTORY & ANALYTICS ----------------
#         date_from = datetime.now() - timedelta(days=30)
#         date_to = datetime.now()
#         deals = mt5.history_deals_get(date_from, date_to)
        
#         trades_data = []
#         rr_ratios = []
#         wins, losses = [], []
#         daily_pnl_map = defaultdict(float)
#         equity_curve = []
        
#         current_equity = account.balance - account.profit
#         gross_profit, gross_loss, net_pnl = 0.0, 0.0, 0.0

#         if deals:
#             sorted_deals = sorted(deals, key=lambda x: x.time)
#             for deal in sorted_deals:
#                 if deal.entry in [mt5.DEAL_ENTRY_OUT, mt5.DEAL_ENTRY_INOUT]:
#                     p_val = deal.profit or 0.0
#                     s_val = deal.swap or 0.0
#                     c_val = deal.commission or 0.0
#                     trade_net = p_val + s_val + c_val
#                     net_pnl += trade_net
                    
#                     current_equity += trade_net
#                     d_time = datetime.fromtimestamp(deal.time).isoformat()
#                     equity_curve.append({"time": d_time, "equity": str(round(current_equity, 2))})

#                     d_date = datetime.fromtimestamp(deal.time).strftime('%Y-%m-%d')
#                     daily_pnl_map[d_date] += trade_net

#                     if trade_net > 0:
#                         wins.append(trade_net)
#                         gross_profit += trade_net
#                     elif trade_net < 0:
#                         losses.append(abs(trade_net))
#                         gross_loss += abs(trade_net)

#                     # Historical Trade Entry - Use standard types to prevent nesting
#                     trades_data.append({
#                         "ticket": int(deal.ticket),
#                         "time": d_time,
#                         "symbol": str(deal.symbol),
#                         "net_profit": str(round(trade_net, 2))
#                     })

#         # --- Performance Metrics ---
#         total_t = len(wins) + len(losses)
#         response["data"]["performance_metrics"] = {
#             "profit_factor": str(round(gross_profit / gross_loss, 2)) if gross_loss > 0 else "0",
#             "win_percentage": f"{round((len(wins)/total_t*100), 2) if total_t > 0 else 0}%",
#             "net_pnl_30d": str(round(net_pnl, 2))
#         }

#         response["data"]["equity_vs_time"] = equity_curve
#         response["data"]["daily_pnl"] = [{"date": d, "pnl": str(round(p, 2))} for d, p in sorted(daily_pnl_map.items())]
#         response["data"]["recent_trades"] = trades_data
#         response["data"]["testing"] = "TESTING_SUCCESSFUL"

#         # ---------------- SAVE TO DYNAMODB ----------------
#         try:
#             # We explicitly define the item to ensure it's a clean dictionary
#             item_to_save = {
#                 "id": str(account.login),
#                 "account": response["data"]["account"],
#                 "open_positions": response["data"]["open_positions"],
#                 "performance_metrics": response["data"]["performance_metrics"],
#                 "equity_vs_time": response["data"]["equity_vs_time"],
#                 "daily_pnl": response["data"]["daily_pnl"],
#                 "recent_trades": response["data"]["recent_trades"],
#                 "last_updated": datetime.now().isoformat()
#             }
            
#             # Using table.put_item with a standard dict prevents the "M" / "S" structure
#             table.put_item(Item=item_to_save)
            
#         except Exception as db_e:
#             print(f"DynamoDB Error: {str(db_e)}")

#         return response

#     except Exception as e:
#         return {"status": "error", "message": str(e)}
#     finally:
#         mt5.shutdown()











from celery_app import celery_app
from mt5_logic import fetch_mt5_analytics
from database import save_user_performance_data

@celery_app.task(name="tasks.get_account_summary", bind=True)
def get_account_summary(self, server, login, password):
    # 1. Fetch Processed Data
    result = fetch_mt5_analytics(server, login, password)
    
    # 2. Save to DynamoDB if successful
    if result["status"] == "success":
        save_user_performance_data(login, result["data"])
        result["data"]["testing"] = "TESTING_SUCCESSFUL"
        
    return result