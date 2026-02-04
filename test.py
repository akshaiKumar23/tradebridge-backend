# # import boto3

# # dynamodb = boto3.resource(
# #     "dynamodb",
# #     region_name="ap-south-1"
# # )

# # table = dynamodb.Table("Users")
# # table.put_item(Item={"id": "1", "name": "Connection"})
# # table.put_item(Item={"id": "2", "name": "Connection"})



# response["data"]["account"] = {
#             "login": account.login,
#             "name": account.name,
#             "server": account.server,
#             "currency": account.currency,
#             "leverage": account.leverage,
#             "balance": account.balance,
#             "equity": account.equity,
#             "profit": account.profit,
#             "margin": account.margin,
#             "margin_free": account.margin_free,
#             "margin_level": (account.equity / account.margin) * 100 if account.margin > 0 else None,
#         }





# # response["data"]["account"] = {
# #             "login": account.login,
# #             "balance": account.balance,
# #             "equity": account.equity,
# #             "profit": account.profit,
# #             "margin": account.margin,
# #             "margin_free": account.margin_free
# #         }




# trades_data.append({
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








# import MetaTrader5 as mt5
# from datetime import datetime, timedelta
# from celery_app import celery_app

# @celery_app.task(name="tasks.get_account_summary", bind=True)
# def get_account_summary(self, server: str, login: int, password: str):
#     try:
#         if not mt5.initialize():
#             return {"status": "error", "message": f"MT5 initialize failed: {mt5.last_error()}"}

#         if not mt5.login(login=login, password=password, server=server):
#             mt5.shutdown()
#             return {"status": "error", "message": f"MT5 login failed: {mt5.last_error()}"}

#         response = {"status": "success", "data": {}}

#         # --- Account Information ---
#         account = mt5.account_info()
#         if account is None:
#             mt5.shutdown()
#             return {"status": "error", "message": "Unable to fetch account info"}

#         response["data"]["account"] = {
#             "login": account.login,
#             "balance": account.balance,
#             "equity": account.equity,
#             "profit": account.profit,
#             "margin": account.margin,
#             "margin_free": account.margin_free
#         }

#         # --- Recent Trades & PnL ---
#         date_from = datetime.now() - timedelta(days=7)
#         date_to = datetime.now()
#         trades_data = []
#         net_pnl = 0.0

#         deals = mt5.history_deals_get(date_from, date_to)
#         if deals:
#             for deal in deals:
#                 # We only count 'OUT' deals (closing trades) for PnL
#                 if deal.entry in [mt5.DEAL_ENTRY_OUT, mt5.DEAL_ENTRY_INOUT]:
#                     profit = deal.profit or 0.0
#                     swap = deal.swap or 0.0
#                     comm = deal.commission or 0.0
#                     trade_net = profit + swap + comm
#                     net_pnl += trade_net

#                     trades_data.append({
#                         "ticket": deal.ticket,
#                         "symbol": deal.symbol,
#                         "time": datetime.fromtimestamp(deal.time).isoformat(),
#                         "net_profit": round(trade_net, 2)
#                     })

#         # ✅ FIXED: Added/Uncommented the requested keys
#         response["data"]["recent_trades"] = trades_data
#         response["data"]["recent_trades_net_pnl"] = round(net_pnl, 2)
#         response["data"]["testing"] = "TESTINGGGGGGG" 

#         return response

#     except Exception as e:
#         return {"status": "error", "message": str(e)}
#     finally:
#         mt5.shutdown()
