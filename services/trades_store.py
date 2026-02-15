from decimal import Decimal
from db.dynamodb import get_trades_table


def save_user_trades(user_id: str, trades: list):
    table = get_trades_table()
    
    with table.batch_writer() as batch: 
        for trade in trades:
            print(f"Saving trade: {trade['position_id']} at {trade['timestamp']}")
            batch.put_item(
                Item={
                    "user_id": user_id,
                    "timestamp": int(trade["timestamp"]),
                    "position_id": int(trade["position_id"]),
                    "symbol": trade["symbol"],
                    "direction": "LONG" if trade["pnl"] >= 0 else "SHORT",
                    "entry_price": Decimal("0"),
                    "exit_price": Decimal("0"),
                    "volume": Decimal(str(trade["volume"])),
                    "pnl": Decimal(str(trade["pnl"])),
                    "r_multiple": Decimal(str(trade["r_multiple"])),
                    "risk_amount": Decimal(str(trade["risk_amount"])),
                    "tags": ["MT5 Trade"]
                }
            )
    print(f"Successfully saved {len(trades)} trades")