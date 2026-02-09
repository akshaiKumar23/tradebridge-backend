from datetime import datetime
from decimal import Decimal
from db.dynamodb import get_performance_snapshots_table

def save_user_performance_snapshot(user_id: str, snapshot_date: str, data: dict):
    table = get_performance_snapshots_table()

    item = {
        "user_id": user_id,
        "snapshot_date": snapshot_date,
        "symbols": {
            k: {
                "pnl": Decimal(str(v["pnl"])),
                "trades": Decimal(v["trades"]),
                "wins": Decimal(v["wins"]),
                "losses": Decimal(v["losses"]),
            }
            for k, v in data["symbols"].items()
        },
        "total_trades": Decimal(data["total_trades"]),
        "total_pnl": Decimal(str(data["total_pnl"])),
        "created_at": datetime.utcnow().isoformat()
    }

    table.put_item(Item=item)
