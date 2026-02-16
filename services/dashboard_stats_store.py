from datetime import datetime
from decimal import Decimal
from db.dynamodb import get_dashboard_stats_table


def save_dashboard_stats(user_id: str, snapshot_date: str, analytics: dict):

    table = get_dashboard_stats_table()

    wins = analytics.get("wins", 0)
    losses = analytics.get("losses", 0)

    avg_rr = 0

    if losses > 0:
        avg_rr = analytics["avg_win"] / analytics["avg_loss"]

    current_time = datetime.utcnow().isoformat()

    item = {
        "user_id": user_id,
        "snapshot_date": snapshot_date,
        "total_pnl": Decimal(str(analytics["total_pnl"])),
        "avg_rr": Decimal(str(round(avg_rr, 2))),
        "profit_factor": Decimal(str(analytics["profit_factor"])),
        "avg_win": Decimal(str(analytics["avg_win"])),
        "avg_loss": Decimal(str(analytics["avg_loss"])),
        "win_rate": Decimal(str(analytics["win_rate"])),
        "created_at": current_time,
        "updated_at": current_time
    }

 
    print(f"\n=== SAVING DASHBOARD STATS ===")
    print(f"User ID: {user_id}")
    print(f"Snapshot Date: {snapshot_date}")
    print(f"Item to save: {item}")
    
    try:
        response = table.put_item(Item=item)
        print(f"Successfully saved dashboard stats")
        print(f"Response: {response}")
    except Exception as e:
        print(f"ERROR saving dashboard stats: {str(e)}")
        raise
    
    try:
        verify = table.get_item(Key={"user_id": user_id, "snapshot_date": snapshot_date})
        if verify.get("Item"):
            print(f"✓ Verified: Item exists in table")
            print(f"Retrieved item: {verify['Item']}")
        else:
            print(f"WARNING: Item not found after put_item!")
    except Exception as e:
        print(f"ERROR verifying save: {str(e)}")