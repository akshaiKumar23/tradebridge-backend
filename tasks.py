from celery_app import celery_app
from datetime import datetime
from mt5_logic import fetch_mt5_analytics
from services.mt5_normalizer import normalize_mt5_data
from services.performance_store import save_user_performance_snapshot
from db.dynamodb import get_onboarding_table


@celery_app.task(name="tasks.get_account_summary", bind=True)
def get_account_summary(self, user_id, server, login, password):
    self.update_state(
        state="PROGRESS",
        meta={"step": "connecting_to_mt5"}
    )

    result = fetch_mt5_analytics(server, login, password)

    if result["status"] != "success":
        return result

    self.update_state(
        state="PROGRESS",
        meta={"step": "normalizing_data"}
    )

    normalized = normalize_mt5_data(result["data"])

    self.update_state(
        state="PROGRESS",
        meta={"step": "saving_snapshot"}
    )

    save_user_performance_snapshot(
        user_id=user_id,
        snapshot_date=datetime.utcnow().date().isoformat(),
        data=normalized
    )

    self.update_state(
        state="PROGRESS",
        meta={"step": "finalizing_onboarding"}
    )

    onboarding_table = get_onboarding_table()
    response = onboarding_table.get_item(Key={"user_id": user_id})
    
    item = response.get("Item", {})
    
    onboarding_table.put_item(
        Item={
            "user_id": user_id,
            "broker_name": item.get("broker_name"), 
            "broker_linked": True,  
            "sync_task_id": item.get("sync_task_id"), 
            "created_at": item.get("created_at", datetime.utcnow().isoformat()),
            "updated_at": datetime.utcnow().isoformat(),
        }
    )

    return {
        "status": "success",
        "message": "Performance snapshot saved",
        "summary": {
            "total_trades": normalized["total_trades"],
            "total_pnl": normalized["total_pnl"]
        }
    }