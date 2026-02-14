from celery_app import celery_app
from datetime import datetime
from mt5_logic import fetch_mt5_analytics
from services.mt5_normalizer import normalize_mt5_data
from services.performance_store import save_user_performance_snapshot
from services.analytics_store import save_user_analytics_stats
from db.dynamodb import get_onboarding_table
from services.equity_store import save_equity_curve
from services.pnl_weekly_store import save_weekly_pnl
from services.r_multiple_store import save_r_multiples




@celery_app.task(name="tasks.get_account_summary", bind=True)
def get_account_summary(self, user_id, server, login, password):

    self.update_state(state="PROGRESS", meta={"step": "connecting_to_mt5"})

    result = fetch_mt5_analytics(server, login, password)

    if result["status"] != "success":
        return result


    self.update_state(state="PROGRESS", meta={"step": "normalizing_data"})

    normalized = normalize_mt5_data(result["data"])


    snapshot_date = datetime.utcnow().date().isoformat()


        
    self.update_state(state="PROGRESS", meta={"step": "saving_analytics_stats"})

    save_user_analytics_stats(
        user_id=user_id,
        snapshot_date=snapshot_date,
        analytics=normalized
    )


    self.update_state(state="PROGRESS", meta={"step": "saving_performance_snapshot"})

    save_user_performance_snapshot(
        user_id=user_id,
        snapshot_date=snapshot_date,
        data=normalized
    )

    self.update_state(state="PROGRESS", meta={"step": "saving_equity_curve"})

    save_equity_curve(
        user_id=user_id,
        equity_curve=result["data"]["equity_vs_time"]
    )

    self.update_state(
        state="PROGRESS",
        meta={"step": "saving_weekly_pnl"}
    )

    weekly_pnl = normalized.get("weekly_pnl", {})

    if weekly_pnl:

        save_weekly_pnl(
            user_id=user_id,
            weekly_pnl=weekly_pnl
        )

    self.update_state(state="PROGRESS", meta={"step": "saving_r_multiples"})

    save_r_multiples(
    user_id=user_id,
    trades=result["data"]["trades"]
    )





    self.update_state(state="PROGRESS", meta={"step": "finalizing_onboarding"})


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
        "message": "Analytics stats saved",
        "summary": {
            "total_trades": normalized["total_trades"],
            "total_pnl": normalized["total_pnl"],
            "win_rate": normalized["win_rate"],
            "profit_factor": normalized["profit_factor"],
            "expectancy": normalized["expectancy"],
        }
    }
