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
from services.trades_store import save_user_trades
from services.daily_pnl_store import save_daily_pnl
from services.dashboard_stats_store import save_dashboard_stats


@celery_app.task(name="tasks.get_account_summary", bind=True)
def get_account_summary(self, user_id, server, login, password):
    
    print(f"\n{'='*60}")
    print(f"STARTING TASK FOR USER: {user_id}")
    print(f"{'='*60}\n")

  
    print("STEP 1: Connecting to MT5...")
    self.update_state(state="PROGRESS", meta={"step": "connecting_to_mt5"})

    result = fetch_mt5_analytics(server, login, password)
    if result["status"] == "success":
        debug = result["data"].get("debug_info", {})
        print(f"✓ MT5 connected successfully")
        print(f"  Debug Info: {debug}")
    else:
        print(f"✗ MT5 connection failed: {result}")
        return result

    
    print("\nSTEP 2: Normalizing data...")
    self.update_state(state="PROGRESS", meta={"step": "normalizing_data"})
    
    normalized = normalize_mt5_data(result["data"])
    print(f"✓ Data normalized")
    print(f"  Total trades: {normalized.get('total_trades')}")
    print(f"  Total PnL: {normalized.get('total_pnl')}")

    snapshot_date = datetime.utcnow().date().isoformat()
    print(f"  Snapshot date: {snapshot_date}")

    
    print("\nSTEP 3: Saving analytics stats...")
    self.update_state(state="PROGRESS", meta={"step": "saving_analytics_stats"})
    
    try:
        save_user_analytics_stats(
            user_id=user_id,
            snapshot_date=snapshot_date,
            analytics=normalized
        )
        print(f"Analytics stats saved")
    except Exception as e:
        print(f"ERROR in save_user_analytics_stats: {e}")
        raise

    
    print("\nSTEP 4: Saving performance snapshot...")
    self.update_state(state="PROGRESS", meta={"step": "saving_performance_snapshot"})
    
    try:
        save_user_performance_snapshot(
            user_id=user_id,
            snapshot_date=snapshot_date,
            data=normalized
        )
        print(f"Performance snapshot saved")
    except Exception as e:
        print(f"ERROR in save_user_performance_snapshot: {e}")
        raise

    
    print("\nSTEP 5: Saving equity curve...")
    self.update_state(state="PROGRESS", meta={"step": "saving_equity_curve"})
    
    try:
        save_equity_curve(
            user_id=user_id,
            equity_curve=result["data"]["equity_vs_time"]
        )
        print(f"Equity curve saved")
    except Exception as e:
        print(f"ERROR in save_equity_curve: {e}")
        raise

    print("\nSTEP 6: Saving weekly PnL...")
    self.update_state(state="PROGRESS", meta={"step": "saving_weekly_pnl"})
    
    weekly_pnl = normalized.get("weekly_pnl", {})
    if weekly_pnl:
        try:
            save_weekly_pnl(
                user_id=user_id,
                weekly_pnl=weekly_pnl
            )
            print(f"Weekly PnL saved ({len(weekly_pnl)} weeks)")
        except Exception as e:
            print(f"ERROR in save_weekly_pnl: {e}")
            raise
    else:
        print(f"  (No weekly PnL data to save)")

    
    print("\nSTEP 7: Saving trades...")
    self.update_state(state="PROGRESS", meta={"step": "saving_trades"})
    
    try:
        save_user_trades(
            user_id=user_id,
            trades=result["data"]["trades"]
        )
        print(f"Trades saved ({len(result['data']['trades'])} trades)")
    except Exception as e:
        print(f"ERROR in save_user_trades: {e}")
        raise

    
    print("\nSTEP 8: Saving R multiples...")
    self.update_state(state="PROGRESS", meta={"step": "saving_r_multiples"})
    
    try:
        save_r_multiples(
            user_id=user_id,
            trades=result["data"]["trades"]
        )
        print(f"R multiples saved")
    except Exception as e:
        print(f"ERROR in save_r_multiples: {e}")
        raise

    
    print("\nSTEP 9: Saving daily PnL...")
    self.update_state(state="PROGRESS", meta={"step": "saving_daily_pnl"})
    
    try:
        save_daily_pnl(
            user_id=user_id,
            trades=result["data"]["trades"]
        )
        print(f"Daily PnL saved")
    except Exception as e:
        print(f"ERROR in save_daily_pnl: {e}")
        raise

    
    print("\nSTEP 10: Saving dashboard stats...")
    print(f"  About to call save_dashboard_stats with:")
    print(f"    user_id: {user_id}")
    print(f"    snapshot_date: {snapshot_date}")
    print(f"    analytics keys: {list(normalized.keys())}")
    
    self.update_state(state="PROGRESS", meta={"step": "saving_dashboard_stats"})
    
    try:
        save_dashboard_stats(
            user_id=user_id,
            snapshot_date=snapshot_date,
            analytics=normalized
        )
        print(f"Dashboard stats saved")
    except Exception as e:
        print(f"ERROR in save_dashboard_stats: {e}")
        import traceback
        print(f"  Traceback:\n{traceback.format_exc()}")
        raise

    
    print("\nSTEP 11: Finalizing onboarding...")
    self.update_state(state="PROGRESS", meta={"step": "finalizing_onboarding"})

    try:
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
        print(f"Onboarding finalized")
    except Exception as e:
        print(f"ERROR in finalizing onboarding: {e}")
        raise

    print(f"\n{'='*60}")
    print(f"TASK COMPLETED SUCCESSFULLY")
    print(f"{'='*60}\n")

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