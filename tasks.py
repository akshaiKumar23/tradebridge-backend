from celery_app import celery_app
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
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
from services.reports_stats_store import save_user_report_stats
from services.reports_symbol_summary_store import save_user_report_symbol_summary
from services.reports_win_rate_store import save_user_report_win_rate
from services.reports_overview_store import save_user_report_overview
from services.drawdown_store import save_drawdown_curve
from services.session_performance_store import save_session_performance
from services.dashboard_session_performance_store import save_dashboard_session_performance
from services.dashboard_symbol_performance_store import save_dashboard_symbol_performance
from services.dashboard_daily_pnl_store import save_dashboard_daily_pnl
from services.dashboard_equity_curve_store import save_dashboard_equity_curve


@celery_app.task(name="tasks.get_account_summary", bind=True)
def get_account_summary(self, user_id, server, login, password):

    print(f"\n{'='*60}")
    print(f"STARTING TASK FOR USER: {user_id}")
    print(f"{'='*60}\n")

    # ---------------- STEP 1: Connect to MT5 ----------------
    print("STEP 1: Connecting to MT5...")
    self.update_state(state="PROGRESS", meta={"step": "connecting_to_mt5"})
    print(f"Connecting with server={server}, login={login}, password=***")

    result = fetch_mt5_analytics(server, login, password)
    if result["status"] == "success":
        print(f"✓ MT5 connected successfully")
    else:
        print(f"✗ MT5 connection failed: {result}")
        return result

    # ---------------- STEP 2: Normalize Data ----------------
    print("\nSTEP 2: Normalizing data...")
    self.update_state(state="PROGRESS", meta={"step": "normalizing_data"})

    normalized = normalize_mt5_data(result["data"])
    print(f"✓ Data normalized")
    print(f"  Total trades: {normalized.get('total_trades')}")
    print(f"  Total PnL: {normalized.get('total_pnl')}")

    snapshot_date = datetime.utcnow().date().isoformat()
    print(f"  Snapshot date: {snapshot_date}")

    trades = result["data"]["trades"]
    equity_curve = result["data"]["equity_vs_time"]
    weekly_pnl = normalized.get("weekly_pnl", {})

    # ---------------- STEP 3: Save All Data in Parallel (Batch 1) ----------------
    print("\nSTEP 3-10: Saving all data in parallel (batch 1)...")
    self.update_state(state="PROGRESS", meta={"step": "saving_snapshot"})

    with ThreadPoolExecutor(max_workers=6) as executor:
        futures = {
            executor.submit(save_user_analytics_stats, user_id, snapshot_date, normalized): "analytics",
            executor.submit(save_user_performance_snapshot, user_id, snapshot_date, normalized): "performance",
            executor.submit(save_equity_curve, user_id, equity_curve): "equity",
            executor.submit(save_user_trades, user_id, trades): "trades",
            executor.submit(save_daily_pnl, user_id, trades): "daily_pnl",
            executor.submit(save_dashboard_stats, user_id, snapshot_date, normalized): "dashboard_stats",
        }
        for future in as_completed(futures):
            name = futures[future]
            try:
                future.result()
                print(f"  ✓ {name} saved")
            except Exception as e:
                print(f"  ✗ {name} failed: {e}")
                raise

    # ---------------- STEP 4: Save Remaining Data in Parallel (Batch 2) ----------------
    print("\nSaving remaining data in parallel (batch 2)...")
    self.update_state(state="PROGRESS", meta={"step": "finalizing_onboarding"})

    def save_weekly_pnl_if_exists(user_id, weekly_pnl):
        if weekly_pnl:
            save_weekly_pnl(user_id=user_id, weekly_pnl=weekly_pnl)
            print(f"  ✓ weekly_pnl saved ({len(weekly_pnl)} weeks)")
        else:
            print(f"  (No weekly PnL data to save)")

    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = {
            executor.submit(save_weekly_pnl_if_exists, user_id, weekly_pnl): "weekly_pnl",
            executor.submit(save_r_multiples, user_id, trades): "r_multiples",
            executor.submit(save_user_report_stats, user_id, snapshot_date, normalized): "report_stats",
            executor.submit(save_user_report_symbol_summary, user_id, snapshot_date, normalized): "report_symbol",
            executor.submit(save_user_report_win_rate, user_id, trades): "report_win_rate",
            executor.submit(save_user_report_overview, user_id, trades): "report_overview",
            executor.submit(save_drawdown_curve, user_id, equity_curve): "drawdown",
            executor.submit(save_session_performance, user_id, trades): "session",
            executor.submit(save_dashboard_session_performance, user_id, trades): "dash_session",
            executor.submit(save_dashboard_symbol_performance, user_id, trades): "dash_symbol",
            executor.submit(save_dashboard_daily_pnl, user_id, trades): "dash_daily_pnl",
            executor.submit(save_dashboard_equity_curve, user_id, equity_curve): "dash_equity",
        }
        for future in as_completed(futures):
            name = futures[future]
            try:
                future.result()
                print(f"  ✓ {name} saved")
            except Exception as e:
                print(f"  ✗ {name} failed: {e}")
                raise

    # ---------------- STEP 5: Finalize Onboarding ----------------
    print("\nFinalizing onboarding...")
    try:
        onboarding_table = get_onboarding_table()
        onboarding_table.update_item(
            Key={"user_id": user_id},
            UpdateExpression="SET broker_linked = :bl, updated_at = :u, last_sync_at = :ls",
            ExpressionAttributeValues={
                ":bl": True,
                ":u": datetime.utcnow().isoformat(),
                ":ls": int(datetime.utcnow().timestamp())
            }
        )
        print("✓ Onboarding finalized")
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