import boto3
import os
from contextlib import contextmanager


from botocore.config import Config

boto_config = Config(
    # A sync fans out to ~18 store functions across two thread pools, and the
    # heavy ones now split their batch writes across threads of their own. Too
    # small a pool here shows up as the sync hanging, not as an error.
    max_pool_connections=100,
    # Two attempts left almost no headroom for the throttling a large sync
    # provokes when it rewrites tens of thousands of rows in a burst.
    retries={"max_attempts": 5, "mode": "standard"},
)

_dynamodb = boto3.resource(
    "dynamodb",
    region_name=os.getenv("AWS_REGION", "ap-south-1"),
    config=boto_config,
)


class _NoDeleteBatchWriter:
    """Batch writer that accepts puts but refuses deletes."""

    def __init__(self, batch, table_name):
        self._batch = batch
        self._table_name = table_name

    def put_item(self, **kwargs):
        return self._batch.put_item(**kwargs)

    def delete_item(self, **kwargs):
        raise RuntimeError(
            f"Refusing to batch-delete from {self._table_name}: this table is "
            f"delete-protected."
        )


class _NoDeleteTable:
    """A DynamoDB Table that cannot delete rows.

    UserOnboarding carries payment state (has_paid), broker credentials and
    onboarding flags that exist nowhere else and cannot be regenerated from MT5.
    Nothing in the sync path has any business removing a row from it, so make
    that impossible at the access point rather than relying on review. Reads and
    updates pass straight through.
    """

    def __init__(self, table):
        self._table = table

    def __getattr__(self, name):
        # Only reached for attributes this wrapper does not define itself.
        return getattr(object.__getattribute__(self, "_table"), name)

    def delete_item(self, **kwargs):
        raise RuntimeError(
            f"Refusing to delete from {self._table.name}: this table is "
            f"delete-protected. Use update_item to clear individual fields."
        )

    @contextmanager
    def batch_writer(self, **kwargs):
        with self._table.batch_writer(**kwargs) as batch:
            yield _NoDeleteBatchWriter(batch, self._table.name)


def get_strategies_table():
    return _dynamodb.Table("UserStrategies")

def get_journals_table():
    return _dynamodb.Table("DailyJournals")

def get_performance_snapshots_table():
    return _dynamodb.Table("UserPerformanceSnapshots")

def get_onboarding_table():
    # Delete-protected: see _NoDeleteTable above.
    return _NoDeleteTable(_dynamodb.Table("UserOnboarding"))

def get_analytics_stats_table():
    return _dynamodb.Table("UserAnalyticsStats")

def get_equity_curve_table():
    return _dynamodb.Table("UserEquityCurve")

def get_pnl_weekly_table():
    return _dynamodb.Table("UserPnLWeekly")

def get_r_multiple_table():
    return _dynamodb.Table("UserRMultiples")

def get_trades_table():
    return _dynamodb.Table("UserTrades")

def get_daily_pnl_table():
    return _dynamodb.Table("UserDailyPnL")

def get_dashboard_stats_table():
    return _dynamodb.Table("UserDashboardStats")

def get_report_stats_table():
    return _dynamodb.Table("UserReportStats")

def get_report_symbol_summary_table():
    return _dynamodb.Table("UserReportSymbolSummary")

def get_report_win_rate_table():
    return _dynamodb.Table("UserReportWinRate")

def get_report_overview_table():
    return _dynamodb.Table("UserReportOverview")

def get_drawdown_curve_table():
    return _dynamodb.Table("UserDrawdownCurve")

def get_session_performance_table():
    return _dynamodb.Table("UserSessionPerformance")

def get_dashboard_session_performance_table():
    return _dynamodb.Table("UserDashboardSessionPerformance")

def get_dashboard_symbol_performance_table():
    return _dynamodb.Table("UserDashboardSymbolPerformance")

def get_dashboard_daily_pnl_table():
    return _dynamodb.Table("UserDashboardDailyPnL")

def get_dashboard_equity_curve_table():
    return _dynamodb.Table("UserDashboardEquityCurve")

def get_atlas_stats_table():
    return _dynamodb.Table("UserAtlasStats")

def get_atlas_prompts_table():
    return _dynamodb.Table("AtlasPrompts")

def get_server_names_table():
    return _dynamodb.Table("ServerNames")