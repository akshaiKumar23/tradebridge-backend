import boto3
import os


_dynamodb = boto3.resource(
    "dynamodb",
    region_name=os.getenv("AWS_REGION", "ap-south-1"),
)


def get_strategies_table():
    return _dynamodb.Table("UserStrategies")

def get_journals_table():
    return _dynamodb.Table("DailyJournals")

def get_performance_snapshots_table():
    return _dynamodb.Table("UserPerformanceSnapshots")

def get_onboarding_table():
    return _dynamodb.Table("UserOnboarding")

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