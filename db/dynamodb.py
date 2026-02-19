import boto3
import os

def get_dynamodb():
    return boto3.resource(
        "dynamodb",
        region_name=os.getenv("AWS_REGION", "ap-south-1"),
    )

def get_strategies_table():
    dynamodb = get_dynamodb()
    return dynamodb.Table("UserStrategies")


def get_journals_table():
    dynamodb = get_dynamodb()
    return dynamodb.Table("DailyJournals")

def get_performance_snapshots_table():
    dynamodb = get_dynamodb()
    return dynamodb.Table("UserPerformanceSnapshots")

def get_onboarding_table():
    dynamodb = get_dynamodb()
    return dynamodb.Table("UserOnboarding")

def get_analytics_stats_table():
    dynamodb = get_dynamodb()
    return dynamodb.Table("UserAnalyticsStats")

def get_equity_curve_table():
    dynamodb = get_dynamodb()
    return dynamodb.Table("UserEquityCurve")

def get_pnl_weekly_table():
    dynamodb = get_dynamodb()
    return dynamodb.Table("UserPnLWeekly")

def get_r_multiple_table():
    dynamodb = get_dynamodb()
    return dynamodb.Table("UserRMultiples")

def get_trades_table():
    dynamodb = get_dynamodb()
    return dynamodb.Table("UserTrades")


def get_daily_pnl_table():
    dynamodb = get_dynamodb()
    return dynamodb.Table("UserDailyPnL")

def get_dashboard_stats_table():
    dynamodb = get_dynamodb()
    return dynamodb.Table("UserDashboardStats")


def get_report_stats_table():
    dynamodb = get_dynamodb()
    return dynamodb.Table("UserReportStats")


def get_report_symbol_summary_table():
    dynamodb = get_dynamodb()
    return dynamodb.Table("UserReportSymbolSummary")


def get_report_win_rate_table():
    dynamodb = get_dynamodb()
    return dynamodb.Table("UserReportWinRate")

def get_report_overview_table():
    dynamodb = get_dynamodb()
    return dynamodb.Table("UserReportOverview")

def get_drawdown_curve_table():
    dynamodb = get_dynamodb()
    return dynamodb.Table("UserDrawdownCurve")

def get_session_performance_table():
    dynamodb = get_dynamodb()
    return dynamodb.Table("UserSessionPerformance")

def get_dashboard_session_performance_table():
    dynamodb = get_dynamodb()
    return dynamodb.Table("UserDashboardSessionPerformance")

def get_dashboard_symbol_performance_table():
    dynamodb = get_dynamodb()
    return dynamodb.Table("UserDashboardSymbolPerformance")

def get_dashboard_daily_pnl_table():
    dynamodb = get_dynamodb()
    return dynamodb.Table("UserDashboardDailyPnL")

def get_dashboard_equity_curve_table():
    dynamodb = get_dynamodb()
    return dynamodb.Table("UserDashboardEquityCurve")
