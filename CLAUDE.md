# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

The FastAPI backend for JournalPips (journalpips.com): syncs a user's MT5 (MetaTrader5) trading account, turns raw broker deal history into analytics/dashboards/reports, and generates AI trading insights ("Atlas"). Auth is AWS Cognito, storage is DynamoDB only (no SQL), async work runs on Celery/Redis, payments go through Razorpay.

## Commands

There is no build step, linter config, or test suite in this repo (no `tests/`, no pytest config).

Install deps:
```
pip install -r requirements.txt
```

Run the API (needs a populated `.env` — see below):
```
uvicorn main:app --reload
```

Run the Celery worker (needed for `/account/sync`, `/onboarding/link-broker`, and anything that kicks off `get_account_summary`; requires Redis reachable at `REDIS_URL`):
```
celery -A celery_app.celery_app worker --loglevel=info
```

Required environment variables (`.env`, loaded via `python-dotenv`): `AWS_REGION`, `COGNITO_USER_POOL_ID`, `COGNITO_APP_CLIENT_ID`, `FRONTEND_URL`, `MT5_SERVER`/`MT5_LOGIN`/`MT5_PASSWORD`, `RAZORPAY_KEY_ID`/`RAZORPAY_KEY_SECRET`/`RAZORPAY_WEBHOOK_SECRET`, `OPENAI_API_KEY`, `WINPROFX_API_KEY`, `REDIS_URL`.

Note: `MetaTrader5` (the `mt5` package used in `mt5_logic.py`) is Windows-only and requires a running MT5 terminal — the sync path cannot be exercised on non-Windows dev machines.

## Architecture

**Request flow**: `main.py` wires up CORS, Cognito auth dependencies, and mounts feature routers (`routers/analytics.py`, `trades_router.py`, `dashboard_router.py`, `reports.py`, `atlas_router.py`, `partners_router.py`). Onboarding, payments, journal, and strategy endpoints live directly in `main.py` rather than a router.

**Auth**: `auth.py` (`CognitoTokenVerifier`) fetches and caches Cognito JWKS (1hr TTL, auto-retries once on key-rotation miss) and verifies JWT signature/issuer/audience/expiry by hand via `python-jose`. `auth_dependency.py` wraps this as FastAPI dependencies: `get_current_user` (verifies token, backfills email from Cognito via `admin_get_user` if the access token doesn't carry it, cached in-memory) and `verify_token_only`. Almost every route depends on `get_current_user`; payment-gated routes depend on `require_payment` (checks `has_paid` in the onboarding table) instead. `auth_middleware.py` is a separate, unused `BaseHTTPMiddleware` implementation of the same verification — it is not registered on the `app` in `main.py`; don't assume it's active.

**Storage**: `db/dynamodb.py` is the single source of truth for table access — one `get_..._table()` function per DynamoDB table, all sharing one boto3 resource/connection pool. There is no ORM; every router/service calls these directly with `boto3.dynamodb.conditions.Key`. Tables are partitioned by `user_id` (sometimes with a `timestamp` sort key), and there is roughly one table per feature slice: raw domain tables (`UserTrades`, `DailyJournals`, `UserStrategies`, `UserOnboarding`) plus a long tail of precomputed/denormalized tables that exist purely so each page can do a single cheap query instead of aggregating on read (`UserAnalyticsStats`, `UserDashboardStats`, `UserDashboardSessionPerformance`, `UserDashboardSymbolPerformance`, `UserDashboardDailyPnL`, `UserDashboardEquityCurve`, `UserReportStats`, `UserReportSymbolSummary`, `UserReportWinRate`, `UserReportOverview`, `UserEquityCurve`, `UserDrawdownCurve`, `UserPnLWeekly`, `UserRMultiples`, `UserSessionPerformance`, `UserAtlasStats`). `database.py` (a separate `Users` table) is legacy/unused elsewhere — don't extend it.

**MT5 sync pipeline** (the core data pipeline): `POST /account/sync` or `/onboarding/link-broker` dispatches the Celery task `get_account_summary` (`tasks.py`, broker/backend in `celery_app.py`). That task:
1. Calls `mt5_logic.fetch_mt5_analytics()`, which logs into the broker via the `MetaTrader5` package, pulls `history_deals_get`, groups deals by `position_id` into closed trades, and computes per-trade PnL/R-multiple/equity curve.
2. Passes the result through `services/mt5_normalizer.normalize_mt5_data()` to compute aggregate stats (win rate, profit factor, expectancy, revenge-trading count, per-symbol/weekly breakdowns, etc).
3. Fans out to ~18 `services/*_store.py` functions in two parallel `ThreadPoolExecutor` batches, each writing one of the precomputed tables above — this is why adding a new derived stat usually means adding a new store module and wiring it into both this task and the corresponding read-side router.
4. Marks onboarding `broker_linked = True`, then (non-fatally) refreshes Atlas AI insights via `TradingDataCompressor.get_or_update_atlas_stats`.

Poll progress via `GET /account/summary/{task_id}` (Celery `AsyncResult`, with `PROGRESS` state steps set by `self.update_state` in the task).

**Atlas AI insights**: `services/trading_data_compressor.py` builds a compressed JSON payload from the precomputed tables (never raw trades), hashes it, and only calls OpenAI (`gpt-4o-mini`, forced JSON response) when the hash changed *and* a metric moved past a meaningful threshold (`WIN_RATE_DELTA`, `PNL_DELTA`, `PF_DELTA`, `TRADE_COUNT_DELTA` in that file) — this is a deliberate cost-control cache, not incidental. Equity/drawdown/weekly curves are excluded from the cache-invalidation hash on purpose since they drift on every sync even with zero new trades. `routers/atlas_router.py` reads the cached response, strips markdown code fences if present, and validates it has `insights`/`summary` keys, wiping the cache and returning a 500 on malformed/missing data so the next request regenerates it.

**Payments**: Razorpay order-create/verify/webhook handlers live in `main.py`. Verification checks stored order-ID match, a 30-minute order expiry, HMAC signature (`hmac.compare_digest`), and re-fetches the payment from Razorpay to confirm amount/currency/captured status before marking `has_paid`. The webhook handler independently re-validates amount/currency and is idempotent (checks `has_paid` before writing). `AMOUNT_PAISE`/`CURRENCY` in `main.py` are the single source of truth for the charge amount.

**Partner activation** (`routers/partners_router.py`): a WinProFX-specific endpoint gated by a static `X-Api-Key` header (`WINPROFX_API_KEY`), looked up directly by `user_id` (despite the field being named `email` in `WinproActivateRequest`) to mark a user as paid outside the Razorpay flow. The file has a large dead first-draft implementation left commented out above the live code.
