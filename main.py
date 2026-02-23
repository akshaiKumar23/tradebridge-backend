import os
import json
import hmac
import hashlib
from fastapi import FastAPI, HTTPException, Request, Depends
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from celery.result import AsyncResult
from dotenv import load_dotenv
from uuid import uuid4
from datetime import datetime, timedelta
from boto3.dynamodb.conditions import Key
from decimal import Decimal

from auth_dependency import get_current_user, verify_token_only
from schemas.strategies import StrategyCreateRequest
from db.dynamodb import get_performance_snapshots_table
from db.dynamodb import get_analytics_stats_table

from tasks import get_account_summary

from db.dynamodb import get_strategies_table
from db.dynamodb import get_journals_table
from db.dynamodb import get_trades_table
from schemas.journal import JournalCreateRequest
from db.dynamodb import get_onboarding_table
from schemas.broker_link import BrokerLinkRequest
from routers.analytics import router as analytics_router
from routers.trades_router import router as trades_router
from routers.dashboard_router import router as dashboard_router
from routers.reports import router as reports_router

import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
)
logger = logging.getLogger("main")

load_dotenv()

RAZORPAY_KEY_ID = os.getenv("RAZORPAY_KEY_ID")
RAZORPAY_KEY_SECRET = os.getenv("RAZORPAY_KEY_SECRET")
RAZORPAY_WEBHOOK_SECRET = os.getenv("RAZORPAY_WEBHOOK_SECRET")


logger.info(f"Razorpay Key ID loaded: {RAZORPAY_KEY_ID[:8] if RAZORPAY_KEY_ID else 'MISSING'}")
logger.info(f"Razorpay Secret loaded: {'YES' if RAZORPAY_KEY_SECRET else 'MISSING'}")

app = FastAPI(
    title="MT5 Secure Analytics & Journal API",
    description="Secure API for MT5 data processing and trading journals protected by AWS Cognito"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",
        "http://localhost:5174",
        os.getenv("FRONTEND_URL", "http://localhost:5173")
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(analytics_router)
app.include_router(trades_router)
app.include_router(dashboard_router)
app.include_router(reports_router)


# ─── Pydantic Models ────────────────────────────────────────────────────────

class BrokerSelectRequest(BaseModel):
    broker: str

class AccountRequest(BaseModel):
    server: str
    login: int
    password: str

class PaymentVerifyRequest(BaseModel):
    razorpay_order_id: str
    razorpay_payment_id: str
    razorpay_signature: str


# ─── Utilities ──────────────────────────────────────────────────────────────

def decimal_to_float(obj):
    if isinstance(obj, Decimal):
        return float(obj)
    return obj


# ─── Dependencies ────────────────────────────────────────────────────────────

async def require_payment(current_user: dict = Depends(get_current_user)):
    """Dependency that blocks access if user has not paid."""
    table = get_onboarding_table()
    response = table.get_item(Key={"user_id": current_user["user_id"]})
    item = response.get("Item", {})
    if not item.get("has_paid"):
        raise HTTPException(status_code=403, detail="Payment required")
    return current_user


async def get_admin_user(current_user: dict = Depends(get_current_user)):
    claims = current_user["claims"]
    groups = claims.get("cognito:groups", [])
    if "admin" not in groups:
        raise HTTPException(status_code=403, detail="Admin access required")
    return current_user


# ─── Middleware ───────────────────────────────────────────────────────────────

@app.middleware("http")
async def log_body(request: Request, call_next):
    if request.url.path == "/journal":
        body = await request.body()
        print("RAW BODY:", body)

        async def receive():
            return {"type": "http.request", "body": body}

        request = Request(request.scope, receive)

    response = await call_next(request)
    return response


# ─── Health & Auth ────────────────────────────────────────────────────────────

@app.get("/health")
async def health_check():
    return {"status": "healthy"}


@app.get("/me")
async def get_current_user_info(current_user: dict = Depends(get_current_user)):
    return {
        "user_id": current_user["user_id"],
        "email": current_user["email"],
        "username": current_user["username"],
    }


# ─── Payment ──────────────────────────────────────────────────────────────────

@app.post("/payment/create-order")
async def create_razorpay_order(current_user: dict = Depends(get_current_user)):
    import razorpay

    user_id = current_user["user_id"]
    table = get_onboarding_table()

    # Idempotency — already paid
    existing = table.get_item(Key={"user_id": user_id})
    item = existing.get("Item", {})
    if item.get("has_paid"):
        return {"already_paid": True}

    client = razorpay.Client(auth=(RAZORPAY_KEY_ID, RAZORPAY_KEY_SECRET))
    order = client.order.create({
        "amount": 2000,
        "currency": "USD",
        "receipt": user_id,
        "notes": {"user_id": user_id},
    })

    now = datetime.utcnow().isoformat()

    # Store order ID + creation timestamp before checkout starts (replay attack prevention)
    if item:
        table.update_item(
            Key={"user_id": user_id},
            UpdateExpression="SET razorpay_order_id = :oid, order_created_at = :oca, updated_at = :u",
            ExpressionAttributeValues={
                ":oid": order["id"],
                ":oca": now,
                ":u": now,
            },
        )
    else:
        table.put_item(Item={
            "user_id": user_id,
            "razorpay_order_id": order["id"],
            "order_created_at": now,
            "has_paid": False,
            "broker_linked": False,
            "created_at": now,
            "updated_at": now,
        })

    return {
        "order_id": order["id"],
        "amount": order["amount"],
        "currency": order["currency"],
        "already_paid": False,
    }


@app.post("/payment/verify")
async def verify_payment(
    request: PaymentVerifyRequest,
    current_user: dict = Depends(get_current_user),
):
    import razorpay

    user_id = current_user["user_id"]
    table = get_onboarding_table()

    existing = table.get_item(Key={"user_id": user_id})
    item = existing.get("Item", {})

    # Idempotency
    if item.get("has_paid"):
        return {"status": "already_paid"}

    # Replay attack — order ID must match what we stored
    stored_order_id = item.get("razorpay_order_id")
    if not stored_order_id or stored_order_id != request.razorpay_order_id:
        raise HTTPException(status_code=400, detail="Order ID mismatch")

    # Order expiry — reject orders older than 30 minutes
    order_created_at = item.get("order_created_at")
    if order_created_at:
        order_time = datetime.fromisoformat(order_created_at)
        if datetime.utcnow() - order_time > timedelta(minutes=30):
            raise HTTPException(status_code=400, detail="Order expired. Please try again.")

    # Signature verification with constant-time comparison (prevents timing attacks)
    body = f"{request.razorpay_order_id}|{request.razorpay_payment_id}"
    expected = hmac.new(
        RAZORPAY_KEY_SECRET.encode(),
        body.encode(),
        hashlib.sha256,
    ).hexdigest()

    if not hmac.compare_digest(expected, request.razorpay_signature):
        raise HTTPException(status_code=400, detail="Invalid payment signature")

    # Fetch payment from Razorpay and validate amount, currency, and captured status
    client = razorpay.Client(auth=(RAZORPAY_KEY_ID, RAZORPAY_KEY_SECRET))
    payment = client.payment.fetch(request.razorpay_payment_id)

    if payment.get("amount") != 2000:
        raise HTTPException(status_code=400, detail="Incorrect payment amount")

    if payment.get("currency") != "USD":
        raise HTTPException(status_code=400, detail="Invalid currency")

    if payment.get("status") != "captured":
        raise HTTPException(status_code=400, detail="Payment not captured")

    # Mark paid
    now = datetime.utcnow().isoformat()
    table.update_item(
        Key={"user_id": user_id},
        UpdateExpression="""
            SET has_paid = :p,
                payment_id = :pid,
                paid_at = :pa,
                updated_at = :u
        """,
        ExpressionAttributeValues={
            ":p": True,
            ":pid": request.razorpay_payment_id,
            ":pa": now,
            ":u": now,
        },
    )

    return {"status": "success"}


@app.post("/payment/webhook")
async def razorpay_webhook(request: Request):
    body = await request.body()
    signature = request.headers.get("X-Razorpay-Signature", "")

    # Constant-time signature verification
    expected = hmac.new(
        RAZORPAY_WEBHOOK_SECRET.encode(),
        body,
        hashlib.sha256,
    ).hexdigest()

    if not hmac.compare_digest(expected, signature):
        raise HTTPException(status_code=400, detail="Invalid webhook signature")

    payload = json.loads(body)
    event = payload.get("event")

    if event == "payment.captured":
        payment = payload["payload"]["payment"]["entity"]
        user_id = payment.get("notes", {}).get("user_id")

        if not user_id:
            return {"status": "ignored"}

        # Validate amount and currency
        if payment.get("amount") != 2000 or payment.get("currency") != "USD":
            logger.warning(f"Webhook ignored: unexpected amount/currency for user {user_id}")
            return {"status": "ignored"}

        table = get_onboarding_table()

        # Duplicate webhook protection — check before writing
        existing = table.get_item(Key={"user_id": user_id})
        if existing.get("Item", {}).get("has_paid"):
            return {"status": "already_processed"}

        now = datetime.utcnow().isoformat()
        table.update_item(
            Key={"user_id": user_id},
            UpdateExpression="SET has_paid = :p, payment_id = :pid, paid_at = :pa, updated_at = :u",
            ExpressionAttributeValues={
                ":p": True,
                ":pid": payment["id"],
                ":pa": now,
                ":u": now,
            },
        )
        logger.info(f"Webhook: user {user_id} marked as paid via webhook")

    return {"status": "ok"}


# ─── Account ─────────────────────────────────────────────────────────────────

@app.post("/account/summary")
async def request_account_summary(
    request: AccountRequest,
    current_user: dict = Depends(get_current_user),
):
    task = get_account_summary.apply_async(
        args=[current_user["user_id"], request.server, request.login, request.password]
    )
    return {"task_id": task.id, "status": "processing"}


@app.get("/account/summary/{task_id}")
async def get_task_result(
    task_id: str,
    current_user: dict = Depends(get_current_user),
):
    task_result = AsyncResult(task_id, app=get_account_summary.app)

    if task_result.state == "PROGRESS":
        return {"status": "progress", "step": task_result.info.get("step")}

    if task_result.ready():
        result = task_result.get()

        if result.get("status") == "error":
            raise HTTPException(status_code=500, detail=result.get("message"))

        table = get_analytics_stats_table()
        response = table.query(
            KeyConditionExpression=Key("user_id").eq(current_user["user_id"]),
            ScanIndexForward=False,
            Limit=1,
        )
        stats = response.get("Items", [])
        analytics = None

        if stats:
            item = stats[0]
            analytics = {
                "total_pnl": decimal_to_float(item["total_pnl"]),
                "total_trades": decimal_to_float(item["total_trades"]),
                "wins": decimal_to_float(item["wins"]),
                "losses": decimal_to_float(item["losses"]),
                "win_rate": decimal_to_float(item["win_rate"]),
                "profit_factor": decimal_to_float(item["profit_factor"]),
                "expectancy": decimal_to_float(item["expectancy"]),
                "avg_win": decimal_to_float(item["avg_win"]),
                "avg_loss": decimal_to_float(item["avg_loss"]),
            }

        return {"status": "success", "sync": result, "analytics_stats": analytics}

    return {"task_id": task_id, "status": task_result.status, "message": "Task is still in progress."}


@app.post("/account/sync")
async def sync_account(current_user: dict = Depends(require_payment)):
    logger.info("/account/sync called")
    user_id = current_user["user_id"]

    onboarding_table = get_onboarding_table()

    try:
        response = onboarding_table.get_item(Key={"user_id": user_id})
    except Exception as e:
        logger.error("DynamoDB get_item FAILED", exc_info=True)
        raise HTTPException(status_code=500, detail="DB fetch failed")

    item = response.get("Item")
    if not item:
        raise HTTPException(status_code=400, detail="Broker not linked")

    missing_fields = [k for k in ["server", "login", "password"] if not item.get(k)]
    if missing_fields:
        raise HTTPException(status_code=400, detail="Broker credentials missing")

    try:
        task = get_account_summary.apply_async(
            args=[user_id, item["server"], int(item["login"]), str(item["password"])]
        )
        logger.info(f"Celery sync task started: {task.id}")
    except Exception as e:
        logger.error("Celery task creation failed", exc_info=True)
        raise HTTPException(status_code=500, detail="Task dispatch failed")

    return {"status": "processing", "task_id": task.id}


@app.get("/account/sync/new-trades")
async def get_new_trades(current_user: dict = Depends(get_current_user)):
    user_id = current_user["user_id"]
    table = get_trades_table()
    response = table.query(
        KeyConditionExpression=Key("user_id").eq(user_id),
        ScanIndexForward=False,
    )

    new_trades = []
    for item in response.get("Items", []):
        if "unreviewed" in item.get("tags", []):
            new_trades.append({
                "timestamp": decimal_to_float(item["timestamp"]),
                "symbol": item.get("symbol"),
                "pnl": float(item["pnl"]),
                "volume": float(item["volume"]),
            })

    return {"status": "success", "data": new_trades}


# ─── Onboarding ───────────────────────────────────────────────────────────────

@app.get("/onboarding/status")
async def get_onboarding_status(current_user: dict = Depends(get_current_user)):
    table = get_onboarding_table()
    user_id = current_user["user_id"]

    response = table.get_item(Key={"user_id": user_id})

    if "Item" not in response:
        snapshots_table = get_performance_snapshots_table()
        snapshot_response = snapshots_table.query(
            KeyConditionExpression=Key("user_id").eq(user_id),
            Limit=1,
        )
        has_snapshots = len(snapshot_response.get("Items", [])) > 0

        table.put_item(Item={
            "user_id": user_id,
            "broker_linked": has_snapshots,
            "broker_name": None,
            "has_paid": False,
            "created_at": datetime.utcnow().isoformat(),
            "updated_at": datetime.utcnow().isoformat(),
        })

        return {"brokerLinked": has_snapshots, "broker": None, "hasPaid": False}

    item = response["Item"]
    return {
        "brokerLinked": item.get("broker_linked", False),
        "broker": item.get("broker_name"),
        "hasPaid": item.get("has_paid", False),
    }


@app.post("/onboarding/select-broker")
async def select_broker(
    request: BrokerSelectRequest,
    current_user: dict = Depends(require_payment), 
):
    table = get_onboarding_table()
    user_id = current_user["user_id"]

    table.update_item(
        Key={"user_id": user_id},
        UpdateExpression="SET broker_name = :broker, broker_linked = :linked, updated_at = :updated",
        ExpressionAttributeValues={
            ":broker": request.broker,
            ":linked": False,
            ":updated": datetime.utcnow().isoformat(),
        },
    )
    return {"status": "success"}


@app.post("/onboarding/link-broker")
async def link_broker(
    request: BrokerLinkRequest,
    current_user: dict = Depends(require_payment), 
):
    logger.info("/onboarding/link-broker called")
    user_id = current_user["user_id"]
    onboarding_table = get_onboarding_table()

    safe_request = request.dict()
    if "password" in safe_request:
        safe_request["password"] = "***hidden***"
    logger.info(f"Broker link request: {safe_request}")

    try:
        existing_response = onboarding_table.get_item(Key={"user_id": user_id})
        existing = existing_response.get("Item")
    except Exception as e:
        logger.error("DynamoDB get_item failed", exc_info=True)
        raise HTTPException(status_code=500, detail="DB read failed")

    now = datetime.utcnow().isoformat()

    try:
        if existing:
            onboarding_table.update_item(
                Key={"user_id": user_id},
                UpdateExpression="""
                    SET broker_name = :b,
                        server = :s,
                        login = :l,
                        password = :p,
                        broker_linked = :bl,
                        updated_at = :u
                """,
                ExpressionAttributeValues={
                    ":b": request.broker,
                    ":s": request.server,
                    ":l": request.login,
                    ":p": request.password,
                    ":bl": False,
                    ":u": now,
                },
            )
        else:
            onboarding_table.put_item(Item={
                "user_id": user_id,
                "broker_name": request.broker,
                "server": request.server,
                "login": request.login,
                "password": request.password,
                "broker_linked": False,
                "created_at": now,
                "updated_at": now,
            })
    except Exception as e:
        logger.error("DynamoDB write failed", exc_info=True)
        raise HTTPException(status_code=500, detail="DB write failed")

    task = get_account_summary.apply_async(
        args=[user_id, request.server, request.login, request.password]
    )
    logger.info(f"Celery task started: {task.id}")

    return {"status": "syncing", "task_id": task.id}


# ─── Strategies ───────────────────────────────────────────────────────────────

@app.post("/strategies")
async def create_strategy(
    request: StrategyCreateRequest,
    current_user: dict = Depends(get_current_user),
):
    strategies_table = get_strategies_table()
    user_id = current_user["user_id"]
    strategy_id = str(uuid4())

    strategy_item = {
        "user_id": user_id,
        "strategy_id": strategy_id,
        "title": request.title,
        "description": request.description,
        "rules": request.rules,
        "win_rate": request.win_rate,
        "avg_rr": request.avg_rr,
        "trades": request.trades,
        "created_at": datetime.utcnow().isoformat(),
    }

    strategies_table.put_item(Item=strategy_item)
    return {"status": "success", "strategy_id": strategy_id, "strategy": strategy_item}


@app.get("/strategies")
async def get_my_strategies(current_user: dict = Depends(get_current_user)):
    strategies_table = get_strategies_table()
    trades_table = get_trades_table()
    user_id = current_user["user_id"]

    strategies_response = strategies_table.query(
        KeyConditionExpression=Key("user_id").eq(user_id)
    )
    strategies = strategies_response.get("Items", [])

    trades_response = trades_table.query(
        KeyConditionExpression=Key("user_id").eq(user_id)
    )
    all_trades = trades_response.get("Items", [])

    strategy_trades_map = {}
    for trade in all_trades:
        for tag in trade.get("tags", []):
            if tag.startswith("strategy#"):
                sid = tag.replace("strategy#", "")
                if sid not in strategy_trades_map:
                    strategy_trades_map[sid] = []
                strategy_trades_map[sid].append(trade)

    enriched = []
    for strategy in strategies:
        sid = strategy["strategy_id"]
        trades = strategy_trades_map.get(sid, [])
        total = len(trades)
        wins = [t for t in trades if float(t.get("pnl", 0)) > 0]
        losses = [t for t in trades if float(t.get("pnl", 0)) < 0]
        win_rate = round((len(wins) / total) * 100, 2) if total else 0
        avg_rr = round(sum(float(t.get("r_multiple", 0)) for t in trades) / total, 2) if total else 0
        total_pnl = round(sum(float(t.get("pnl", 0)) for t in trades), 2)

        enriched.append({
            **{k: (float(v) if isinstance(v, Decimal) else v) for k, v in strategy.items()},
            "trades": total,
            "wins": len(wins),
            "losses": len(losses),
            "win_rate": win_rate,
            "avg_rr": avg_rr,
            "total_pnl": total_pnl,
        })

    return {"status": "success", "data": enriched}


# ─── Journal ──────────────────────────────────────────────────────────────────

@app.post("/journal")
async def create_journal_entry(
    request: JournalCreateRequest,
    current_user: dict = Depends(get_current_user),
):
    table = get_journals_table()
    user_id = current_user["user_id"]

    item = {
        "user_id": user_id,
        "journal_date": request.date,
        "pnl": Decimal(str(request.pnl)),
        "trades": Decimal(request.trades),
        "session_quality": Decimal(request.session_quality),
        "notes": request.notes,
        "learnings": request.learnings,
        "created_at": datetime.utcnow().isoformat(),
    }

    try:
        table.put_item(
            Item=item,
            ConditionExpression="attribute_not_exists(journal_date)",
        )
        return {"status": "success", "data": item}
    except table.meta.client.exceptions.ConditionalCheckFailedException:
        raise HTTPException(status_code=409, detail="Journal entry already exists for this date")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to create journal entry: {str(e)}")


@app.get("/journal")
async def get_my_journals(current_user: dict = Depends(get_current_user)):
    table = get_journals_table()
    user_id = current_user["user_id"]

    try:
        response = table.query(
            KeyConditionExpression=Key("user_id").eq(user_id),
            ScanIndexForward=False,
        )
        return {"status": "success", "data": response.get("Items", [])}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch journals: {str(e)}")


# ─── Reports ──────────────────────────────────────────────────────────────────

@app.get("/reports/summary")
async def get_reports_summary(current_user: dict = Depends(get_current_user)):
    table = get_performance_snapshots_table()
    user_id = current_user["user_id"]

    response = table.query(
        KeyConditionExpression=Key("user_id").eq(user_id),
        ScanIndexForward=False,
        Limit=1,
    )

    if not response.get("Items"):
        return {}

    snapshot = response["Items"][0]
    symbols = snapshot.get("symbols", {})

    if not symbols:
        return {
            "best_performing_symbol": None,
            "worst_performing_symbol": None,
            "most_active_symbol": None,
            "best_win_rate_symbol": None,
        }

    best = max(symbols.items(), key=lambda x: x[1].get("pnl", 0))
    worst = min(symbols.items(), key=lambda x: x[1].get("pnl", 0))
    active = max(symbols.items(), key=lambda x: x[1].get("trades", 0))
    win_rate = max(
        symbols.items(),
        key=lambda x: (x[1]["wins"] / x[1]["trades"] if x[1].get("trades", 0) > 0 else 0),
    )

    return {
        "best_performing_symbol": {"symbol": best[0], "pnl": float(best[1].get("pnl", 0)), "trades": int(best[1].get("trades", 0))},
        "worst_performing_symbol": {"symbol": worst[0], "pnl": float(worst[1].get("pnl", 0)), "trades": int(worst[1].get("trades", 0))},
        "most_active_symbol": {"symbol": active[0], "trades": int(active[1].get("trades", 0))},
        "best_win_rate_symbol": {
            "symbol": win_rate[0],
            "win_rate": round((win_rate[1]["wins"] / win_rate[1]["trades"]) * 100, 2),
            "trades": int(win_rate[1]["trades"]),
        },
    }


@app.get("/reports/last-sync")
async def get_last_sync(current_user: dict = Depends(get_current_user)):
    table = get_performance_snapshots_table()
    user_id = current_user["user_id"]

    response = table.query(
        KeyConditionExpression=Key("user_id").eq(user_id),
        ScanIndexForward=False,
        Limit=1,
    )

    if not response.get("Items"):
        return {"last_synced_at": None, "needs_sync": True}

    last = response["Items"][0]["created_at"]
    last_dt = datetime.fromisoformat(last)
    needs_sync = (datetime.utcnow() - last_dt).total_seconds() > 86400

    return {"last_synced_at": last, "needs_sync": needs_sync}


# ─── Admin ────────────────────────────────────────────────────────────────────

@app.get("/admin/users")
async def list_all_users(admin_user: dict = Depends(get_admin_user)):
    return {"message": "Admin access granted", "admin": admin_user["username"]}