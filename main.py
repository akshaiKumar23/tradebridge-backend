import os
from fastapi import FastAPI, HTTPException, Request, Depends
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from celery.result import AsyncResult
from dotenv import load_dotenv
from uuid import uuid4
from datetime import datetime
from boto3.dynamodb.conditions import Key
from fastapi import Request
from decimal import Decimal

from auth_dependency import get_current_user, verify_token_only
from schemas.strategies import StrategyCreateRequest
from db.dynamodb import get_performance_snapshots_table
from db.dynamodb import get_analytics_stats_table

from tasks import get_account_summary

from db.dynamodb import get_strategies_table
from db.dynamodb import get_journals_table
from schemas.journal import JournalCreateRequest
from boto3.dynamodb.conditions import Key
from db.dynamodb import get_onboarding_table
from schemas.broker_link import BrokerLinkRequest
from routers.analytics import router as analytics_router
from routers.trades_router import router as trades_router
from routers.dashboard_router import router as dashboard_router






load_dotenv()

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

class BrokerSelectRequest(BaseModel):
    broker: str

@app.middleware("http")
async def log_body(request: Request, call_next):
    if request.url.path == "/journal":
        body = await request.body()
        print("RAW BODY:", body)
    return await call_next(request)

class AccountRequest(BaseModel):
    server: str
    login: int
    password: str


def decimal_to_float(obj):
    if isinstance(obj, Decimal):
        return float(obj)
    return obj


@app.get("/health")
async def health_check():
   
    return {"status": "healthy"}



@app.get("/me")
async def get_current_user_info(current_user: dict = Depends(get_current_user)):

    return {
        "user_id": current_user['user_id'],
        "email": current_user['email'],
        "username": current_user['username']
    }



@app.post("/account/summary")
async def request_account_summary(
    request: AccountRequest,
    current_user: dict = Depends(get_current_user)
):
    task = get_account_summary.apply_async(
        args=[
            current_user["user_id"],
            request.server,
            request.login,
            request.password
        ]
    )

    return {
        "task_id": task.id,
        "status": "processing"
    }



@app.get("/account/summary/{task_id}")
async def get_task_result(
    task_id: str,
    current_user: dict = Depends(get_current_user)
):

    task_result = AsyncResult(task_id, app=get_account_summary.app)

    if task_result.state == "PROGRESS":
        return {
            "status": "progress",
            "step": task_result.info.get("step")
        }

    if task_result.ready():

        result = task_result.get()

        if result.get("status") == "error":
            raise HTTPException(
                status_code=500,
                detail=result.get("message")
            )

        table = get_analytics_stats_table()

        response = table.query(
            KeyConditionExpression=Key("user_id").eq(current_user["user_id"]),
            ScanIndexForward=False,
            Limit=1
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

        return {
            "status": "success",
            "sync": result,
            "analytics_stats": analytics
        }

    return {
        "task_id": task_id,
        "status": task_result.status,
        "message": "Task is still in progress."
    }

@app.get("/reports/summary")
async def get_reports_summary(
    current_user: dict = Depends(get_current_user)
):
    table = get_performance_snapshots_table()
    user_id = current_user["user_id"]

    response = table.query(
        KeyConditionExpression=Key("user_id").eq(user_id),
        ScanIndexForward=False,
        Limit=1
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
        key=lambda x: (
            x[1]["wins"] / x[1]["trades"]
            if x[1].get("trades", 0) > 0 else 0
        )
    )

    return {
        "best_performing_symbol": {
            "symbol": best[0],
            "pnl": float(best[1].get("pnl", 0)),
            "trades": int(best[1].get("trades", 0)),
        },
        "worst_performing_symbol": {
            "symbol": worst[0],
            "pnl": float(worst[1].get("pnl", 0)),
            "trades": int(worst[1].get("trades", 0)),
        },
        "most_active_symbol": {
            "symbol": active[0],
            "trades": int(active[1].get("trades", 0)),
        },
        "best_win_rate_symbol": {
            "symbol": win_rate[0],
            "win_rate": round(
                (win_rate[1]["wins"] / win_rate[1]["trades"]) * 100, 2
            ),
            "trades": int(win_rate[1]["trades"]),
        },
    }








async def get_admin_user(current_user: dict = Depends(get_current_user)):
 
    claims = current_user['claims']
    groups = claims.get('cognito:groups', [])

    if 'admin' not in groups:
        raise HTTPException(
            status_code=403,
            detail="Admin access required"
        )
    return current_user


@app.get("/admin/users")
async def list_all_users(admin_user: dict = Depends(get_admin_user)):
    return {"message": "Admin access granted", "admin": admin_user['username']}


@app.post("/strategies")
async def create_strategy(
    request: StrategyCreateRequest,
    current_user: dict = Depends(get_current_user)
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

    return {
        "status": "success",
        "strategy_id": strategy_id,
        "strategy": strategy_item
    }


@app.get("/strategies")
async def get_my_strategies(
    current_user: dict = Depends(get_current_user)
):
    strategies_table = get_strategies_table()
    user_id = current_user["user_id"]

    response = strategies_table.query(
        KeyConditionExpression=Key("user_id").eq(user_id)
    )

    return {
        "status": "success",
        "data": response.get("Items", [])
    }


@app.post("/journal")
async def create_journal_entry(
    request: JournalCreateRequest,
    current_user: dict = Depends(get_current_user)
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
            ConditionExpression="attribute_not_exists(journal_date)"
        )
        return {"status": "success", "data": item}

    except table.meta.client.exceptions.ConditionalCheckFailedException:
        raise HTTPException(
            status_code=409,
            detail="Journal entry already exists for this date"
        )

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to create journal entry: {str(e)}"
        )

@app.get("/journal")
async def get_my_journals(
    current_user: dict = Depends(get_current_user)
):
    table = get_journals_table()
    user_id = current_user["user_id"]

    try:
        response = table.query(
            KeyConditionExpression=Key("user_id").eq(user_id),
            ScanIndexForward=False
        )

        return {
            "status": "success",
            "data": response.get("Items", [])
        }

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch journals: {str(e)}"
        )


@app.get("/reports/last-sync")
async def get_last_sync(current_user: dict = Depends(get_current_user)):
    table = get_performance_snapshots_table()
    user_id = current_user["user_id"]

    response = table.query(
        KeyConditionExpression=Key("user_id").eq(user_id),
        ScanIndexForward=False,
        Limit=1
    )

    if not response.get("Items"):
        return {
            "last_synced_at": None,
            "needs_sync": True
        }

    last = response["Items"][0]["created_at"]
    last_dt = datetime.fromisoformat(last)

    needs_sync = (datetime.utcnow() - last_dt).total_seconds() > 86400

    return {
        "last_synced_at": last,
        "needs_sync": needs_sync
    }





@app.get("/onboarding/status")
async def get_onboarding_status(
    current_user: dict = Depends(get_current_user)
):
    table = get_onboarding_table()
    user_id = current_user["user_id"]

    response = table.get_item(
        Key={"user_id": user_id}
    )

    if "Item" not in response:
        snapshots_table = get_performance_snapshots_table()
        snapshot_response = snapshots_table.query(
            KeyConditionExpression=Key("user_id").eq(user_id),
            Limit=1
        )
        
        has_snapshots = len(snapshot_response.get("Items", [])) > 0
        
      
        table.put_item(
            Item={
                "user_id": user_id,
                "broker_linked": has_snapshots, 
                "broker_name": None,
                "created_at": datetime.utcnow().isoformat(),
                "updated_at": datetime.utcnow().isoformat(),
            }
        )

        return {
            "brokerLinked": has_snapshots,
            "broker": None,
        }

    item = response["Item"]

    return {
        "brokerLinked": item.get("broker_linked", False),
        "broker": item.get("broker_name"),
    }

@app.post("/onboarding/select-broker")
async def select_broker(
    request: BrokerSelectRequest,
    current_user: dict = Depends(get_current_user)
):
    table = get_onboarding_table()
    user_id = current_user["user_id"]

    table.update_item(
        Key={"user_id": user_id},
        UpdateExpression="""
            SET broker_name = :broker,
                broker_linked = :linked,
                updated_at = :updated
        """,
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
    current_user: dict = Depends(get_current_user)
):
    user_id = current_user["user_id"]

    task = get_account_summary.apply_async(
        args=[
            user_id,
            request.server,
            request.login,
            request.password
        ]
    )

    onboarding_table = get_onboarding_table()
    
   
    onboarding_table.put_item(
        Item={
            "user_id": user_id,
            "broker_name": request.broker,
            "broker_linked": False,
            "sync_task_id": task.id,
            "created_at": datetime.utcnow().isoformat(),
            "updated_at": datetime.utcnow().isoformat(),
        }
    )

    return {
        "status": "syncing",
        "task_id": task.id
    }
