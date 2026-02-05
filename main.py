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


from tasks import get_account_summary

from db.dynamodb import get_strategies_table
from db.dynamodb import get_journals_table
from schemas.journal import JournalCreateRequest
from boto3.dynamodb.conditions import Key


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
        args=[request.server, request.login, request.password]
    )
    return {
        "task_id": task.id,
        "status": "processing",
        "message": "MT5 analytics task submitted.",
        "user": current_user['username']
    }


@app.get("/account/summary/{task_id}")
async def get_task_result(
    task_id: str,
    current_user: dict = Depends(get_current_user)
):
    
    task_result = AsyncResult(task_id, app=get_account_summary.app)

    if task_result.ready():
        result = task_result.get()
        if result.get("status") == "error":
            raise HTTPException(status_code=500, detail=result.get("message"))
        return result

    return {
        "task_id": task_id,
        "status": task_result.status,
        "message": "Task is still in progress."
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
            ScanIndexForward=False  # latest first
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