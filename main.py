from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from celery.result import AsyncResult
from tasks import get_account_summary
import os
from dotenv import load_dotenv

load_dotenv()

app = FastAPI(title="MT5 Account API with Celery")


class AccountRequest(BaseModel):
    server: str
    login: int
    password: str


@app.post("/account/summary")
def request_account_summary(request: AccountRequest):

    task = get_account_summary.apply_async(
        args=[request.server, request.login, request.password]
    )
    
    return {
        "task_id": task.id,
        "status": "processing",
        "message": "Task submitted. Use /account/summary/{task_id} to check status"
    }


@app.get("/account/summary/{task_id}")
def get_task_result(task_id: str):

    task_result = AsyncResult(task_id, app=get_account_summary.app)
    
    if task_result.ready():
        result = task_result.get()
        
        if result.get("status") == "error":
            raise HTTPException(status_code=500, detail=result.get("message"))
        
        return result
    
    elif task_result.failed():
        raise HTTPException(
            status_code=500,
            detail=f"Task failed: {str(task_result.info)}"
        )
    
    else:
        return {
            "task_id": task_id,
            "status": "processing",
            "message": "Task is still being processed"
        }


@app.get("/account/summary/{task_id}/status")
def get_task_status(task_id: str):
 
    task_result = AsyncResult(task_id, app=get_account_summary.app)
    
    return {
        "task_id": task_id,
        "status": task_result.state,
        "ready": task_result.ready()
    }
