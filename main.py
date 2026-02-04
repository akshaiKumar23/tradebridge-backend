# # from fastapi import FastAPI, HTTPException
# # from pydantic import BaseModel
# # from celery.result import AsyncResult
# # from tasks import get_account_summary
# # import os
# # from dotenv import load_dotenv

# # load_dotenv()

# # app = FastAPI(title="MT5 Account API with Celery")


# # class AccountRequest(BaseModel):
# #     server: str
# #     login: int
# #     password: str


# # @app.post("/account/summary")
# # def request_account_summary(request: AccountRequest):

# #     task = get_account_summary.apply_async(
# #         args=[request.server, request.login, request.password]
# #     )
    
# #     return {
# #         "task_id": task.id,
# #         "status": "processing",
# #         "message": "Task submitted. Use /account/summary/{task_id} to check status"
# #     }


# # @app.get("/account/summary/{task_id}")
# # def get_task_result(task_id: str):

# #     task_result = AsyncResult(task_id, app=get_account_summary.app)
    
# #     if task_result.ready():
# #         result = task_result.get()
        
# #         if result.get("status") == "error":
# #             raise HTTPException(status_code=500, detail=result.get("message"))
        
# #         return result
    
# #     elif task_result.failed():
# #         raise HTTPException(
# #             status_code=500,
# #             detail=f"Task failed: {str(task_result.info)}"
# #         )
    
# #     else:
# #         return {
# #             "task_id": task_id,
# #             "status": "processing",
# #             "message": "Task is still being processed"
# #         }


# # @app.get("/account/summary/{task_id}/status")
# # def get_task_status(task_id: str):
 
# #     task_result = AsyncResult(task_id, app=get_account_summary.app)
    
# #     return {
# #         "task_id": task_id,
# #         "status": task_result.state,
# #         "ready": task_result.ready()
# #     }




# #################################################################################




# from fastapi import FastAPI, HTTPException
# from pydantic import BaseModel
# from celery.result import AsyncResult
# from tasks import get_account_summary
# import os
# from dotenv import load_dotenv

# load_dotenv()

# app = FastAPI(title="MT5 Account API with Celery")

# class AccountRequest(BaseModel):
#     server: str
#     login: int
#     password: str

# @app.post("/account/summary")
# def request_account_summary(request: AccountRequest):
#     task = get_account_summary.apply_async(
#         args=[request.server, request.login, request.password]
#     )
#     return {
#         "task_id": task.id,
#         "status": "processing",
#         "message": "Task submitted. Use /account/summary/{task_id} to check status"
#     }

# @app.get("/account/summary/{task_id}")
# def get_task_result(task_id: str):
#     task_result = AsyncResult(task_id, app=get_account_summary.app)
    
#     if task_result.ready():
#         result = task_result.get()
#         if result.get("status") == "error":
#             raise HTTPException(status_code=500, detail=result.get("message"))
#         return result
    
#     return {
#         "task_id": task_id,
#         "status": task_result.status,
#         "message": "Task is still processing or pending"
#     }

import os
from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel
from celery.result import AsyncResult
from dotenv import load_dotenv

# Import our refactored logic and middleware
from auth_middleware import CognitoAuthMiddleware
from tasks import get_account_summary
from journal_logic import create_journal_entry, get_user_journals

load_dotenv()

app = FastAPI(
    title="MT5 Secure Analytics & Journal API",
    description="Secure API for MT5 data processing and trading journals protected by AWS Cognito"
)

# ------------------------------------------------------------------------------
# MIDDLEWARE
# ------------------------------------------------------------------------------
# This protects all routes. The middleware verifies the JWT from the 
# Authorization header before allowing the request to reach the routes.
app.add_middleware(CognitoAuthMiddleware)

# ------------------------------------------------------------------------------
# MODELS (Pydantic)
# ------------------------------------------------------------------------------
class AccountRequest(BaseModel):
    server: str
    login: int
    password: str

class JournalEntryRequest(BaseModel):
    user_id: str  # This should match the 'id' in the Users table
    title: str
    content: str
    session_type: str
    trading_date: str

# ------------------------------------------------------------------------------
# ROUTES: AUTH INFO
# ------------------------------------------------------------------------------
@app.get("/me")
async def get_current_user(request: Request):
    """
    Returns the decoded Cognito token claims for the current user.
    Useful for debugging or getting the user's 'sub' or 'email'.
    """
    return {"user_claims": request.state.user}

# ------------------------------------------------------------------------------
# ROUTES: MT5 ACCOUNT SUMMARY
# ------------------------------------------------------------------------------
@app.post("/account/summary")
def request_account_summary(request: AccountRequest):
    """
    Trigger a background task to fetch MT5 analytics and save to DynamoDB.
    """
    task = get_account_summary.apply_async(
        args=[request.server, request.login, request.password]
    )
    return {
        "task_id": task.id,
        "status": "processing",
        "message": "MT5 analytics task submitted."
    }

@app.get("/account/summary/{task_id}")
def get_task_result(task_id: str):
    """
    Check the status or get the result of an MT5 analytics task.
    """
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

# ------------------------------------------------------------------------------
# ROUTES: TRADING JOURNAL
# ------------------------------------------------------------------------------
@app.post("/journal")
def add_journal_note(request: JournalEntryRequest):
    """
    Create a new journal entry in the UserNotes table.
    """
    result = create_journal_entry(request.user_id, request.dict())
    if result["status"] == "error":
        raise HTTPException(status_code=500, detail=result["message"])
    return result

@app.get("/journal/{user_id}")
def fetch_journal_notes(user_id: str):
    """
    Retrieve all journal notes for a specific user ID.
    """
    result = get_user_journals(user_id)
    if result["status"] == "error":
        raise HTTPException(status_code=500, detail=result["message"])
    return result

# ------------------------------------------------------------------------------
# HEALTH CHECK
# ------------------------------------------------------------------------------
@app.get("/health")
async def health_check():
    return {"status": "healthy"}