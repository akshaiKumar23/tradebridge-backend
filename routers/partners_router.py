import os
import hmac
import logging
from datetime import datetime
from fastapi import APIRouter, Depends, HTTPException, Request
from boto3.dynamodb.conditions import Key
from db.dynamodb import get_onboarding_table
from schemas.partners import WinproActivateRequest

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/partner", tags=["partners"])
WINPROFX_API_KEY = os.getenv("WINPROFX_API_KEY")


def verify_winprofx_key(request: Request):
    api_key = request.headers.get("X-Api-Key")
    if not api_key or not WINPROFX_API_KEY:
        raise HTTPException(status_code=401, detail="Missing API key")
    if not hmac.compare_digest(api_key, WINPROFX_API_KEY):
        raise HTTPException(status_code=401, detail="Invalid API key")


@router.post("/winprofx/activate", include_in_schema=False)
async def winprofx_partner_activate(
    body: WinproActivateRequest,
    request: Request,
    _: None = Depends(verify_winprofx_key),
):
    table = get_onboarding_table()
    now = datetime.utcnow().isoformat()

    # Find user by email via GSI
    response = table.query(
        IndexName="email-index",
        KeyConditionExpression=Key("email").eq(body.email),
        Limit=1,
    )
    items = response.get("Items", [])

    if not items:
        raise HTTPException(status_code=404, detail="User not found")

    item = items[0]
    user_id = item["user_id"]

    if item.get("has_paid"):
        return {"status": "already_activated"}

    table.update_item(
        Key={"user_id": user_id},
        UpdateExpression="""
            SET has_paid = :p,
                paid_via = :via,
                winpro_account_id = :wid,
                paid_at = :pa,
                updated_at = :u
        """,
        ExpressionAttributeValues={
            ":p":   True,
            ":via": "winprofx_partner",
            ":wid": body.winpro_account_id,
            ":pa":  now,
            ":u":   now,
        },
    )

    logger.info(f"WinProFX activation: user {user_id} ({body.email}) marked as paid")
    return {"status": "success"}