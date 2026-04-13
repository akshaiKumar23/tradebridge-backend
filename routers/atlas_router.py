import json
import logging

from fastapi import APIRouter, Depends, HTTPException

from auth_dependency import get_current_user
from db.dynamodb import get_atlas_stats_table
from services.trading_data_compressor import TradingDataCompressor

logger = logging.getLogger("atlas_router")

router = APIRouter(prefix="/atlas", tags=["Atlas Intelligence"])

compressor = TradingDataCompressor()


@router.get("/insights")
async def get_atlas_insights(current_user: dict = Depends(get_current_user)):

    user_id = current_user["user_id"]


    try:
        atlas_table   = get_atlas_stats_table()
        existing_item = atlas_table.get_item(Key={"user_id": user_id}).get("Item")

        if existing_item and existing_item.get("altas_prompt_response"):
            raw_response = existing_item["altas_prompt_response"]
        else:
         
            logger.info(f"Atlas cache miss for user {user_id} — running compressor")
            raw_response = compressor.get_or_update_atlas_stats(
                user_id, prompt_type="universal"
            )
    except Exception as e:
        logger.error(f"Atlas insights failed for user {user_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to load Atlas insights")

    try:
       
        cleaned = raw_response.strip()
        if cleaned.startswith("```"):
            cleaned = cleaned.split("```")[1]
            if cleaned.startswith("json"):
                cleaned = cleaned[4:]
            cleaned = cleaned.strip()

        parsed = json.loads(cleaned)
    except (json.JSONDecodeError, AttributeError) as e:
        logger.error(f"Atlas response was not valid JSON for user {user_id}: {e}")
        _clear_atlas_cache(user_id)
        raise HTTPException(
            status_code=500,
            detail="Atlas response was malformed (invalid JSON). Cache cleared — please retry.",
        )

    if "insights" not in parsed or "summary" not in parsed:
        logger.warning(
            f"Atlas response missing required keys for user {user_id}. "
            f"Got keys: {list(parsed.keys())}"
        )
        _clear_atlas_cache(user_id)
        raise HTTPException(
            status_code=500,
            detail="Atlas response had unexpected structure. Cache cleared — please retry.",
        )

    return {
        "status": "success",
        "data": parsed,
        "raw": None,
    }


def _clear_atlas_cache(user_id: str) -> None:
  
    try:
        get_atlas_stats_table().delete_item(Key={"user_id": user_id})
        logger.info(f"Atlas cache cleared for user {user_id}")
    except Exception as e:
        logger.warning(f"Failed to clear Atlas cache for user {user_id}: {e}")