from fastapi import APIRouter, Depends, Query
from boto3.dynamodb.conditions import Key
from decimal import Decimal
from datetime import datetime
from typing import Optional, List

from auth_dependency import get_current_user
from db.dynamodb import get_trades_table, get_strategies_table
from fastapi import HTTPException
from pydantic import BaseModel


router = APIRouter(
    prefix="/trades",
    tags=["Trades"]
)


def decimal_to_native(v):
    if isinstance(v, Decimal):
        if v % 1 == 0:
            return int(v)
        return float(v)
    return v


class TradeTagUpdateRequest(BaseModel):
    timestamp: int
    strategy_ids: List[str]


class BulkTradeTagUpdateRequest(BaseModel):
    updates: List[TradeTagUpdateRequest]


@router.get("/")
async def get_trades(
    current_user: dict = Depends(get_current_user),
    search: Optional[str] = Query(None, description="Search by symbol, direction, or tags"),
    symbol: Optional[str] = Query(None, description="Filter by specific symbol"),
    direction: Optional[str] = Query(None, description="Filter by LONG or SHORT"),
    min_pnl: Optional[float] = Query(None, description="Minimum P&L"),
    max_pnl: Optional[float] = Query(None, description="Maximum P&L"),
    min_r: Optional[float] = Query(None, description="Minimum R multiple"),
    max_r: Optional[float] = Query(None, description="Maximum R multiple"),
    start_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
    tag: Optional[str] = Query(None, description="Filter by tag")
):
    user_id = current_user["user_id"]
    table = get_trades_table()

    # Fetch strategies and build lookup map
    strategies_table = get_strategies_table()
    strategies_response = strategies_table.query(
        KeyConditionExpression=Key("user_id").eq(user_id)
    )
    strategy_map = {
        item["strategy_id"]: item["title"]
        for item in strategies_response.get("Items", [])
    }

    response = table.query(
        KeyConditionExpression=Key("user_id").eq(user_id),
        ScanIndexForward=False
    )

    items = response.get("Items", [])
    trades = []

    for item in items:
       
        resolved_tags = []
        for t in item.get("tags", []):
            if t.startswith("strategy#"):
                strategy_id = t.replace("strategy#", "")
                resolved_tags.append(strategy_map.get(strategy_id, t))
            else:
                resolved_tags.append(t)

        trade = {
            "trade_id": item.get("position_id"),
            "date": datetime.fromtimestamp(
                decimal_to_native(item["timestamp"])
            ).strftime("%Y-%m-%d"),
            "symbol": item.get("symbol"),
            "direction": item.get("direction"),
            "entry": decimal_to_native(item.get("entry_price", 0)),
            "exit": decimal_to_native(item.get("exit_price", 0)),
            "size": decimal_to_native(item.get("volume", 0)),
            "pnl": decimal_to_native(item.get("pnl", 0)),
            "r": decimal_to_native(item.get("r_multiple", 0)),
            "tags": resolved_tags,
            "timestamp": decimal_to_native(item["timestamp"])
        }

        should_include = True

        if search:
            search_lower = search.lower()
            symbol_match = trade["symbol"] and search_lower in trade["symbol"].lower()
            direction_match = trade["direction"] and search_lower in trade["direction"].lower()
            tag_match = any(search_lower in t.lower() for t in resolved_tags)

            if not (symbol_match or direction_match or tag_match):
                should_include = False

        if symbol and should_include:
            if not trade["symbol"] or trade["symbol"].upper() != symbol.upper():
                should_include = False

        if direction and should_include:
            if not trade["direction"] or trade["direction"].upper() != direction.upper():
                should_include = False

        if min_pnl is not None and should_include:
            if trade["pnl"] < min_pnl:
                should_include = False

        if max_pnl is not None and should_include:
            if trade["pnl"] > max_pnl:
                should_include = False

        if min_r is not None and should_include:
            if trade["r"] < min_r:
                should_include = False

        if max_r is not None and should_include:
            if trade["r"] > max_r:
                should_include = False

        if start_date and should_include:
            if trade["date"] < start_date:
                should_include = False

        if end_date and should_include:
            if trade["date"] > end_date:
                should_include = False

        if tag and should_include:
            if tag not in resolved_tags:
                should_include = False

        if should_include:
            trades.append(trade)

    return {
        "status": "success",
        "data": trades,
        "count": len(trades)
    }


@router.put("/tags")
async def update_trade_tags(
    request: TradeTagUpdateRequest,
    current_user: dict = Depends(get_current_user)
):
    table = get_trades_table()
    user_id = current_user["user_id"]

    try:
        new_tags = ["MT5 Trade"]
        for sid in request.strategy_ids:
            new_tags.append(f"strategy#{sid}")

        table.update_item(
            Key={
                "user_id": user_id,
                "timestamp": request.timestamp
            },
            UpdateExpression="SET tags = :tags",
            ExpressionAttributeNames={"#ts": "timestamp"},
            ExpressionAttributeValues={":tags": new_tags},
            ConditionExpression="attribute_exists(#ts)"
        )

        return {
            "status": "success",
            "timestamp": request.timestamp,
            "tags": new_tags
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/tags/bulk")
async def bulk_update_trade_tags(
    request: BulkTradeTagUpdateRequest,
    current_user: dict = Depends(get_current_user)
):
    table = get_trades_table()
    user_id = current_user["user_id"]
    updated = []

    try:
        for update in request.updates:
            new_tags = ["MT5 Trade"]
            for sid in update.strategy_ids:
                new_tags.append(f"strategy#{sid}")

            table.update_item(
                Key={
                    "user_id": user_id,
                    "timestamp": update.timestamp
                },
                UpdateExpression="SET tags = :tags",
                ExpressionAttributeNames={"#ts": "timestamp"},
                ExpressionAttributeValues={":tags": new_tags},
                ConditionExpression="attribute_exists(#ts)"
            )
            updated.append(update.timestamp)

        return {
            "status": "success",
            "updated_count": len(updated),
            "timestamps": updated
        }

    except Exception as e:
        print(f"Bulk update error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))