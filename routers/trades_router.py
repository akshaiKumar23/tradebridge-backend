from fastapi import APIRouter, Depends, Query
from boto3.dynamodb.conditions import Key, Attr
from decimal import Decimal
from datetime import datetime
from typing import Optional

from auth_dependency import get_current_user
from db.dynamodb import get_trades_table


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

    response = table.query(
        KeyConditionExpression=Key("user_id").eq(user_id),
        ScanIndexForward=False
    )

    items = response.get("Items", [])
    trades = []

    for item in items:
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
            "tags": item.get("tags", []),
            "timestamp": decimal_to_native(item["timestamp"])
        }
        
        
        should_include = True
        
        
        if search:
            search_lower = search.lower()
            symbol_match = search_lower in trade["symbol"].lower()
            direction_match = search_lower in trade["direction"].lower()
            tag_match = any(search_lower in tag.lower() for tag in trade["tags"])
            
            if not (symbol_match or direction_match or tag_match):
                should_include = False
        
        
        if symbol and should_include:
            if trade["symbol"].upper() != symbol.upper():
                should_include = False
        
        
        if direction and should_include:
            if trade["direction"].upper() != direction.upper():
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
            if tag not in trade["tags"]:
                should_include = False
        
        if should_include:
            trades.append(trade)

    return {
        "status": "success",
        "data": trades,
        "count": len(trades)
    }