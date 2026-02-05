from pydantic import BaseModel, Field
from typing import Optional

class JournalCreateRequest(BaseModel):
    date: str
    pnl: float
    trades: int = Field(..., ge=0)
    session_quality: float = Field(..., ge=0, le=10)
    notes: Optional[str] = ""
    learnings: Optional[str] = ""
