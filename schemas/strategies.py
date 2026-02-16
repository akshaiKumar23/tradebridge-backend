from typing import List
from pydantic import BaseModel, Field

class StrategyCreateRequest(BaseModel):
    title: str = Field(..., min_length=1)
    description: str | None = None
    rules: List[str] = Field(default_factory=list)
    win_rate: str | None = "—"
    avg_rr: str | None = "—"
    trades: int = 0
