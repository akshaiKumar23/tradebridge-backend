from pydantic import BaseModel

class WinproActivateRequest(BaseModel):
    user_id: str
    winpro_account_id: str