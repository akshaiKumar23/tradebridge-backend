from pydantic import BaseModel, EmailStr

class WinproActivateRequest(BaseModel):
    email: str
    winpro_account_id: str