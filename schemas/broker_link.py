from pydantic import BaseModel
class BrokerLinkRequest(BaseModel):
    broker: str
    login: int
    password: str
    server: str
