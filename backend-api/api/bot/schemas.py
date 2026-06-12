# backend-api/api/bot/schemas.py
from pydantic import BaseModel

class BindCodeResponse(BaseModel):
    status: str
    bind_code: str
    expires_in_secs: int
    bot_username: str