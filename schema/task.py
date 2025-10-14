from pydantic import BaseModel

class Task(BaseModel):
    type: str
    status: str
    user_id: str