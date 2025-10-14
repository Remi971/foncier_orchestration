from pydantic import BaseModel

class DataAcquisitionDto(BaseModel):
    code_insee: str
    user_id: str
    
