from pydantic import BaseModel
from dto.process import ProcessType

class DataAcquisitionDto(BaseModel):
    code_insee: str
    user_id: str
    
class DataFormat(BaseModel):
    type: str
    data: str