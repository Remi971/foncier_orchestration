from enum import Enum
from dto.process import ProcessStatus, ProcessType
from typing import Optional
from pydantic import BaseModel
from uuid import UUID

class TaskDto(Enum):
    DATA_DOWNLOAD = "DATA_DOWNLOAD"
    POTENTIEL_CALCULATION = "POTENTIEL_CALCULATION"
    ENVELOPPE_GENERATION = "ENVELOPPE_GENERATION"
    DATA_PROCESSING = "DATA_PROCESSING"
    
class TaskCreationDto(BaseModel):
    type: ProcessType
    status: Optional[ProcessStatus]
    userId: str
    
class TaskUpdateDto(BaseModel):
    status: ProcessStatus
    id: UUID