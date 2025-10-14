from pydantic import BaseModel
from dto.process import PotentielParamsDto, EnveloppeParamsDto, CommuneDto, ProcessType



class ProcessSchema(BaseModel):
    type: ProcessType
    parameters: PotentielParamsDto | EnveloppeParamsDto | CommuneDto
    userId: str