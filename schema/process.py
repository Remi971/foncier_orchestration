from pydantic import BaseModel
from dto.process import PotentielParamsDto, EnveloppeParamsDto, CommuneDto, ProcessType



class ProcessSchema(BaseModel):
    type: ProcessType
    parameters: CommuneDto | PotentielParamsDto | EnveloppeParamsDto
    userId: str
    
    # model_config = {
    #     "json_schema_extra": {
    #         "examples": [
    #             {
    #                 "type": ProcessType.DATA_DOWNLOAD.value,
    #                 "parameters": {
    #                     "code_insee": "84056",
    #                     "nom": "Jonquières"
    #                 },
    #                 "userId": "c6affaba-254b-4fa0-8305-36570587fdd3"
    #             }
    #         ]
    #     }
    # }