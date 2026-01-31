from enum import Enum
from pydantic import BaseModel

class ProcessStatus(str, Enum):
    PENDING = "PENDING"
    IN_PROGRESS = "IN_PROGRESS"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    
class ProcessType(Enum):
    DATA_DOWNLOAD = "DATA_DOWNLOAD"
    DATA_PROCESSING = "DATA_PROCESSING"
    POTENTIEL_CALCULATION = "POTENTIEL_CALCULATION"
    ENVELOPPE_GENERATION = "ENVELOPPE_GENERATION"
    
class CommuneDto(BaseModel):
    code_insee: str
    nom: str
    
class PotentielParamsDto(CommuneDto):
    minSurfParNue: int = 400 # Surface minimale de la parcelle non bâtie
    minSurfParBatie: int = 1000 # Surface minimale de la parcelle bâtie
    maxCes: int = 50 # CES maximum de la parcelle bâtie
    minSurfDivision: int = 400 # Surface minimale du résultat de la division parcellaire
    distBufferTest: int = 10 # Distance du buffer pour le test
    distBufferBati: int = 8 # Distance du buffer autour du bâti
    
class EnveloppeParamsDto(BaseModel):
    minSurfBati: int = 30 # Surface minimale du Bâti
    bufferBati: int = 4 # Buffer autour du bâti
    dilatation: int = 50 # Distance Buffer pour la dilatation
    erosion: int = -30 # Distance Buffer pour l'érosion
    minPartInBuffer: int = 50 # Part minimale de la parcelle comprise dans le buffer dilation-erosion
    maxSurfTrou: int = 2000 # Surface maximale des trous à combler
    minSurfEnv: int = 30000 # Surface minimale des enveloppes
    maxSurfResidus: int = 5 # Surface maximale des résidus à supprimer
    
class EnveloppeParameters(EnveloppeParamsDto):
    id: str
    