from pydantic import BaseModel
from models import Enveloppe
from sqlalchemy.orm import Session
from shapely.geometry import shape


    
def createEnveloppe(db: Session, idx: int, feature: dict):
    geo = shape(feature["geometry"])
    print("FEATURES : ", feature.keys())
    enveloppe = Enveloppe(
                id=idx, 
                commune=feature['properties']["code_insee"], 
                geom=geo.wkt
                )
    db.add(enveloppe)
    db.commit()
    db.refresh(enveloppe)
    return enveloppe