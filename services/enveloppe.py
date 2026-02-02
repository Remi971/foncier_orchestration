from pydantic import BaseModel
from models import Enveloppe
from sqlalchemy.orm import Session
from sqlalchemy import Engine
from shapely.geometry import shape
import geopandas as gpd


    
def createEnveloppe(db: Engine, gdf: gpd.GeoDataFrame):
    try:
        gdf.to_crs(3857)
        gdf.to_postgis('enveloppe_layer', db, if_exists='append')
    except Exception as e:
        raise Exception(e)
    # geo = shape(feature["geometry"])
    # print("FEATURES : ", feature.keys())
    # enveloppe = Enveloppe(
    #             id=idx, 
    #             commune=feature['properties']["code_insee"], 
    #             geom=geo.wkt
    #             )
    # db.add(enveloppe)
    # db.commit()
    # db.refresh(enveloppe)
    # return enveloppe