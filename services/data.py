import requests
import tempfile
from dependencies import s3_client
import zipfile
import tarfile
import gzip
import shutil
import os
import json
from dependencies import env
from dto.process import ProcessType
from dto.data import DataFormat
import geopandas as gpd
from sqlalchemy import Engine
from sqlalchemy.orm import Session
from models import Commune
from dto.process import CommuneDto

def download_extract_data(url: str, code: str, layer_name: str):
    print("# Download the .GZ file PCI VECTEUR")
    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()
        
        print("#Upload the .GZ file in MinIO")
        with tempfile.NamedTemporaryFile(delete=False) as tmp_gz:
            for chunk in response.iter_content():
                    tmp_gz.write(chunk)
            tmp_gz_path = tmp_gz.name
        
            s3_client.upload_file(tmp_gz_path, "cartofoncier", f"{code}/gz_file/{layer_name}.gz")
        
        print("#Extract gz file in MinIO")
        with gzip.open(tmp_gz_path, 'rb') as f_in:
            with tempfile.NamedTemporaryFile(delete=False) as layer:
                shutil.copyfileobj(f_in, layer)
                s3_client.upload_file(layer.name, env.MINIO_BUCKET_NAME, f"{code}/geojson_file/{layer_name}.geojson")
    except Exception as e:
        print("download_extract_data ERROR : ", e)
        raise e

def download_extract_cadastre(code: str):
    try:
        url = f"https://cadastre.data.gouv.fr/bundler/pci-vecteur/communes/{code}/dxf"
        response = requests.get(url, stream=True)
        response.raise_for_status()
        
        print("#Upload the ZIP file in MinIO")
        with tempfile.NamedTemporaryFile(delete=False) as tmp_zip:
            for chunk in response.iter_content():
                    tmp_zip.write(chunk)
            tmp_zip_path = tmp_zip.name
        
            s3_client.upload_file(tmp_zip_path, "cartofoncier", f"/{code}-temp.zip")
        
        print("#Extract zip file in MinIO")
        with tempfile.TemporaryDirectory() as tmp_dir:
            with zipfile.ZipFile(tmp_zip_path, 'r') as zip_ref:
                zip_ref.extractall(tmp_dir)
            
            for root, _, files in os.walk(tmp_dir):
                for file in files:
                    file_path = os.path.join(root, file)
                    rel_path = os.path.relpath(file_path, tmp_dir)
                    tar_file = tarfile.open(file_path, 'r:bz2')
                    tar_file.extractall(tmp_dir)
            for root, _, files in os.walk(tmp_dir):
                for file in files:
                    if not file.endswith('.bz2'):
                        file_path = os.path.join(root, file) 
                        s3_client.upload_file(file_path, "cartofoncier", f"{code}/dxf_file/{file}")
    except Exception as e:
        print("download_extract_cadastre ERROR : ", e)
        raise e

def get_data(code: str):
    print("### GET DATA STARTED ###")
    layers = [
              {"name": "parcelle", "download": f"cadastre-{code}-parcelles.json.gz"},
              {"name": "batiments", "download": f"cadastre-{code}-batiments.json.gz"}
              ]
    for layer in layers:
        url = f"https://cadastre.data.gouv.fr/data/etalab-cadastre/latest/geojson/communes/{code[:2]}/{code}/{layer["download"]}"
        download_extract_data(url, code, layer["name"])
    download_extract_cadastre(code)
            
def save_commune_to_db(engine: Engine, communedto: CommuneDto, user_id: str):
    try:
        commune = Commune(code=communedto["code"], nom=communedto["nom"], long=communedto["centre"]["coordinates"][0], lat=communedto["centre"]["coordinates"][1], userId=user_id)
        session = Session(engine)
        if (session.query(Commune).filter_by(code=communedto["code"], userId=user_id).first()):
            print(f"Commune with code {communedto['code']} already exists in the database for this user.")
            return
        session.add(commune)
        session.commit()
    except Exception as e:
        raise Exception(e)

def remove_zip_foler(code: str):
    obj_from_s3 = s3_client.list_objects(Bucket=env.MINIO_BUCKET_NAME)['Contents']
    s3_client.delete_object(
        Bucket="cartofoncier", 
        Key=f"{code}-temp.zip"
    )
    
def save_to_database(engine: Engine, body: DataFormat) -> None:
    print("save_to_database : BODY = ", body.model_dump()["type"])
    try:
        obj = body.model_dump()
        # type = ""
        # match obj["type"]:
        #     case ProcessType.ENVELOPPE_GENERATION.value:
        #         type = "enveloppe"
                
        #     case ProcessType.POTENTIEL_CALCULATION.value:
        #         type = "potentiel"
        geojson = json.loads(obj["data"])
        gdf = gpd.GeoDataFrame.from_features(geojson["features"])
        gdf.crs = 3857
        print("CRS INITIAL : ", gdf.crs)
        gdf.geometry = gdf.geometry.to_crs("EPSG:4326")
        print("NEW CRS : ", gdf.crs)
        gdf.rename_geometry('geom', inplace=True)
        print(gdf.columns)
        columns_to_keep = ['geom', 'nom', 'code', 'minSurfBati', 'bufferBati', 'dilatation', 'maxSurfResidus', 'user', 'erosion', 'minPartInBuffer', 'maxSurfTrou', 'minSurfEnv']
        for column in gdf.columns:
            if column not in columns_to_keep:
                gdf.drop(column, axis=1, inplace=True)
        gdf.to_postgis(obj["type"], engine, if_exists='append')
    except Exception as e:
        raise Exception(e)
