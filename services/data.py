import requests
import tempfile
from dependencies import s3_client
import zipfile
import tarfile
import os

def get_data(code_insee: str):
    print("### GET DATA STARTED ###")
    print("# Download the ZIP file PCI VECTEUR")
    url = f"https://cadastre.data.gouv.fr/bundler/pci-vecteur/communes/{code_insee}/dxf"
    response = requests.get(url, stream=True)
    response.raise_for_status()
    
    print("#Upload the ZIP file in MinIO")
    with tempfile.NamedTemporaryFile(delete=False) as tmp_zip:
        for chunk in response.iter_content():
                tmp_zip.write(chunk)
        tmp_zip_path = tmp_zip.name
    
        s3_client.upload_file(tmp_zip_path, "cartofoncier", f"{code_insee}-temp.zip")
    
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
                    s3_client.upload_file(file_path, "cartofoncier", f"{code_insee}/dxf_file/{file}")
            

def remove_zip_foler(code_insee: str):
    s3_client.delete_object(
        Bucket="cartofoncier", 
        Key=f"{code_insee}-temp.zip"
    )
    
def data_processing():
    ...