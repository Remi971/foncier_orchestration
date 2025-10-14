from dto.data import DataAcquisitionDto
import requests
import tempfile
from dependencies import s3_client
import zipfile
import os
from sqlalchemy.orm import Session
from models import Task
from dto.process import ProcessStatus

def get_data(self, data: DataAcquisitionDto, db: Session, task_id: str):
    try:
        # Download the ZIP file PCI VECTEUR
        url = f"https://cadastre.data.gouv.fr/bundler/pci-vecteur/communes/{data.code_insee}/edigeo"
        response = requests.get(url, stream=True)
        response.raise_for_status()
        
        #Upload the ZIP file in MinIO
        with tempfile.NamedTemporaryFile(delete=False) as tmp_zip:
                for chunk in response.iter_content(chunk_size=8192):
                        tmp_zip.write(chunk)
                tmp_zip_path = tmp_zip.name
        
        s3_client.upload_file(tmp_zip_path, "pci_temporaire", "cartofoncier")
        
        #Extract zip file in MinIO
        with tempfile.TemporaryDirectory() as tmp_dir:
                with zipfile.ZipFile(tmp_zip_path, 'r') as zip_ref:
                        zip_ref.extractall(tmp_dir)
                
                for root, _, files in os.walk(tmp_dir):
                        for file in files:
                                file_path = os.path.join(root, file)
                                rel_path = os.path.relpath(file_path, tmp_dir)
                                #s3_key = f"{extract_to.rstrip('/')}/{rel_path}"
                                s3_client.upload_file(file_path, "pci-temporaire", "cartofoncier")
        
        #Update the task
        task = db.query(Task).get({"id": task_id})
        task.status = ProcessStatus.COMPLETED.value
        db.commit()
    except Exception as e:
         # Set task to failed
        task = db.query(Task).get({"id": task_id})
        task.status = ProcessStatus.FAILED.value
        db.commit()
        self.retry(exc=e, countdown=60)