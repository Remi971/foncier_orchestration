import requests
from dependencies import env
from dto.process import PotentielParamsDto, EnveloppeParamsDto

def format_data(code_insee: str, task_id) -> None:
    response = requests.post(f"{env.MICROSERVICE_SIG}/cadastre/{code_insee}", json={"task_id": task_id})
    response.raise_for_status()
    
def potential_calculation(task_id: str, parameters: PotentielParamsDto):
    response = requests.post(f"{env.MICROSERVICE_SIG}/potentiel", json={"task_id": task_id, "parameters": parameters})
    response.raise_for_status()
    
def enveloppe_calculation(task_id: str, parameters: EnveloppeParamsDto, user_id: str):
    response = requests.post(f"{env.MICROSERVICE_SIG}/enveloppe", json={"task_id": task_id, "parameters": parameters, "user_id": user_id})
    response.raise_for_status()