from fastapi import Depends
from celery import Celery
from celery.signals import task_success, task_failure
from dependencies import env, EngineDb
from dto.data import DataAcquisitionDto
from schema.process import ProcessSchema
from services.data import get_data
from dto.task import TaskDto
from sqlalchemy.orm import Session
from models import Task
from dto.process import ProcessStatus
import httpx

celery = Celery('orchestration', broker=env.BROKER_URL)
database = EngineDb()

@celery.task(bind=True)
def data_acquisition_task(self, data: DataAcquisitionDto, task_id: TaskDto, db: Session):
    # Create a task in the Database Tasks Table with status "in_progress"
    task = db.query(Task).get({"id": task_id})
    task.status = ProcessStatus.IN_PROGRESS.value
    db.commit()
    # Launch data acquisition service
    get_data(data, db, task_id)
        
    return "This is an example task."

@celery.task(bind=True)
def potentiel_calculation_task(self, data: ProcessSchema, task_id: TaskDto):
    # Create a task in the Database Tasks Table with status "in_progress"
    return "This is an example task."

@celery.task(bind=True)
def enveloppe_generation_task(self, data: ProcessSchema, task_id: TaskDto):
    # Create a task in the Database Tasks Table with status "in_progress"
    return "This is an example task."
    
@task_success.connect
def task_success_handler(sender=None, result=None, **kwargs):
    task = sender
    task_id = task.request.kwargs.get('task_id')
    
    match task_id:
        case TaskDto.DATA_ACQUISITION:
            print("Data acquisition task completed successfully.")
            # Update the task status in the Database to "completed"
        case TaskDto.POTENTIEL_CALCULATION:
            print("Potentiel calculation task completed successfully.")
        case TaskDto.ENVELOPPE_GENERATION:
            print("Enveloppe generation task completed successfully.")
        case _:
            print("Unknown task completed successfully.")
            
@task_failure.connect
def task_failure_handler(sender=None, exception=None, **kwargs):
    task = sender
    task_id = task.request.kwargs.get('task_id')
    
    match task_id:
        case TaskDto.DATA_ACQUISITION:
            print(f"Data acquisition task failed: {exception}")
            # Update the task status in the Database to "failed"
        case TaskDto.POTENTIEL_CALCULATION:
            print(f"Potentiel calculation task failed: {exception}")
        case TaskDto.ENVELOPPE_GENERATION:
            print(f"Enveloppe generation task failed: {exception}")
        case _:
            print(f"Unknown task failed: {exception}")  