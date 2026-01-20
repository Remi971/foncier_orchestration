from fastapi import Depends
from celery import Celery
from celery.signals import task_success, task_failure, worker_process_init, worker_process_shutdown
from dependencies import env, EngineDb
from dto.process import CommuneDto
from schema.process import ProcessSchema
from services.data import get_data, remove_zip_foler
from services.task import createNewTask, updateTask
from dto.task import TaskDto, TaskCreationDto, TaskUpdateDto
from sqlalchemy.orm import Session
from models import Task
from dto.process import ProcessStatus, ProcessType
import httpx
from fastapi import HTTPException, status, Depends
from uuid import UUID
import os
import dotenv

print("DATABASE_URL : ", env.DATABASE_URL)

celery = Celery('orchestration', broker=env.BROKER_URL)
database = EngineDb()

db = None

@worker_process_init.connect
def init_worker(**kwargs):
    print("#### INIT WORKER ####")
    # db = Session(database.engine)
    # print("DB INIT : ", db)

@celery.task(bind=True)
def data_acquisition_task(self, task_type: str, code_insee: str, user_id: str, task_id: UUID):
    ## task_type and user_id are used in task_success_handler and task_failure_handler
    get_data(code_insee)
    
    update_task = TaskUpdateDto(
        status = ProcessStatus.COMPLETED.value, 
        id = task_id
    )
    remove_zip_foler(code_insee)
    ## Convert Layers to geojson and upload to MinIO
    global db
    db = Session(database.engine)
    print("update_task : ", update_task)
    updateTask(db, update_task)
    db.close()
    
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
    print("task request", task.request.args)
    db = Session(database.engine)
    ## TODO: Find a way to update task and create a new one to launch DATA PROCESSING task (microservice SIG)
    match task.request.args[0]:
        case TaskDto.DATA_DOWNLOAD.value:
            _, code_insee, user_id, task_id = task.request.args
            completed_task = TaskUpdateDto(
                status = ProcessStatus.COMPLETED.value,
                id = task_id
            )
            updateTask(db, completed_task)
            print("Data acquisition task completed successfully.")
            create_task = TaskCreationDto(
                type = ProcessType.DATA_PROCESSING.value,
                status = ProcessStatus.IN_PROGRESS.value,
                userId = user_id
            )
            newTask = createNewTask(db, create_task)
            try:
                # Launch DATA_PROCESSING : GIS service
                response_sig = httpx.post(f"{env.MICROSERVICE_SIG}/cadastre/{code_insee}", json={"task_id": str(newTask.id)})
                update_task = TaskUpdateDto(
                    status = ProcessStatus.COMPLETED.value,
                    id = newTask.id
                )
                updateTask(db, update_task)
                print("RESPONSE SIG : ", response_sig)
                # updateTask(newTask.id, "status", ProcessStatus.COMPLETED.value)
            except Exception as e:
                print("$$$$$ ERROR LAUNCHING MICROSERVICE SIG $$$$$ : ", e)
                update_task = TaskUpdateDto(
                    status = ProcessStatus.FAILED.value,
                    id = newTask.id
                )
                updateTask(db, update_task)
                
        case TaskDto.POTENTIEL_CALCULATION:
            print("Potentiel calculation task completed successfully.")
        case TaskDto.ENVELOPPE_GENERATION:
            print("Enveloppe generation task completed successfully.")
        case TaskDto.DATA_PROCESSING:
            print("Data Transformation task completed successfully")
        case _:
            print("Unknown task completed successfully.")
    db.close()
            
@task_failure.connect
def task_failure_handler(sender=None, exception=None, **kwargs):
    task = sender
    task_id = task.request.kwargs.get('task_id')
    
    match task_id:
        case TaskDto.DATA_DOWNLOAD:
            print(f"Data acquisition task failed: {exception}")
            # Update the task status in the Database to "failed"
        case TaskDto.POTENTIEL_CALCULATION:
            print(f"Potentiel calculation task failed: {exception}")
        case TaskDto.ENVELOPPE_GENERATION:
            print(f"Enveloppe generation task failed: {exception}")
        case _:
            print(f"Unknown task failed: {exception}")  
            
@worker_process_shutdown.connect
def shutdown_worker(**kwargs):
    print("#### SHUTDOWN WORKER ####")
    # global db
    # if db:
    #     print("DB Shutdown : ", db)
    #     db.close()