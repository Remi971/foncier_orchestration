from fastapi import Depends
from celery import Celery
from celery.signals import task_success, task_failure, worker_process_init, worker_process_shutdown
from dependencies import env, EngineDb
from services.data import get_data, remove_zip_foler, save_commune_to_db
from services.task import createNewTask, updateTask
from services import sig
from dto.task import TaskDto, TaskCreationDto, TaskUpdateDto
from dto.process import PotentielParamsDto, EnveloppeParamsDto, CommuneDto
from sqlalchemy.orm import Session
from dto.process import ProcessStatus, ProcessType
from uuid import UUID
from publisher import Publisher

print("DATABASE_URL : ", env.DATABASE_URL)

celery = Celery('orchestration', broker=env.BROKER_URL)
database = EngineDb()
publisher = Publisher()
db = None

@worker_process_init.connect
def init_worker(**kwargs):
    print("#### INIT WORKER ####")
    # db = Session(database.engine)
    # print("DB INIT : ", db)

@celery.task(bind=True)
def data_acquisition_task(self, task_type: str, commune: CommuneDto, user_id: str, task_id: UUID,):
    try:
        publisher.publish_event(env.REDIS_CHANNEL, {"type": "DATA_DOWNLOAD", "status": ProcessStatus.STARTED.value, "message": "Data acquisition task started"})
        get_data(commune["code"])
        remove_zip_foler(commune["code"])
        save_commune_to_db(database.engine, commune, user_id)
        publisher.publish_event(env.REDIS_CHANNEL, {"type": "DATA_DOWNLOAD", "status": ProcessStatus.COMPLETED.value, "message": "Data acquisition task completed successfully"})
        
    except Exception as e:
        raise e
    
@celery.task(bind=True)
def format_data_task(self, task_type: str, commune: CommuneDto, user_id: str, task_id: UUID):
    print("CALLING SIG MICROSERVICE - Format Data")
    try:
        sig.format_data(commune["code"], str(task_id))
        # save
        return {"message": "Data fomated COMPLETE"}
    except Exception as e:
        print("$$$$$ ERROR LAUNCHING MICROSERVICE SIG - FORMAT DATA $$$$$")
        raise Exception(e)
    
@celery.task(bind=True)
def potentiel_calculation_task(self, task_type: str, parameters: PotentielParamsDto, user_id: str, task_id: UUID):
    print("CALLING SIG MICROSERVICE - Potential calculation")
    
    try:
        sig.potential_calculation(str(task_id), parameters)
        return {"message": "Potential Calculation COMPLETE"}
    except Exception as e:
        print("$$$$$ ERROR LAUNCHING MICROSERVICE SIG - POTENTIAL CALCULATION $$$$$ : Potential Calculation FAILED")
        raise Exception(e)

@celery.task(bind=True)
def enveloppe_generation_task(self, task_type: str, parameters: EnveloppeParamsDto, user_id: str, task_id: UUID):
    print("CALLING SIG MICROSERVICE - Enveloppe Calculation")
    try:
        sig.enveloppe_calculation(str(task_id), parameters, user_id)
        return {"message": "Enveloppe Calculation COMPLETE"}
    except Exception as e:
        print("$$$$$ ERROR LAUNCHING MICROSERVICE SIG - ENVELOPPE CALCULATION $$$$")
        raise Exception(e)
    
@task_success.connect
def task_success_handler(sender=None, result=None, **kwargs):
    task = sender
    print("REQUEST", task.request.args)
    db = Session(database.engine)
    _, commune, user_id, task_id = task.request.args
    completed_task = TaskUpdateDto(
        status = ProcessStatus.COMPLETED.value,
        id = task_id
    )
    updateTask(db, completed_task)
    match task.request.args[0]:
        case TaskDto.DATA_DOWNLOAD.value:
            print("Data acquisition task completed successfully.")
            try:
                task_update = TaskUpdateDto(
                    status = ProcessStatus.COMPLETED.value,
                    id = task_id
                )
                updateTask(db, task_update)
                create_task = TaskCreationDto(
                type = ProcessType.DATA_PROCESSING.value,
                status = ProcessStatus.IN_PROGRESS.value,
                userId = user_id
                )
                newTask = createNewTask(db, create_task)
                format_data_task.delay(ProcessType.DATA_PROCESSING.value, commune, user_id, newTask.id)
            except Exception as e:
                print(str(e))
                raise e
        case TaskDto.POTENTIEL_CALCULATION.value:
            print("Potentiel calculation task completed successfully.")
        case TaskDto.ENVELOPPE_GENERATION.value:
            print("Enveloppe generation task completed successfully.")
        case TaskDto.DATA_PROCESSING.value:
            print("Data Transformation task completed successfully")
            try:
                task_update = TaskUpdateDto(
                    status = ProcessStatus.COMPLETED.value,
                    id = task_id
                )
                updateTask(db, task_update)
            except Exception as e:
                print(str(e))
                raise e
        case _:
            print("Unknown task completed successfully.")
    db.close()
            
@task_failure.connect
def task_failure_handler(sender=None, exception=None, **kwargs):
    task = sender
    print("REQUEST", task.request.args)
    task_id = task.request.kwargs.get('task_id')
    
    publisher.publish_event(env.REDIS_CHANNEL, {"type": task.request.args[0], "status": "ERROR", "message": f"Task failed: {exception}"})
    
    match task_id:
        case TaskDto.DATA_DOWNLOAD.value:
            print(f"Data acquisition task failed: {exception}")
            # Update the task status in the Database to "failed"
        case TaskDto.POTENTIEL_CALCULATION.value:
            print(f"Potentiel calculation task failed: {exception}")
        case TaskDto.ENVELOPPE_GENERATION.value:
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