from fastapi import FastAPI, HTTPException, Body, status, HTTPException, Depends
from sqlalchemy.orm import Session
from dependencies import env, EngineDb, s3_client
from contextlib import asynccontextmanager
from schema.process import ProcessSchema
from dto.process import ProcessType, ProcessStatus
from dto.task import TaskCreationDto, TaskUpdateDto
from dto.data import DataFormat
from task import data_acquisition_task, potentiel_calculation_task, enveloppe_generation_task, format_data_task
from services.task import createNewTask, updateTask
from services.data import save_to_database
import json
import geopandas as gpd

# origins

if (env.ENV != "test"):
    @asynccontextmanager
    async def init_bucket(app: FastAPI):
        print("##### MinIO Bucket Check #####")
        try:
            ## Check if the bucket exists
            bucket_list = s3_client.list_buckets().get('Buckets', [])
            bucket_list_names = [bucket["Name"] for bucket in bucket_list]
            if 'cartofoncier' in bucket_list_names:
                print("Bucket 'cartofoncier' exists")
            ## Create the bucket if it does not exist   
            else:
                print("Bucket 'cartofoncier' does not exist, creating it...")
                s3_client.create_bucket(Bucket='cartofoncier')
                print("Bucket 'cartofoncier' created")
        except Exception as error:
            raise HTTPException(status_code=500, detail=f"Error creating bucket 'cartofoncier': {error}")
        print("##### MinIO Bucket Check Done #####")
        yield

app = FastAPI(
    title="Foncier Orchestration API",
    description="Microservice for Foncier Orchestration",
    version="0.1.0",
    lifespan=init_bucket if env.ENV != "test" else None
)

database = EngineDb()

@app.get("/health", tags=["Root"])
def health_check():
    return {"message": "Welcome to Orchestration of CartoFoncier app!"}

@app.post("/orchestrate", tags=["Orchestration"])
async def orchestrate(request: ProcessSchema = Body, db: Session = Depends(database.get_db)):
    try:
        celery_task = None
        newTask = TaskCreationDto(
            type = request.type.value,
            status = ProcessStatus.IN_PROGRESS.value,
            userId = request.userId
        )
        task = createNewTask(db, newTask)
        match request.type.value:
            case ProcessType.DATA_DOWNLOAD.value:
                try:
                    print("#### DATA ACQUISITION TASK STARTED ####")
                    data_acquisition_task.delay(request.type.value, request.parameters.code_insee, request.userId, task.id)
                    return {"message": "Data acquisition task terminated successfully"}
                except Exception as e:
                    print("Error in DATA ACQUISITION TASK : ", e)
                    task_update = TaskUpdateDto(
                        status = ProcessStatus.FAILED.value,
                        id = task.id
                    )
                    updateTask(db, task_update)
                    raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR)
                
            case ProcessType.POTENTIEL_CALCULATION.value:
                try:
                    print("#### POTENTIAL CALCULATION STARTED ####")
                    potentiel_calculation_task.delay(request.type.value, request.parameters.model_dump(), request.userId, task.id)
                    task_update = TaskUpdateDto(
                        status = ProcessStatus.COMPLETED.value,
                        id = task.id
                    )
                    updateTask(db, task_update)
                    return {"message": "Potential Calculation task terminated successfully"}
                except Exception as e:
                    print("Error in POTENTIAL CALCULATION TASK : ", e)
                    task_update = TaskUpdateDto(
                        status = ProcessStatus.FAILED.value,
                        id = task.id
                    )
                    updateTask(db, task_update)
                    raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR)
            case ProcessType.ENVELOPPE_GENERATION.value:
                try:
                    print("#### ENVELOPPE CALCULATION STARTED ####")
                    #Create a row in enveloppe parameters table -> get id of the new table
                    enveloppe_generation_task(request.type.value, request.parameters.model_dump(), request.userId, task.id)
                    task_update = TaskUpdateDto(
                        status = ProcessStatus.COMPLETED.value,
                        id = task.id
                    )
                    updateTask(db, task_update)
                    return {"message": "Enveloppe Calculation task terminated successfully"}
                except Exception as e:
                    print("Error in ENVELOPPE CALCULATION TASK : ", e)
                    task_update = TaskUpdateDto(
                        status = ProcessStatus.FAILED.value,
                        id = task.id
                    )
                    updateTask(db, task_update)
                    raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR)
        
        return { "message": "Process started successfully", "task_id": ""}
    except Exception as e:
        print("Error orchestrating : ", e)
        raise HTTPException(status_code=500, detail=str(e))
  
    
@app.post("/tasks/{task_id}/status", tags=["Tasks"], description="Update the status of a task")
def update_task_status(task_id: str, status: ProcessStatus = Body(), db: Session = Depends(database.get_db)):
    print("BODY :", status)
    try:
        update_task = TaskUpdateDto(
            status = status.value,
            id = task_id
        )
        print("Update task status called with : ", update_task)
        updateTask(db, update_task)
        return {"message": f"Task {task_id} status updated to {status.value}"}
    except Exception as e:
        print("Error updating task status : ", e)
        raise HTTPException(status_code=500, detail=str(e))
  

    #parameters: EnveloppeParamsDto
     
    
@app.post("/save-data", tags=["Data"], description="Save GeoJson layer to DataBase")
def save_data(body: DataFormat = Body(), db: Session = Depends(database.get_db)):
    try:
        save_to_database(database.engine, body)
    except Exception as e:
        print("ERROR IN SAVE DATA FROM SIG PROCESSING : ", e)
        raise HTTPException(status_code=500, detail=str(e))