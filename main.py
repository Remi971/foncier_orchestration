from fastapi import FastAPI, HTTPException, Depends, Body
import boto3
from dependencies import env, EngineDb, s3_client
from contextlib import asynccontextmanager
from schema.process import ProcessSchema
from dto.process import ProcessType
from models import Task
from sqlalchemy.orm import Session
from task import data_acquisition_task, potentiel_calculation_task, enveloppe_generation_task

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

database =  EngineDb()

@app.get("/health", tags=["Root"])
def health_check():
    return {"message": "Welcome to Orchestration of CartoFoncier app!"}

@app.post("/orchestrate", tags=["Orchestration"])
async def orchestrate(request: ProcessSchema = Body, db: Session = Depends(database.get_db)):
    # Save the task in the database with status "pending"
    print("Request : ", request.type.value)
    task = Task(
        type=request.type.value,  # Generate a unique task ID
        status="pending",
        owner=request.userId
    )
    db.add(task)
    db.commit()
    db.refresh(task)
    print("task saved in database : ", task.id)
    # Lauch the first task in the chain (data acquisition)
    match request.type.value:
        case ProcessType.DATA_DOWNLOAD:
            return data_acquisition_task(task_id=task.id, db=db)
        case ProcessType.POTENTIEL_CALCULATION:
            return potentiel_calculation_task(task_id=task.id, db=db)
        case ProcessType.ENVELOPPE_GENERATION:
            return enveloppe_generation_task(task_id=task.id, db=db)
    
    return { "message": "Process started successfully", "task_id": ""}
    
    
