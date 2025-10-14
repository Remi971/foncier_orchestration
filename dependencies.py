from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy import create_engine
import boto3
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv
import os
load_dotenv()

#Handle env variable differently : uvicorn main:app --reload --env-file .env
# TODO:Use python-dotenv to declare variable from environnement variables


class Env:
    # model_config = SettingsConfigDict(env_file=".env.development", env_file_encoding="utf-8")
    # Database settings
    DATABASE_URL= os.getenv("DATABASE_URL")
    # MinIO settings
    MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
    MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
    MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
    MINIO_BUCKET_NAME = os.getenv("MINIO_BUCKET_NAME")
    # CartoFoncier API URL
    CARTOFONCIER_API_URL = os.getenv("CARTOFONCIER_API_URL")
    BROKER_URL = os.getenv("BROKER_URL")
    MICROSERVICE_SIG = os.getenv("MICROSERVICE_SIG")
    ENV = os.getenv("ENV")

env = Env()


class EngineDb:
    def __init__(self):
        self.engine = create_engine(env.DATABASE_URL)
        self.session = self.getSession()
        
    def getSession(self):
        return sessionmaker(autocommit=False, autoflush=False, bind=self.engine)
    
    def get_db(self):
        db = self.session()
        try:
            yield db
        finally:
            db.close()
            
s3_client = boto3.client(
    "s3",
    endpoint_url= env.MINIO_ENDPOINT,
    aws_access_key_id= env.MINIO_ACCESS_KEY,
    aws_secret_access_key= env.MINIO_SECRET_KEY,
)
