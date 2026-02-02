from sqlalchemy import Column, String, TIMESTAMP, text, ForeignKey, Integer
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import declarative_base
from dto.users import Roles
import uuid
from geoalchemy2 import Geometry

Base = declarative_base()

class User(Base):
    __tablename__= 'users'
    id = Column(UUID(as_uuid=True), primary_key=True, nullable=False, unique=True, default=uuid.uuid4)
    firstname = Column(String, nullable=False)
    lastname = Column(String, nullable=False)
    email = Column(String, nullable=False, unique=True)
    hashed_password = Column(String, nullable=False)
    role = Column(String, nullable=False, default=Roles.BASIC.value)
    createdAt = Column(TIMESTAMP(timezone=True), nullable=False, default=text('Now()'))
    updatedAt = Column(TIMESTAMP(timezone=True), nullable=False, default=text('Now()'), server_onupdate=text('Now()'))
    
class Task(Base):
    __tablename__ = 'tasks'
    
    id = Column(UUID(as_uuid=True), primary_key=True, index=True, nullable=False, unique=True, default=uuid.uuid4)
    type = Column(String, index=True)
    status = Column(String, index=True)
    owner = Column(UUID(as_uuid=True), ForeignKey("users.id"), nullable=False)
    created_at = Column(TIMESTAMP(timezone=True), nullable=False, default=text('now()'))
    updated_at = Column(TIMESTAMP(timezone=True), nullable=False, default=text('now()'), server_onupdate=text('now()'))
    