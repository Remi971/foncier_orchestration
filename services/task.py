from dto.process import ProcessStatus
from dto.task import TaskCreationDto, TaskUpdateDto
from models import Task
from sqlalchemy.orm import Session

def createNewTask(db: Session, task: TaskCreationDto):
    task = Task(
        type= task.type.value, 
        status= task.status.value if task.status else ProcessStatus.IN_PROGRESS,
        owner = task.userId,
        )
    db.add(task)
    db.commit()
    db.refresh(task)
    return task

def updateTask(db: Session, task: TaskUpdateDto):
    current_task = db.query(Task).get({"id": task.id})
    current_task.status = task.status
    db.commit()
    db.refresh(current_task)
    return current_task
    