from enum import Enum

class Roles(Enum):
    def __str__(self):
        return str(self.value)
    SUPERADMIN="super-admin"
    ADMIN="admin"
    BASIC="basic"


    