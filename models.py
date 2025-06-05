from pydantic import BaseModel
from enum import Enum
from typing import List

class Priority(str, Enum):
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"

class IngestRequest(BaseModel):
    ids: List[int]
    priority: Priority
