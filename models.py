from pydantic import BaseModel
from typing import List

class FileDeleteRequest(BaseModel):
    file_ids: List[int]
