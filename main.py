from fastapi import FastAPI, BackgroundTasks
from models import IngestRequest
from qmanager import enqueue_request
from state import get_ingestion_status

app = FastAPI()

@app.post("/ingest")
async def ingest_data(request: IngestRequest, background_tasks: BackgroundTasks):
    ingestion_id = enqueue_request(request)
    background_tasks.add_task(process_queue)  # triggers background processor
    return {"ingestion_id": ingestion_id}

@app.get("/status/{ingestion_id}")
def get_status(ingestion_id: str):
    return get_ingestion_status(ingestion_id)


# import this here to avoid circular import issues
from processor import process_queue
