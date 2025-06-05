import heapq
from threading import Lock
from models import IngestRequest
from state import create_batches
import state

# Thread-safe priority queue
priority_map = {"HIGH": 0, "MEDIUM": 1, "LOW": 2}
job_queue = []
lock = Lock()

def enqueue_request(request: IngestRequest):
    ingestion_id = create_batches(request)
    priority = priority_map[request.priority]
    
    with lock:
        heapq.heappush(job_queue, (priority, state.ingestions[ingestion_id]["created_time"], ingestion_id))
    
    return ingestion_id

def dequeue_request():
    with lock:
        if job_queue:
            return heapq.heappop(job_queue)[2]  # returns ingestion_id
        return None
