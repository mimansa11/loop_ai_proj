from collections import defaultdict
from uuid import uuid4

# Global state
ingestions = {}
batches = defaultdict(list)  # ingestion_id -> list of batches

def create_batches(request):
    ingestion_id = str(uuid4())
    ids = request.ids
    batch_size = 3
    all_batches = [ids[i:i+batch_size] for i in range(0, len(ids), batch_size)]

    batch_list = []
    for batch in all_batches:
        batch_id = str(uuid4())
        batch_list.append({"batch_id": batch_id, "ids": batch, "status": "yet_to_start"})

    ingestions[ingestion_id] = {
        "priority": request.priority,
        "created_time": uuid4().int,  # helps for sorting by time
        "status": "yet_to_start"
    }
    batches[ingestion_id] = batch_list
    return ingestion_id

def update_batch_status(batch_id, ingestion_id, status):
    for batch in batches[ingestion_id]:
        if batch["batch_id"] == batch_id:
            batch["status"] = status
            break
    update_ingestion_status(ingestion_id)

def update_ingestion_status(ingestion_id):
    statuses = [b["status"] for b in batches[ingestion_id]]
    if all(s == "yet_to_start" for s in statuses):
        ingestions[ingestion_id]["status"] = "yet_to_start"
    elif all(s == "completed" for s in statuses):
        ingestions[ingestion_id]["status"] = "completed"
    else:
        ingestions[ingestion_id]["status"] = "triggered"

def get_ingestion_status(ingestion_id):
    return {
        "ingestion_id": ingestion_id,
        "status": ingestions[ingestion_id]["status"],
        "batches": batches[ingestion_id]
    }
