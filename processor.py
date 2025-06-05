import asyncio
import time
from qmanager import dequeue_request
from state import batches, update_batch_status

processing_lock = asyncio.Lock()

async def simulate_external_api(id_):
    await asyncio.sleep(1)  # simulate delay
    return {"id": id_, "data": "processed"}

async def process_batch(batch, ingestion_id):
    update_batch_status(batch["batch_id"], ingestion_id, "triggered")
    await asyncio.gather(*(simulate_external_api(id_) for id_ in batch["ids"]))
    update_batch_status(batch["batch_id"], ingestion_id, "completed")

last_processed_time = 0

async def process_queue():
    global last_processed_time

    async with processing_lock:
        while True:
            ingestion_id = dequeue_request()
            if not ingestion_id:
                break

            for batch in batches[ingestion_id]:
                if batch["status"] == "yet_to_start":
                    # Enforce rate limit: 1 batch per 5 sec
                    now = time.time()
                    wait_time = 5 - (now - last_processed_time)
                    if wait_time > 0:
                        await asyncio.sleep(wait_time)
                    await process_batch(batch, ingestion_id)
                    last_processed_time = time.time()
