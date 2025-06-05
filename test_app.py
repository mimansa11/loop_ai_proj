import time
import requests

BASE_URL = "http://localhost:8000"

def ingest(payload):
    response = requests.post(f"{BASE_URL}/ingest", json=payload)
    print("Ingest Response:", response.json())
    return response.json()["ingestion_id"]

def check_status(ingestion_id):
    response = requests.get(f"{BASE_URL}/status/" + ingestion_id)
    print(f"Status for {ingestion_id}:", response.json())

if __name__ == "__main__":
    # Send MEDIUM priority first
    medium_id = ingest({
        "ids": [1, 2, 3, 4, 5],
        "priority": "MEDIUM"
    })

    time.sleep(4)

    # Then send HIGH priority while previous still processing
    high_id = ingest({
        "ids": [6, 7, 8, 9],
        "priority": "HIGH"
    })

    # Wait for some processing to happen
    time.sleep(7)

    print("\n--- Status Check After 7 Seconds ---")
    check_status(medium_id)
    check_status(high_id)

    time.sleep(10)
    print("\n--- Final Status Check ---")
    check_status(medium_id)
    check_status(high_id)
