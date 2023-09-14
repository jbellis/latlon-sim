import random
import threading
import time
from concurrent.futures import ThreadPoolExecutor

from tqdm.auto import tqdm

from db import DB

thread_local_storage = threading.local()

def get_db_handle():
    if not hasattr(thread_local_storage, 'db_handle'):
        thread_local_storage.db_handle = DB('hornet_profile_vectors_development', 'profile_vectors_by_lat_lng')
    return thread_local_storage.db_handle

def upsert_row(_):
    min_lat, max_lat = 34.5, 71.2
    min_lng, max_lng = -31.3, 40.2

    latitude = random.uniform(min_lat, max_lat)
    longitude = random.uniform(min_lng, max_lng)
    vector = [round(random.uniform(-1.0, 1.0), 9) for _ in range(1536)]
    profile_id = random.randint(0, 90_000_000)
    row = {'profile_id': profile_id, 'embedding': vector, 'lat': latitude, 'lng': longitude }

    db = get_db_handle()
    db.upsert_one(row)

def main():
    print("Waiting for Cassandra schema")
    get_db_handle() # let one thread create the table + index
    time.sleep(1)

    print("Loading data")
    num_threads = 64
    n_rows = 1_000_000
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        list(tqdm(executor.map(upsert_row, range(n_rows)), total=n_rows))

    print('Data load complete')

if __name__ == '__main__':
    main()
