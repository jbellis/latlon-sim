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

def query_one(_):
    min_lat, max_lat = 34.5, 71.2
    min_lng, max_lng = -31.3, 40.2

    lat_spread = 0.3
    lng_spread = 0.08
    lat_min = random.uniform(min_lat + lat_spread/2, max_lat - lat_spread/2)
    lat_max = random.uniform(lat_min, lat_min + lat_spread)
    lng_min = random.uniform(min_lng + lng_spread/2, max_lng - lng_spread/2)
    lng_max = random.uniform(lng_min, lng_spread)
    vector = [round(random.uniform(-1.0, 1.0), 9) for _ in range(1536)]

    db = get_db_handle()
    db.query(lat_min, lat_max, lng_min, lng_max, vector)

def main():
    print("Waiting for Cassandra schema")
    get_db_handle() # let one thread create the table + index
    time.sleep(1)

    print("Querying data")
    num_threads = 64
    n_rows = 100_000
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        list(tqdm(executor.map(query_one, range(n_rows)), total=n_rows))

if __name__ == '__main__':
    main()
