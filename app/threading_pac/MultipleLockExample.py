import threading
import time

from select import select

cache = {}
lock = threading.Lock()


def fetch_user(user_id):
    print(f"Fetching user {user_id} from DB")
    time.sleep(3)
    return {"id": user_id}


def get_user(user_id):
    with lock:
        if user_id in cache:
            return cache[user_id]
    user = fetch_user(user_id)
    with lock:
        cache.setdefault(user_id, user)
    return cache[user_id]


def worker():
    user = get_user("100")
    print(threading.current_thread().name, user)


threads = [
    threading.Thread(target=worker, name=f"T{i}")
    for i in range(3)
]

for t in threads:
    t.start()

for t in threads:
    t.join()
