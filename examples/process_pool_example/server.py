import multiprocessing as mp
import redis
import time



def task(task_time, busy_workers, lock):
    with lock:
        busy_workers.value += 1
        
    print(f"Processing task; ETF {task_time} seconds")

    # some task
    time.sleep(task_time)
    result = "Finished"

    # after task is finished, update the number of busy workers
    with lock:
        busy_workers.value -= 1
    
    return "Finished"

r =  redis.Redis(host='localhost', port=6379, db=0)
subscriber = r.pubsub()
subscriber.subscribe('tasks')

num_workers = 3
pool = mp.Pool(processes=num_workers)

results = []
manager = mp.Manager()
busy_workers = manager.Value('i', 0)
lock = manager.Lock()

busy_ = 0
while True:
    if busy_workers.value < num_workers:
        message = subscriber.get_message()
        if message is not None and message['type'] == 'message':
            task_time = int(message['data'].decode('utf-8'))
            result = pool.apply_async(task, args=(task_time, busy_workers, lock))
            results.append(result)

    for result in results:
        if result.ready():
            print(result.get())
            results.remove(result)

    if busy_workers.value != busy_:
        print(f"Busy workers: {busy_workers.value}")
        busy_ = busy_workers.value
    





