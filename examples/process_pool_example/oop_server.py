import multiprocessing as mp
import redis
import time


def init_pool_processes(a_lock, a_value):
    global lock
    lock = a_lock
    global busy_workers
    busy_workers = a_value 

def task(task_time):
        with lock:
            busy_workers.value += 1
            
        print(f"Processing task; ETF {task_time} seconds")

        # some task
        time.sleep(task_time)
        result = "Finished"

        # after task is finished, update the number of busy workers
        with lock:
            busy_workers.value -= 1
        
        return result

class LocalServer:
    def __init__(self, num_workers):
        self.r = redis.Redis(host='localhost', port=6379, db=0)
        self.subscriber = self.r.pubsub()
        self.subscriber.subscribe('tasks')
        self.num_workers = num_workers
        self.results = []

    def start(self):
        lock = mp.Lock()
        busy_workers = mp.Value('i', 0)
        pool = mp.Pool(initializer=init_pool_processes, initargs=(lock, busy_workers))

        busy_=0
        while True:
            if busy_workers.value < self.num_workers:
                message = self.subscriber.get_message()
                if message is not None and message['type'] == 'message':
                    task_time = int(message['data'].decode('utf-8'))
                    result = pool.apply_async(task, args=(task_time,))
                    self.results.append(result)

            for result in self.results:
                if result.ready():
                    print(result.get())
                    self.results.remove(result)

            if busy_workers.value != busy_:
                print(f"Busy workers: {busy_workers.value}")
                busy_ = busy_workers.value
            
if __name__ == "__main__":
    server = LocalServer(num_workers=3)
    server.start()




