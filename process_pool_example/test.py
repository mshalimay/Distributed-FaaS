from multiprocessing import Lock, Pool, Value
import time
import redis

def init_pool_processes(the_lock, the_value):
    '''Initialize each process with a global variable lock.
    '''
    global lock
    lock = the_lock

    global busy_workers
    busy_workers = the_value 


class LocalServer:
    def __init__(self, num_workers):
        #self.busy_workers = mp.Value('i', 0)  # 'i' for integer
        #self.lock = mp.Lock()
        self.r = redis.Redis(host='localhost', port=6379, db=0)
        self.subscriber = self.r.pubsub()
        self.subscriber.subscribe('tasks')
        self.num_workers = num_workers
        self.results = []
    
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

    def anotherfunction(self):
        lock = Lock()
        busy_workers = Value('i', 0)
        pool = Pool(initializer=init_pool_processes, initargs=(lock,busy_workers))
        result = pool.apply_async(self.task, (10,))
        print(result.get())



if __name__ == '__main__':
    t = LocalServer(10)
    t.anotherfunction()