
import redis
import json
import multiprocessing
import codecs
import dill
import uuid
from queue import Queue


num_workers = 3 

# Create a Redis client
r = redis.Redis(host='localhost', port=6379, db=1)

# Create a Redis subscriber
subscriber = r.pubsub()

# Subscribe to the "tasks" channel
subscriber.subscribe('tasks')

# Task Queue
task_queue = Queue()

#===============================================================================
# Auxiliary functions
#===============================================================================
def serialize(obj) -> str:
    return codecs.encode(dill.dumps(obj), "base64").decode()

def deserialize(obj: str):
    return dill.loads(codecs.decode(obj.encode(), "base64"))

def execute_fn(task_id:uuid.UUID, ser_fn: str, ser_params: str):
    # deserialize function sent by client
    fn = deserialize(ser_fn)

    # deserialize params sent by client
    params = deserialize(ser_params)

    # execute the function
    try:
        result = fn(params)
        status = "COMPLETED"

    except Exception as e:
        result = None
        status = "FAILED"
        
    return task_id, status, serialize(result)

def local_mode_fast():
    # define a worker
    def worker(task_queue):
        while True:
            args = task_queue.get()

            if args is None:
                break
            
            task_id, ser_fn, ser_params = args
            task_id, status, result = execute_fn(task_id, ser_fn, ser_params)

            # update task status and result in redis database
            r.hset(str(task_id), mapping={"status":status, "result":result})

            
    # create the pool of workers
    workers = []
    for _ in range(num_workers):
        worker = multiprocessing.Process(target=worker, args=(task_queue))
        worker.start()
        workers.append(worker)


    # Wait for messages on the "tasks" channel
    for message in subscriber.listen():
        if message['type'] == 'message':

            # Extract the task ID from tasks queue
            # task_id = message['data'].decode('utf-8')
            task_id = uuid.UUID(message['data'].decode('utf-8'))

            # Get the task payload from the Redis database
            data = json.loads(r.get(task_id))

            # retrieve serialized function and parameters
            fn_payload = data["body_payload"]
            param_payload = data["param_payload"]

            task_queue.put((task_id, fn_payload, param_payload))

    # put sentinle value to stop workers if for some reason the loop ends
    for _ in range(num_workers):
        task_queue.put(None)

    # .join() = main function will stall until the worker finishes his job
    for worker in workers:
        worker.join()



def local_mode():
    # define a worker
    def worker(task_queue):
        while True:
            args = task_queue.get()

            if args is None:
                break
            
            task_id, ser_fn, ser_params = args
            task_id, status, result = execute_fn(task_id, ser_fn, ser_params)

            # update task status and result in redis database
            r.hset(str(task_id), mapping={"status":status, "result":result})

            
    # create the pool of workers
    workers = []
    for _ in range(num_workers):
        worker = multiprocessing.Process(target=worker, args=(task_queue))
        worker.start()
        workers.append(worker)


    # if worker is available

    # put sentinle value to stop workers if for some reason the loop ends
    for _ in range(num_workers):
        task_queue.put(None)

    # .join() = main function will stall until the worker finishes his job
    for worker in workers:
        worker.join()
