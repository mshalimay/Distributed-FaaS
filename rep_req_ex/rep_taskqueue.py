import zmq
from collections import deque
import redis
import dill
import uuid

# redis
redis_client = redis.Redis(host='localhost', port=6379, db=0)
subscriber = redis_client.pubsub()
subscriber.subscribe('tasks')

# router
router = zmq.Context().socket(zmq.REP)
#router.setsockopt(zmq.IDENTITY, str(uuid.uuid4()).encode('utf-8'))

router.bind("tcp://0.0.0.0:8001")
# poller = zmq.Poller()
# poller.register(router, zmq.POLLIN)

# aux vars
workers = []
free_workers = deque()

task_id = 1000

task_queue = deque()

max_queue_size = 1000000

while True:
    # get any messages from the Redis channel and enqueue
    redis_message = subscriber.get_message()
    while redis_message is not None and redis_message['type'] == 'message' and len(task_queue) < max_queue_size:
        task_queue.append(redis_message['data'])
        redis_message = subscriber.get_message()
       
    # parse worker message
    worker_message = dill.loads(router.recv_string())

    if worker_message['type'] == 'register':
        workers.append(worker)
        free_workers.append(worker)

    # if worker_message['type'] == 'ready':
    #     free_workers.append(worker)

    elif worker_message['type'] == 'result':
        data = worker_message['data']
        task_id, status, result = data['task_id'], data['status'], data['result']
        #redis_client.hset(task_id, mapping={"status":status, "result":result})
        print(f"Task {task_id} finished with status {status} and result {result}")
        # free_workers.append(worker)
        router.send_string(dill.dumps({"type":"ack"}))

    if free_workers and task_queue:    
        # Extract data from RedisDB and send to worker
        task_id = task_queue.popleft()
        
        data = int(worker_message['data'])
        task_id += 1
        message_to_worker = {
            "type": "task",
            "data": {
                    "task_id": task_id,
                    "payload":data
                }
        }
        worker = free_workers.popleft()
        router.send_string(dill.dumps(message_to_worker))
        