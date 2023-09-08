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
router = zmq.Context().socket(zmq.ROUTER)
#router.setsockopt(zmq.IDENTITY, str(uuid.uuid4()).encode('utf-8'))

router.bind("tcp://0.0.0.0:8001")
poller = zmq.Poller()
poller.register(router, zmq.POLLIN)

# aux vars
workers = []
free_workers = deque()

task_id = 1000

while True:
    # TODO evaluate if can gain by having code to retrieve items 
    # from REDIS channel while workers are occupied. See router_taskqueue.py
    
    sockets = dict(poller.poll(0))
    if router in sockets:
        worker, message = router.recv_multipart()
        #worker = worker.decode('utf-8')
        message = dill.loads(message)

        if message['type'] == 'register':
            workers.append(worker)
            #free_workers.append(worker)

        if message['type'] == 'ready':
            free_workers.append(worker)

        elif message['type'] == 'result':
            data = message['data']
            task_id, status, result = data['task_id'], data['status'], data['result']
            #redis_client.hset(task_id, mapping={"status":status, "result":result})
            print(f"Task {task_id} finished with status {status} and result {result}")
            free_workers.append(worker)

    if free_workers:
        message = subscriber.get_message()
        if message is not None and message['type'] == 'message':
            # Extract data from RedisDB and send to worker
            data = int(message['data'])
            task_id += 1
            message_to_worker = {
                "type": "task",
                "data": {
                        "task_id": task_id,
                        "payload":data
                    }
            }
            worker = free_workers.popleft()
            router.send_multipart([worker, dill.dumps(message_to_worker)])

        

        
        