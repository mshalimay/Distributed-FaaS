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
poller = zmq.Poller()
poller.register(router, zmq.POLLIN)

# aux vars
workers = []
free_workers = deque()

task_id = 1000

while True:
    # listen to workers
    sockets = dict(poller.poll())

    if router in sockets:
        #----------------------------------------------------------------------
        # Parse message received from worker
        #----------------------------------------------------------------------
        worker_message = dill.loads(router.recv())
        
        if worker_message['type'] == 'register':
            workers.append(worker_message['data']['worker_id'])

        elif worker_message['type'] == 'result':
            data = worker_message['data']
            task_id, status, result = data['task_id'], data['status'], data['result']
            #redis_client.hset(task_id, mapping={"status":status, "result":result})
            print(f"Task {task_id} finished with status {status} and result {result}")

        # elif worker_message['type'] == 'ready'
        
        #-----------------------------------------------------------------------
        # Send message back to worker
        #-----------------------------------------------------------------------
        # send a message back either to 'wait' or to work; 
        # Obs1: msgs must always be sent back, due to sync nature of REP-REQ
        # Obs2: once msg is received, whatever the type, worker is classified
        # as ready for another task. This way a REP-REQ cycle is not wasted
        
        # if REDIS has tasks, send to worker
        redis_message = subscriber.get_message()
        if redis_message is not None and redis_message['type'] == 'message':
            # Extract data from RedisDB and send to worker
            data = int(redis_message['data'])
            task_id += 1
            message_to_worker = {
                "type": "task",
                "data": {
                        "task_id": task_id,
                        "payload":data
                    }
            }
            router.send(dill.dumps(message_to_worker))

        # if there are no tasks, tell worker to wait
        else:
            message_to_worker = {
                "type": "wait",
            }
            router.send(dill.dumps(message_to_worker))
        