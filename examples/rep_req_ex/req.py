import multiprocessing as mp
import zmq
import uuid
import dill
import time

IP_ADDRESS = "0.0.0.0"
PORT = 8001

def task(task_id, task_time):
    # with lock:
    #     busy_workers.value += 1
        
    print(f"Processing task; ETF {task_time} seconds")

    # some task
    time.sleep(task_time)
    result = task_time

    # after task is finished, update the number of busy workers
    # with lock:
    #     busy_workers.value -= 1
    
    return task_id, task_time

# create REQ socket
socket = zmq.Context().socket(zmq.REQ)

# connect to the dispatcher
socket.connect(f"tcp://{IP_ADDRESS}:{PORT}")


# create unique ID for this worker
worker_id = str(uuid.uuid4()).encode('utf-8')

# register with the dispatcher
message_to_dispatcher = {
    "type": "register",
    "data": {
        "worker_id": worker_id
    }
}
socket.send(dill.dumps(message_to_dispatcher))

# create a poller and register the socket
poller = zmq.Poller()
poller.register(socket, zmq.POLLIN)

# processes
num_processes = 5
lock = mp.Lock()
#busy_workers = mp.Value('i', 0)

pool = mp.Pool(num_processes)
results = []

#manager = mp.Manager()
busy_workers = 0
#busy_workers = manager.Value('i', 0)
#lock = manager.Lock()

while True:   
    time.sleep(0.1)
    sockets = dict(poller.poll(0))
    if socket in sockets:
        message = dill.loads(socket.recv())
        #---------------------------------------------------------------------
        # Process received message
        #---------------------------------------------------------------------
        if message['type'] == 'wait':
            pass

        elif message['type'] == 'task' and busy_workers < num_processes:
            task_time = int(message['data']['payload'])
            task_id = message['data']['task_id']

            result = pool.apply_async(task, args=(task_id, task_time))
            results.append(result)
            busy_workers += 1

    sent_message = 0
    for result in results:
        if result.ready():
            task_id, r = result.get()
            message_to_dispatcher = {
                "type": "result",
                "data":{
                    "task_id": task_id,
                    "status": "Finished",
                    "result": r
                    }
                }
            busy_workers -= 1
            socket.send(dill.dumps(message_to_dispatcher))
            results.remove(result)
            sent_message = 1

            #TODO can gain some time listening to work and dispatching here?
            
            break

    if busy_workers < num_processes and sent_message == 0:
        message_to_dispatcher = {
                "type": "ready",
                }
        socket.send(dill.dumps(message_to_dispatcher))

        
            
