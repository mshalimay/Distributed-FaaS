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

# register dealer
socket = zmq.Context().socket(zmq.DEALER)
#socket.setsockopt(zmq.IDENTITY, str(uuid.uuid4()).encode('utf-8'))
socket.connect(f"tcp://{IP_ADDRESS}:{PORT}")
socket.send(dill.dumps({"type": "register"}))

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
    if busy_workers < num_processes:
        socket.send(dill.dumps({"type": "ready"}))
        busy_workers += 1
        
    sockets = dict(poller.poll(0))
    if socket in sockets:
        message = dill.loads(socket.recv())

        task_time = int(message['data']['payload'])
        task_id = message['data']['task_id']

        results.append(pool.apply_async(task, args=(task_id, task_time)))

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
            
