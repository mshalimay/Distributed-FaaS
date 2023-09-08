import multiprocessing as mp
import zmq
import uuid
import time
from helper_functions import serialize, deserialize, execute_fn
import argparse
from collections import deque



class PullWorker():
    def __init__(self, num_processes:int, dispatcher_url:str, delay:float=0.1):
        self.num_processes = num_processes
        self.dispatcher_url = dispatcher_url
        self.delay = delay
        self.busy_workers = 0
        self.results = deque()
        self.id = str(uuid.uuid4()).encode('utf-8')

    def connect(self):
        self.socket = zmq.Context().socket(zmq.REQ)
        self.socket.connect(self.dispatcher_url)

    def print_status(self, task_id, status):
        print(f"Tried to executed {task_id}. Status {status}")
        
    def register(self):
        # register with the dispatcher
        message_to_dispatcher = {
            "type": "register",
            "data": {
                "worker_id": self.id
            }
        }
        self.send_message(message_to_dispatcher)

    def receive_message(self) -> object:
        return deserialize(self.socket.recv_string())
        
    def send_message(self, message):
        self.socket.send_string(serialize(message))

    def pop_result(self):
        for _ in range(len(self.results)):
            result = self.results.popleft()
            if result.ready():
                return result
            else:
                self.results.append(result)
        return None
    
    def start(self):       
        # create ZMQ poller and register socket
        poller = zmq.Poller()
        poller.register(self.socket, zmq.POLLIN)

        # create pool of processes
        pool = mp.Pool(self.num_processes)

        # send register message to dispatcher
        self.register()
        while True:
            time.sleep(self.delay)
            sockets = dict(poller.poll())
            if self.socket in sockets:
                message = self.receive_message()
                #---------------------------------------------------------------
                # Process received message
                #---------------------------------------------------------------
                if message['type'] == 'wait':
                    pass

                elif message['type'] == 'task' and self.busy_workers < self.num_processes:
                    task_id = message['data']['task_id']
                    fn_payload = message['data']['fn_payload']
                    param_payload = message['data']['param_payload']

                    result = pool.apply_async(execute_fn, args=(task_id, fn_payload, param_payload))
                    self.results.append(result)
                    
                    self.busy_workers += 1

            # check if any results are ready
            # TODO add a dispatch of work inside the for loop to improve performance
            result = self.pop_result()

            if result is not None:
                task_id, status, fn_result = result.get()
                message_to_dispatcher = {
                        "type": "result",
                        "data":{
                            "task_id": task_id,
                            "status": status,
                            "result": fn_result
                            }
                        }
                #self.print_status(task_id, status)
                self.busy_workers -= 1
                self.send_message(message_to_dispatcher)                
            else:
                if self.busy_workers < self.num_processes:
                    message_to_dispatcher = {"type": "ready"}
                    self.send_message(message_to_dispatcher) 
            
if __name__ == "__main__":
    # # instantiate argument parser
    parser = argparse.ArgumentParser()
    parser.add_argument("num_worker_processors", help="number of worker processors", type=int)
    parser.add_argument("dispatcher_url", help="the URL of the task dispatcher", type = str)
    parser.add_argument("--delay", help="seconds to wait to send another message to dispatcher",
                        default=0.1, type = float)
    
    args = parser.parse_args()
    num_processes = args.num_worker_processors
    dispatcher_url = args.dispatcher_url
    delay = args.delay

    # # for debugging   
    # args = argparse.Namespace()
    # args.num_worker_processors = 5
    # args.dispatcher_url = "tcp://0.0.0.0:8000"
    # args.delay = 0.1
    
    worker = PullWorker(args.num_worker_processors, args.dispatcher_url, args.delay)
    worker.connect()
    worker.start()           
