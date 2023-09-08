import multiprocessing as mp
import zmq
from helper_functions import serialize, deserialize, execute_fn
from collections import deque
import argparse
import time

TIME_HEARTBEAT = 1

class PushWorker():
    def __init__(self, num_processes:int, dispatcher_url:str, 
                 time_heartbeat:int = TIME_HEARTBEAT):
        self.num_processes = num_processes
        self.dispatcher_url = dispatcher_url
        self.busy_workers = 0
        self.results = deque()
        self.time_heartbeat = time_heartbeat


    def print_status(self, task_id, status):
        print(f"Tried to executed {task_id}. Status {status}")

    def connect(self):
        self.socket = zmq.Context().socket(zmq.DEALER)
        self.socket.connect(self.dispatcher_url)

    def receive_message(self) -> object:
        return deserialize(self.socket.recv().decode('utf-8'))

    def send_message(self, message):
        self.socket.send_string(serialize(message))

    def register(self):
        # register with the dispatcher
        message_to_dispatcher = {"type": "register", 
                                 "data": {"num_processes": self.num_processes}}
        self.send_message(message_to_dispatcher)

    def get_result(self):
        for result in self.results():
            if result.ready():
                return result
        return None 
            
    def start_heartbeat(self):
        # register and tell router number of processes 
        self.register()

        # create ZMQ poller and register the socket
        poller = zmq.Poller()
        poller.register(self.socket, zmq.POLLIN)

        self.register()

        # create pool of processes
        pool = mp.Pool(self.num_processes)

        last_sent_heartbeat = time.time()

        while True:
            if time.time() - last_sent_heartbeat > self.time_heartbeat:
                self.send_message({"type": "heartbeat"})
                  
            sockets = dict(poller.poll(0))
            if self.socket in sockets:
                message = self.receive_message()

                if message['type'] == "task":
                    task_id = message['data']['task_id']
                    fn_payload = message['data']['fn_payload']
                    param_payload = message['data']['param_payload']

                    result = pool.apply_async(execute_fn, args=(task_id, fn_payload, param_payload))
                    self.results.append(result)

                elif message['type'] == "reconnect":
                    self.send_message({
                        "type": "reconnect", 
                        "data": {
                            "free_processes": self.num_processes - len(self.results)
                        }})
                    continue
                
            for _ in range(len(self.results)):
                result = self.results.popleft()
                if result.ready():
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
                    self.send_message(message_to_dispatcher)
                else:
                    self.results.append(result)

    def start(self):
        # register and tell router number of processes 
        self.register()

        # create ZMQ poller and register the socket
        poller = zmq.Poller()
        poller.register(self.socket, zmq.POLLIN)

        # create pool of processes
        pool = mp.Pool(self.num_processes)

        while True:
            sockets = dict(poller.poll(0))
            if self.socket in sockets:
                message = self.receive_message()

                if message['type'] == "task":
                    task_id = message['data']['task_id']
                    fn_payload = message['data']['fn_payload']
                    param_payload = message['data']['param_payload']

                    result = pool.apply_async(execute_fn, args=(task_id, fn_payload, param_payload))
                    self.results.append(result)
                
            for _ in range(len(self.results)):
                result = self.results.popleft()
                if result.ready():
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
                    self.send_message(message_to_dispatcher)
                else:
                    self.results.append(result)
    

if __name__ == "__main__":
    # instantiate argument parser
    parser = argparse.ArgumentParser()
    parser.add_argument("num_worker_processors", help="number of worker processors", type=int)
    parser.add_argument("dispatcher_url", help="the URL of the task dispatcher", type = str)
    parser.add_argument("--hb", action="store_true", 
                        help="Run in heartbeat mode")

    args = parser.parse_args()

    # # for debugging   
    # args = argparse.Namespace()
    # args.num_worker_processors = 5
    # args.dispatcher_url = "tcp://0.0.0.0:8001"

    if args.hb:
        worker = PushWorker(args.num_worker_processors, args.dispatcher_url)
        worker.connect()
        worker.start_heartbeat()

    else:
        worker = PushWorker(args.num_worker_processors, args.dispatcher_url)
        worker.connect()
        worker.start()

    
         

