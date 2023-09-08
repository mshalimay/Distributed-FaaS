import zmq
import redis
import multiprocessing
from collections import deque, OrderedDict
from helper_functions import serialize, deserialize, execute_fn
import argparse
import sys
import configparser
import os
import time

# -----------------------------------------------------------------------------
# Get configuration
config = configparser.ConfigParser()
os.chdir(os.path.dirname(os.path.abspath(__file__)))
config.read('config.ini')

ip_address = config.get('dispatcher', 'IP_ADDRESS')
tasks_channel = config.get('redis', 'TASKS_CHANNEL')
time_to_expire = config.getint('dispatcher', 'TIME_TO_EXPIRE')

# -----------------------------------------------------------------------------


# TaskDispatcher Super Class
class TaskDispatcher():
    """Super class for task dispatchers"""
    
    def __init__(self):
        # redis client for querying tasks from database and storing results
        self.redis_client = redis.Redis(host='localhost', port=6379, db=1)

        # redis pub-sub for listening to new tasks in the 'tasks' channel
        self.subscriber = self.redis_client.pubsub()
        self.subscriber.subscribe(tasks_channel)        

    def query_redis(self, message) -> tuple:
        """Query redis for serialized function and parameters given redis pub-sub message
        Args:
            message: Redis pub-sub message object
        Returns:
            task_id (str): the task id
            fn_payload (str): body of the function serialized
            param_payload (str): function parameters serialized
        """

        task_id = message['data'].decode('utf-8')        
        # retrieve serialized function and parameters
        fn_payload = self.redis_client.hget(task_id, "fn_payload")
        param_payload = self.redis_client.hget(task_id, "param_payload")
        return task_id, fn_payload.decode('utf-8'), param_payload.decode('utf-8')

#-----------------------------------------------------------------------------
# Obs: not the case for this version, but useful if using multiprocessing.Pool
# w/ shared variables and OOP design: make functions executed in the pool global 
# and use a initializer for the pool. See init_pool_processes.py for example and more details
#-----------------------------------------------------------------------------
class LocalDispatcher(TaskDispatcher):
    def __init__(self, num_workers):
        super().__init__()
        self.num_workers = num_workers
        self.busy_workers = 0
        self.results = deque()

    def start(self):
        """Deploy the local dispatcher"""

        # create multiprocessing pool of workers
        pool = multiprocessing.Pool(self.num_workers)

        while True:
            # if available workers, look for tasks in redis pub-sub
            if self.busy_workers < self.num_workers:
                message = self.subscriber.get_message()

                # if task found, get data from redis-db and execute function
                if message is not None and message['type'] == 'message':
                    # query redis for serialized function and parameters
                    task_id, fn_payload, param_payload = self.query_redis(message)
                    
                    # execute function asynchronously and update busy workers
                    result = pool.apply_async(execute_fn, args=(task_id, fn_payload, param_payload))
                    self.results.append(result)
                    self.redis_client.hset(task_id, mapping={"status":"RUNNING"})
                    self.busy_workers += 1

            # check if any results are ready and update redis-db
            # Obs: queue popleft and append avoid possibly additional O(n) operation
            # to remove result (such as list.remove())
            for _ in range(len(self.results)):
                result = self.results.popleft()
                if result.ready():
                    # if result is ready, update redis-db and make a worker free
                    task_id, status, result = result.get()
                    self.redis_client.hset(task_id, mapping={"status":status, "result":result})
                    self.busy_workers -= 1
                    
                    # if status=="COMPLETED":
                    #     print(f"task {task_id} completed sucessfully" )
                else:
                    # if result is not ready, append it back to the queue
                    self.results.append(result)

class PullDispatcher(TaskDispatcher):   
    def __init__(self, ip_address, port):
        super().__init__()
        self.port = port
        self.ip_address = ip_address
        self.workers = []
        self.poller = zmq.Poller()

        # bind to socket and register it with ZMQ.poller to listen to messages
        # put here instead of start() for better performance comparison with local dispatcher
        self.bind_socket()
        self.poller.register(self.socket, zmq.POLLIN)
        
    def bind_socket(self):
        """ Create and bind to ZMQ REP socket
        """
        self.socket = zmq.Context().socket(zmq.REP)
        self.socket.bind(f"tcp://{self.ip_address}:{self.port}")
 
    def send_message(self, message):
        """Serialize message object and send to client
        Args:
            message (any): a Python object to be sent to client
        """
        self.socket.send_string(serialize(message))

    def receive_message(self) -> object:
        """Receive message, deserialize and return it
        """
        # note: recv.decode('utf-8') is equivalent to recv_string(), which is
        # not working in my python when used with socket.send_string
        return deserialize(self.socket.recv().decode('utf-8'))
    
    def start(self):
        while True:
            # listen for incoming messages from workers
            sockets = dict(self.poller.poll())

            # if message received, parse it and send message back
            if self.socket in sockets:
                #---------------------------------------------------------------
                # Parse message received from worker
                #---------------------------------------------------------------                
                worker_message = self.receive_message()
                                    
                if worker_message['type'] == "register":
                    self.workers.append(worker_message['data']['worker_id'])

                elif worker_message['type'] == "result":
                    data = worker_message['data']
                    task_id, status, result = data['task_id'], data['status'], data['result']
                    self.redis_client.hset(task_id, mapping={"status":status, "result":result})

                # elif worker_message['type'] == 'ready'
                
                #---------------------------------------------------------------
                # Send message back to worker
                #---------------------------------------------------------------
                # send a message back either to 'wait' or to work; 
                # Obs: msgs must always be sent back, due to sync nature of REP-REQ.
                # Obs: once msg is received, whatever the type, the worker is
                #      classified as ready for task and a task is sent (if there is one). 
                # This way a REP-REQ cycle is not wasted.

                # if redis pub-sub has tasks, send it to worker; otherwise, tell to wait
                redis_message = self.subscriber.get_message()
                if redis_message is not None and redis_message['type'] == 'message':
                    # Extract data from RedisDB and send to worker
                    task_id, fn_payload, param_payload = self.query_redis(redis_message)
                    message_to_worker = {
                        "type": "task",
                        "data": {
                                "task_id": task_id,
                                "fn_payload": fn_payload,
                                "param_payload": param_payload
                            }
                        }
                    self.redis_client.hset(task_id, mapping={"status":"RUNNING"})
                else:
                    message_to_worker = {"type": "wait"}

                # send wait/task message to worker
                self.send_message(message_to_worker)

class PushDispatcher(TaskDispatcher):   
    def __init__(self, ip_address, port, time_to_expire=time_to_expire):
        super().__init__()
        self.port = port
        self.ip_address = ip_address
        self.workers = {}
        self.free_workers = OrderedDict()
        self.time_to_expire = time_to_expire

        # bind to socket and register it with ZMQ.poller to listen to messages
        # put here instead of start() for better performance comparison with local dispatcher
        self.bind_socket()
        self.poller = zmq.Poller()
        self.poller.register(self.socket, zmq.POLLIN)

    class PushWorker():
        def __init__(self, time_to_expire):
            self.free_processes = 0 
            self.last_heartbeat = time.time()
            self.time_to_expire = time_to_expire

        def is_alive(self):
            if time.time() - self.last_heartbeat > self.time_to_expire:
                return False
            return True

    
    def bind_socket(self):
        """ Create and bind to ROUTER ZMQ socket
        """
        self.socket = zmq.Context().socket(zmq.ROUTER)
        self.socket.bind(f"tcp://{self.ip_address}:{self.port}")

    def send_message(self, worker_id:bytes, message:object):
        """Serialize message object and send to worker identified by 'worker_id'
        Args:
            worker_id (bytes): ZMQ.ROUTER identifier for connected workers
            message (object): an object to send to workers (e.g: dict with task data)
        """
        
        # note: sending a string.encode('utf-8') is equivalent to socket.send_string()
        # cannot use send_string() directly because have to specify the worker to send to
        self.socket.send_multipart([worker_id, serialize(message).encode('utf-8')])
    
    def receive_message(self) -> tuple:
        """Receive message, deserialize and return it along with sender identifier
        Returns:
            worker (bytes): ZMQ.ROUTER identifier for connected worker that sent the message
            object: a python object deserialized from the message (e.g: dict with results data)
        """
        worker, message = self.socket.recv_multipart()
        return worker, deserialize(message.decode('utf-8'))       

    def purge_workers_oop(self, free_workers):
        remove_ids = []
        for worker_id, worker in self.workers.items():
            if not worker.is_alive():
                remove_ids.append(worker_id)
        for remove_id in remove_ids:
            del self.workers[remove_id]
            if remove_id in free_workers:
                del free_workers[remove_id]

    def purge_workers(self, free_workers):
        remove_ids = []
        for worker_id, worker in self.workers.items():
            if time.time() - worker['last_heartbeat'] > self.time_to_expire:
                remove_ids.append(worker_id)
        for remove_id in remove_ids:
            del self.workers[remove_id]
            if remove_id in free_workers:
                del free_workers[remove_id]


    def start(self):
        """Deploy the push dispatcher
        """
        free_workers = deque()
        while True:
            # listen for incoming messages from workers asynchronously
            sockets = dict(self.poller.poll(0))

            # ---------------------------------------------------------------
            # Parse message received and update list of workers
            #----------------------------------------------------------------
            # A worker "X" can have multiple processes to do tasks.
            # The dispatcher stores separately the worker ID (a socket) in 'workers'
            # and its processes = workers ready to receive tasks, in 'free_workers'.

            # Workers initially communicate threads are available to do work by sending 
            # a 'ready' message. Afterwards, available threads are identified when 
            # a 'result' message is received.
                
            # if message received, parse it and update lists of workers/database
            if self.socket in sockets:
                # Parse message received from worker
                worker_id, worker_message = self.receive_message()

                # if 'register', append worker to in memory list of existing workers
                if worker_message['type'] == 'register':
                    #self.workers[worker_id] = self.PushWorker(self.time_to_expire)
                    #self.workers[worker_id].free_processes = worker_message['data']['num_processes']
                    self.workers[worker_id] = 0
                    self.workers[worker_id] = worker_message['data']['num_processes']
                    
                    if self.workers[worker_id] > 0:
                        free_workers.appendleft(worker_id)

                # if result, update database and append worker process to list of workers ready to receive tasks
                elif worker_message['type'] == 'result':
                    data = worker_message['data']
                    task_id, status, result = data['task_id'], data['status'], data['result']
                    self.redis_client.hset(task_id, mapping={"status":status, "result":result})
                    #self.workers[worker_id].free_processes += 1
                    self.workers[worker_id] += 1
                    if self.workers[worker_id] == 1:
                        free_workers.append(worker_id)    

            # if any available workers, check for tasks in redis PUB-SUB and send to worker
            if free_workers:
                redis_message = self.subscriber.get_message()
                if redis_message is not None and redis_message['type'] == 'message':
                    # Extract data from RedisDB and send to worker
                    task_id, fn_payload, param_payload = self.query_redis(redis_message)
                    message_to_worker = {
                        "type": "task",
                        "data": {
                                "task_id": task_id,
                                "fn_payload": fn_payload,
                                "param_payload": param_payload
                            }
                        }
                    # remove worker from list of available workers and send task
                    worker_id = free_workers.popleft()    
                    self.redis_client.hset(task_id, mapping={"status":"RUNNING"})
                    self.send_message(worker_id, message_to_worker)
                    self.workers[worker_id] -= 1
                    if self.workers[worker_id] > 0:
                        free_workers.append(worker_id)
    
    def start_heartbeat(self):
        """Deploy the push dispatcher
        """
        free_workers = OrderedDict()
        while True:
            # listen for incoming messages from workers asynchronously
            sockets = dict(self.poller.poll(0))

            # ---------------------------------------------------------------
            # Parse message received and update list of workers
            #----------------------------------------------------------------
            # A worker "X" can have multiple processes to do tasks.
            # The dispatcher stores separately the worker ID (a socket) in 'workers'
            # and its processes = workers ready to receive tasks, in 'free_workers'.
            # Workers initially communicate threads are available to do work by sending 
            # a 'ready' message. Afterwards, available threads are identified when 
            # a 'result' message is received.
                
            # if message received, parse it and update lists of workers/database
            if self.socket in sockets:
                # Parse message received from worker
                worker_id, worker_message = self.receive_message()

                if worker_message['type'] == 'register':
                    # self.workers[worker_id] = self.PushWorker(self.time_to_expire)
                    # self.workers[worker_id].free_processes = worker_message['data']['num_processes']
                    self.workers[worker_id] = {"num_processes": 0, "last_heartbeat": time.time()}
                    self.workers[worker_id]["free_processes"] = worker_message['data']['num_processes']
                    
                    if self.workers[worker_id]["free_processes"] > 0:
                        free_workers[worker_id] = None
                        free_workers.move_to_end(worker_id, last=False)

                else:
                    if worker_id not in self.workers:
                        self.workers[worker_id] = {"num_processes": 0, "last_heartbeat": time.time()}
                        self.send_message(worker_id, {"type": "reconnect"})

                    elif worker_message['type'] == 'reconnect':
                        self.workers[worker_id]["last_heartbeat"] = time.time()
                        self.workers[worker_id]["free_processes"] \
                            = worker_message['data']['free_processes']
                        
                        if self.workers[worker_id]["free_processes"] > 0:
                            free_workers[worker_id] = None
                            free_workers.move_to_end(worker_id, last=False)
                        
                    # if 'heartbeat', update worker last heartbeat
                    elif worker_message['type'] == 'heartbeat':
                        self.workers[worker_id]["last_heartbeat"] = time.time()
                        
                    # if result, update database and append worker process to list of workers ready to receive tasks
                    elif worker_message['type'] == 'result':
                        # update number of worker's free processes and last heartbeat
                        self.workers[worker_id]["free_processes"]  += 1
                        self.workers[worker_id]["last_heartbeat"] = time.time()
                        
                        # get result data from worker
                        data = worker_message['data']
                        task_id, status, result = data['task_id'], data['status'], data['result']

                        # update redis database
                        self.redis_client.hset(task_id, mapping={"status":status, "result":result})

                        if self.workers[worker_id]["free_processes"]  == 1:
                            free_workers[worker_id] = None

            # check which workers are alive before submitting tasks
            self.purge_workers(free_workers)
            
            # if available workers, check for tasks in RedisDB and send to LRU worker
            if free_workers:
                redis_message = self.subscriber.get_message()
                if redis_message is not None and redis_message['type'] == 'message':

                    # Extract data from RedisDB and create message to worker
                    task_id, fn_payload, param_payload = self.query_redis(redis_message)
                    message_to_worker = {
                        "type": "task",
                        "data": {
                                "task_id": task_id,
                                "fn_payload": fn_payload,
                                "param_payload": param_payload
                            }
                        }

                    # pop LRU worker from queue and send task
                    worker_id = free_workers.popitem(last=False)[0]   
                    self.send_message(worker_id, message_to_worker)
                    
                    # update redis database
                    self.redis_client.hset(task_id, mapping={"status":"RUNNING"})

                    # update number of worker's free processes
                    self.workers[worker_id]["free_processes"] -= 1
                    # if worker still has free processes, add to end of queue
                    if self.workers[worker_id]["free_processes"] > 0:
                        free_workers[worker_id] = None

        
if __name__ == "__main__":
    #instantiate argument parser
    parser = argparse.ArgumentParser(description='Task Dispatcher')

    # Add the arguments
    parser.add_argument('-m', type=str, choices=['local', 'pull', 'push'],
                        help='The mode to run the task dispatcher')

    parser.add_argument('-p', type=int, required=False, 
                        help='The port number task dispatcher binds to')

    parser.add_argument('-w', type=int, required=False, 
                        help='The number of worker processors to use. For local workers only.')

    parser.add_argument("--nh", action="store_true", 
                        help="Run PUSH dispatcher in non-heartbeat mode")

    parser.add_argument('-d', type=int, required=False, 
                        help='A delay in seconds to wait before starting the dispatcher; \
                            used for performance evaluation.',
                        default = 0)
    
    # Parse the arguments
    args = parser.parse_args()

    # for debugging
    # args = argparse.Namespace()
    # args.m = 'local'
    # args.w = 4
    # args.p = 8001
    # args.d = 10

    if args.m ==  'local':
        if args.w is None:
            print("Error: -w argument is required for local mode")
            parser.print_help()
            sys.exit(0)
        # instantiate dispatcher and start listen to Redis channel
        dispatcher = LocalDispatcher(args.w)

        # start distributing work to workers
        time.sleep(args.d)
        #print("Starting local dispatcher...")
        dispatcher.start()

    else:
        if args.p is None:
            print("Error: -p argument is required for pull/push mode")
            parser.print_help()
            sys.exit(0)

        if args.m == 'pull':
            # instantiate dispatcher and start listen to Redis channel
            dispatcher = PullDispatcher(ip_address, args.p)

            # # start distributing work to workers
            time.sleep(args.d)
            #print("Starting pull dispatcher...")
            dispatcher.start()
        else:
            dispatcher = PushDispatcher(ip_address, args.p)
            time.sleep(args.d)
            #print("Starting push dispatcher...")
            if args.nh:
                dispatcher.start()
            else:
                dispatcher.start_heartbeat()

