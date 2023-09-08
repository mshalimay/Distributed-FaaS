import requests
from helper_functions import serialize, deserialize
import random
import time
import string
import redis
import subprocess
import argparse
from collections import deque
import numpy as np

base_url = "http://127.0.0.1:8000/"

DISPATCHER_IP = "0.0.0.0"

#-------------------------------------------------------------------------------
# noop function and parameters to simulate it
#-------------------------------------------------------------------------------
def immediate_function(number):
    return number

def params_immediate_function(n_tasks, number=1):
    params = [((number,), {}) for _ in range(n_tasks)]
    return params

#-------------------------------------------------------------------------------
# Slow function and parameters to simulate it
#-------------------------------------------------------------------------------
def slow_function(sleep_time):
    import time
    time.sleep(sleep_time)

def params_slow_function(n_tasks, min_time, max_time):
    random.seed(1)
    random_nums =  [random.uniform(float(min_time), float(max_time)) for _ in range(n_tasks)]
    params = [((number,), {}) for number in random_nums]
    return params

#-------------------------------------------------------------------------------
# Arithmetic intensity function and parameters to simulate it
#-------------------------------------------------------------------------------
def arithmetic_function(n):
    return sum([i**2 for i in range(n)])

def params_arithmetic_function(n_tasks, min_n, max_n):
    random.seed(1)
    random_nums =  [random.randint(min_n, max_n) for _ in range(n_tasks)]
    params = [((number,), {}) for number in random_nums]
    return params

#-------------------------------------------------------------------------------
# Sorting numbers function and parameters to simulate it
#-------------------------------------------------------------------------------
def sort_function(input_list):
    return sorted(input_list)

def params_sort_number_function(n_tasks, min_n, max_n, min_value, max_value):
    random.seed(1)
    random_lists =  [[random.randint(int(min_value), int(max_value)) \
        for _ in range(random.randint(min_n, max_n))] for _ in range(n_tasks)]
    params = [((list,), {}) for list in random_lists]
    return params

#-------------------------------------------------------------------------------
# Sorting strings function and parameters to simulate it
#-------------------------------------------------------------------------------
def generate_random_string(n):
    word = ''
    for _ in range(n):
        word += random.choice(string.ascii_letters)
    return word

def params_sort_string_function(n_tasks, min_n, max_n, list_size):
    random.seed(1)
    random_lists =  [[generate_random_string(random.randint(min_n, max_n)) \
        for _ in range(list_size)] for _ in range(n_tasks)]
    params = [((list,), {}) for list in random_lists]
    return params

# Reverse strings function and parameters to simulate it
def reverse_string(input_string):
    return input_string[::-1]

def params_reverse_string(n_tasks, min_n, max_n):
    random.seed(1)
    random_lists =  [generate_random_string(random.randint(min_n, max_n)) \
         for _ in range(n_tasks)]
    params = [((list,), {}) for list in random_lists]
    return params

function_mapping = {
    'immediate_function': [immediate_function, params_immediate_function],
    'slow_function': [slow_function, params_slow_function],
    'arithmetic_function': [arithmetic_function, params_arithmetic_function],
    'sort_function': [sort_function, params_sort_number_function],
    'sort_string_function': [sort_function, params_sort_string_function],
    'reverse_string_function': [reverse_string, params_reverse_string],
}

# ==========================================================================
# service measurement
# ==========================================================================

def start_dispatcher(command_dispatcher, dispatcher_delay=0):

    command_dispatcher = command_dispatcher + " -d " + str(dispatcher_delay)
    # start dispatcher
    dispatcher  = subprocess.Popen(command_dispatcher.split(), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    time.sleep(1)
    return dispatcher

def start_workers(worker_command, num_workers):
    popen_processes = []    
    for _ in range(num_workers):
        popen_processes.append(subprocess.Popen(worker_command.split()))
        time.sleep(1)

    return popen_processes

def kill_popen_processes(popen_processes):
    for process in popen_processes:
        process.kill()
        process.wait()
    time.sleep(1)
    

# Measure service throughput and average latency for a given function

def register_tasks(fn_id, fn_params):

    t_register_0 = time.time()
    tasks = deque()
    for i in range(len(fn_params)):
        resp = requests.post(base_url + "execute_function",
                            json={"function_id":fn_id,
                                "payload": serialize(fn_params[i])})
        tasks.append((resp.json()['task_id'], fn_params[i]))
    t_register_1 = time.time()
    return tasks, t_register_1 - t_register_0


def measure_service(fn_obj, fn_params:list, mode, command_dispatcher, worker_command, num_workers,
                    redis_client, n_simulations:int = 1):
    round_trip_avg_times = []
    time_for_all_tasks = []
    throughputs = []
    time_to_register = []
    n_tasks = len(fn_params)

    # register functions in FAAS service
    for _ in range(n_simulations):
        
        # computes a delay to start the dispatcher
        # Explantion: we want the dispatcher to listen the redis channel and get the tasks
        # being published. But to have a cleaner measure of latency to get results back
        # we do not want the workers to start processing until all tasks are there.
        # TODO increment the code to make this work; 
        # have to alter pull worker to only retrieve tasks after a delay
        
        # register a function to test performance
        resp = requests.post(base_url + "register_function",
            json={"name": fn_obj.__name__,"payload": serialize(fn_obj)})
        fn_id = resp.json()['function_id']
        
        _, t_register = register_tasks(fn_id, fn_params)
        redis_client.flushdb()

        # if mode == "local":
        #     dispatcher_delay = t_register + 1.5 # 1 second for the popen subprocess to start
        # else:
        #     dispatcher_delay = t_register + num_workers + 2

        dispatcher_delay = 0
        # register a function to test performance
        resp = requests.post(base_url + "register_function", json={"name": fn_obj.__name__,"payload": serialize(fn_obj)})
        fn_id = resp.json()['function_id']
        
        # start dispatcher
        dispatcher_process = start_dispatcher(command_dispatcher, dispatcher_delay=dispatcher_delay)

        # start workers
        # popen_processes = []
        if mode != "local":
            popen_processes = start_workers(worker_command, num_workers)

        # register tasks for execution
        # obs: if can get delayed starting of worker/dispatcher to work,
        # register tasks before starting workers
        tasks, t_register = register_tasks(fn_id, fn_params)        
        time_to_register.append(t_register)
      
        # # guarantees local dispatcher has started
        # if mode == "local":
        #     while True:
        #         if "local" in dispatcher_process.stdout.readline().decode("utf-8"):
        #             break

        # loop while all tasks are completed
        t0 = time.time()
        while len(tasks)>0:
            for _ in range(len(tasks)):
                task = tasks.popleft()
                resp = requests.get(f"{base_url}result/{task[0]}")

                status = resp.json()['status']
                        
                if status == "COMPLETED":                   
                    result = deserialize(resp.json()['result'])
                    assert result == fn_obj(*task[1][0], **task[1][1])
                    continue
                elif status == "FAILED":
                    assert False
                else:
                    tasks.append(task)
        t1 = time.time()

        # store times
        time_for_all_tasks.append(t1-t0)
        round_trip_avg_times.append((t1-t0)/n_tasks)
        throughputs.append(n_tasks/(t1-t0))
        time_to_register.append(t_register)

        # kill dispatcher and workers
        if mode != "local":
            popen_processes.append(dispatcher_process)
            kill_popen_processes(popen_processes)
        else:
            kill_popen_processes([dispatcher_process])
        
    
    average_throughput = sum(throughputs)/len(throughputs)
    average_latency = sum(round_trip_avg_times)/len(round_trip_avg_times)
    average_time_to_register = sum(time_to_register)/len(time_to_register)
    return average_throughput, average_latency, average_time_to_register
        

# ==========================================================================
# simulation deployment
# ==========================================================================

if __name__ == "__main__":
    # redis to flush db for cleaner comparison
    r = redis.Redis(host='localhost', port=6379, db=1)

    parser = argparse.ArgumentParser(description='Work/TaskDispatcher deployer')

    parser.add_argument('-m',  type=str, choices=['local', 'pull', 'push'], help='The mode deployed.', dest='mode')
    parser.add_argument('-w', type=int, help='Number of workers to deploy. If local mode, number of local processes', dest='number_workers')
    parser.add_argument('-p', type=int, help='Port number for dispatcher.', dest='port', default=9000)
    parser.add_argument('-d', type=float, help='Delay for pull worker.', dest='delay', default=0.1)
    parser.add_argument('-t', type=int, help='Number of tasks to deploy.', dest='n_tasks', default = 10)
    parser.add_argument('-np', type=int, help='Number of processes per worker.', dest='number_processes', default = 4)
    parser.add_argument("-ns", type = int, help = "Number of simulations to run", dest = "n_simulations", default = 10)
    parser.add_argument("--hb", action="store_true", 
                        help="Run PUSH dispatcher in heartbeat mode")
    parser.add_argument("--plb", action="store_true", 
                        help="Run PUSH dispatcher with process balance loading")
    parser.add_argument("--equivalent", type=int, default = 1, help = "For local mode only. \
                        Run local benchmark for the '--equivalent' number push/pull workers.")

                        
    args = parser.parse_args()

    ## debugging ---------------------------
    # args = argparse.Namespace()
    # # number of times to simulate
    # args.n_simulations = 3
    # # number of tasks to deploy
    # args.n_tasks = 10

    # # dispatcher parameters
    # args.port = 9000
    # args.mode = 'local'
    # args.delay = 0.1

    # # worker parameters
    # args.number_processes = 4
    # args.number_workers = 4
    # args.h = False
    # args.plb = False
    # args.equivalent = 1
    ##--------------------------------------
    
    dispatcher_url = f"tcp://{DISPATCHER_IP}:{args.port}"
    
    # create commands to initialize dispatcher and workers
    worker_command = ""
    if args.mode == "pull":
        task_disp_command = f"python task_dispatcher.py -p {args.port} -m pull"
        worker_command = f"python pull_worker.py {args.number_processes} {dispatcher_url} --delay {args.delay}"
    elif args.mode == "push":
        if args.hb:
            task_disp_command = f"python task_dispatcher.py -p {args.port} -m push --h"
            worker_command = f"python push_worker.py {args.number_processes} {dispatcher_url} --h"
        else:
            if args.plb:
                task_disp_command = f"python task_dispatcher.py -p {args.port} -m push --plb"
            else:
                task_disp_command = f"python task_dispatcher.py -p {args.port} -m push"                
            worker_command = f"python push_worker.py {args.number_processes} {dispatcher_url}"
    elif args.mode == "local":
        if args.equivalent>1:
            args.number_workers = args.number_processes * args.equivalent           
        task_disp_command = f"python task_dispatcher.py -p {args.port} -m local -w {args.number_workers}"

    
    problem_size = args.n_tasks * args.number_workers if args.mode != "local" else args.n_tasks * args.equivalent

    ## function to deploy; choose functions defined in this file

    ## immediate fun
    #function = immediate_function
    #fn_params = params_immediate_function(problem_size)

    ## arithmetic fun
    function = arithmetic_function
    fn_params = params_arithmetic_function(problem_size, 100, 100)

    ## slow function
    #function = slow_function
    #fn_params = params_slow_function(problem_size, 1, 10)

    ## reverse string function
    #function = reverse_string
    #fn_params = params_reverse_string(problem_size, 1, 10)

    # start subprocesses for dispatcher and workers and execute measurement function
    # do this n_simulations times
    throughputs = []
    latencies = []
    times_to_register = []

    for i in range(args.n_simulations):
        print(f"Simulation {i+1} of {args.n_simulations}")
        r.flushdb()
        
        # calculate average time just to register function and tasks
        #print(measure_service(function, fn_params, n_simulations, only_register=True))
                
        # measure service
        throughput, latency, time_to_register = measure_service(function, fn_params, args.mode, 
                                              task_disp_command, worker_command, args.number_workers, r) 

        # print to see distribution
        print(throughput); print(latency); print(time_to_register)
        
        throughputs.append(throughput)
        latencies.append(latency)
        times_to_register.append(time_to_register)

    # compute median throughput and latency over all simulations    
    median_throughput = np.median(throughputs)
    median_latency = np.median(latencies)
    median_time_to_register = np.median(times_to_register)
    
    # print results
    print("===========================================")
    print("Simulation results")
    print("===========================================")
    print("Mode:", args.mode)
    print("Number of simulations:", args.n_simulations)
    print("Number of tasks:", args.n_tasks)
    print("Problem size:", problem_size)
    print("Number of workers:", args.number_workers)
    if args.mode != "local":
        print("Number of processes per worker:", args.number_processes)
    print(f"Median throughput: {median_throughput} tasks/s")
    print(f"Median latency: {1000*median_latency} ns")
    print(f"Median time to register: {1000*median_time_to_register} ns")

   