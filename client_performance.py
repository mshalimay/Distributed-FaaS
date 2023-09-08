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
    # return sum([i**2 for i in range(n)])
    sum = 0
    for i in range(n):
        sum += i**2

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

# ==========================================================================
# service measurement
# Measure service throughput and average latency for a given function
# ==========================================================================
def measure_service(fn_obj, fn_params:list):

    # register a function to test performance
    resp = requests.post(base_url + "register_function",
                            json={"name": fn_obj.__name__,
                                "payload": serialize(fn_obj)})
    fn_id = resp.json()['function_id']

    # register tasks for execution
    n_tasks = len(fn_params)
    tasks = deque()
    t_register_0 = time.time()
    for i in range(n_tasks):
        resp = requests.post(base_url + "execute_function",
                            json={"function_id":fn_id,
                                "payload": serialize(fn_params[i])})

        tasks.append((resp.json()['task_id'], fn_params[i], time.time()))
    t_register_1 = time.time()
    
    # loop while all tasks are completed
    t0 = time.time()
    latencies = []
    while len(tasks)>0:
        for _ in range(len(tasks)):
            task = tasks.popleft()
            resp = requests.get(f"{base_url}result/{task[0]}")

            status = resp.json()['status']
                    
            if status == "COMPLETED":                   
                # result = deserialize(resp.json()['result'])
                # assert result == fn_obj(*task[1][0], **task[1][1])
                latencies.append(time.time()-task[2])
                continue
            elif status == "FAILED":
                assert False
            else:
                tasks.append(task)
    t1 = time.time()
    
    throughput = (n_tasks/(t1-t0))
    average_latency = sum(latencies)/len(latencies)
    time_to_register = t_register_1-t_register_0
    
    return throughput, average_latency, time_to_register
        

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
    parser.add_argument('-d', type=float, help='Delay for pull worker.', dest='delay', default=0.01)
    parser.add_argument('-t', type=int, help='Number of tasks to deploy.', dest='n_tasks', default = 10)
    parser.add_argument('-np', type=int, help='Number of processes per worker.', dest='number_processes', default = 4)
    parser.add_argument("-ns", type = int, help = "Number of simulations to run", dest = "n_simulations", default = 5)
    parser.add_argument("--hb", action="store_true", 
                        help="Run PUSH dispatcher in heartbeat mode")
    parser.add_argument("--plb", action="store_true", 
                        help="Run PUSH dispatcher with process balance loading")
    parser.add_argument("-equivalent", type=int, default = 1, help = "For local mode only. \
                        Run local benchmark for the '--equivalent' number push/pull workers.")

                        
    args = parser.parse_args()

    ## -------------------------------------------------------
    ## for debugging ---------------------------------------
    ##--------------------------------------------------------
    # args = argparse.Namespace()
    # # number of times to simulate
    # args.n_simulations = 3
    # # number of tasks to deploy
    # args.n_tasks = 10

    # # dispatcher parameters
    # args.port = 9000
    # args.mode = 'local'
    # args.delay = 0

    # # worker parameters
    # args.number_processes = 4
    # args.number_workers = 4
    # args.nh = False
    # args.equivalent = 1
    ##-------------------------------------------------------

    dispatcher_url = f"tcp://{DISPATCHER_IP}:{args.port}"
    # create commands to initialize dispatcher and workers
    if args.mode == "pull":
        task_disp_command = f"python task_dispatcher.py -p {args.port} -m pull"
        worker_command = f"python pull_worker.py {args.number_processes} {dispatcher_url} --delay {args.delay}"

    elif args.mode == "push":
        if args.hb:
            task_disp_command = f"python task_dispatcher.py -p {args.port} -m push --hb"
            worker_command = f"python push_worker.py {args.number_processes} {dispatcher_url} --hb"
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
    print(f"Problem size: {problem_size}")

    #==========================================================================
    ## (!) CHOOSE A FUNCTION TO MEASURE
    #==========================================================================
    # ## immediate fun
    # function = immediate_function
    # fn_params = params_immediate_function(problem_size)

    ## arithmetic fun
    function = arithmetic_function
    fn_params = params_arithmetic_function(problem_size, 10000, 10000)

    ## slow function
    # function = slow_function
    # fn_params = params_slow_function(problem_size, 5, 5)

    # sort function
    # function = sort_function
    # fn_params = params_sort_number_function(problem_size, 100000, 100000, -10000, 10000)

    ## reverse string function
    #function = reverse_string
    #fn_params = params_reverse_string(problem_size, 1, 10)
    
    #==========================================================================
     # Start subprocesses for dispatcher/workers and measure service.
     # repeat n_simulations times.
    #==========================================================================
    throughputs = []
    latencies = []
    times_to_register = []
    for i in range(args.n_simulations):
        print(f"\nSimulation {i+1} of {args.n_simulations}")
        r.flushdb()
        # start dispatcher and workers
        popen_processes = []
        popen_processes.append(subprocess.Popen(task_disp_command.split()))
        time.sleep(1)

        if not args.mode=="local":
            for _ in range(args.number_workers):
                popen_processes.append(subprocess.Popen(worker_command.split())),
                time.sleep(1)

        # calculate average time just to register function and tasks
        #print(measure_service(function, fn_params, n_simulations, only_register=True))
                
        # measure service
        throughput, latency, time_to_register = measure_service(function, fn_params)
        throughputs.append(throughput)
        latencies.append(latency)
        times_to_register.append(time_to_register)

        print(throughput); print(latency); print(time_to_register)

        task_disp = popen_processes.pop(0)
        for process in popen_processes:
            process.kill()
            process.wait()
            
        task_disp.kill()
        task_disp.wait()
        time.sleep(1)

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

    # write results into new line in results.txt
    # with open(f"results.txt", "a") as f:
    #     f.write(f"{args.mode},{args.number_workers},{median_throughput},{1000*median_latency},{1000*median_time_to_register}\n")
    
