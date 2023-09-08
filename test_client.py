import requests
from helper_functions import serialize, deserialize
import random
import time
import string
import redis
import subprocess
from collections import deque
import psutil


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

#-------------------------------------------------------------------------------
# Reverse strings function and parameters to simulate it
#-------------------------------------------------------------------------------
def reverse_string(input_string):
    return input_string[::-1]

def params_reverse_string(n_tasks, min_n, max_n):
    random.seed(1)
    random_lists =  [generate_random_string(random.randint(min_n, max_n)) \
         for _ in range(n_tasks)]
    params = [((list,), {}) for list in random_lists]
    return params

# makes a round trip for the service for a specified payload function and parameters
# if any result is wrong, returns False
def service_test(fn_obj, fn_params:list, n_simulations:int = 1):
    for _ in range(n_simulations):
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

            tasks.append((resp.json()['task_id'], fn_params[i]))

        # loop while all tasks are completed
        while len(tasks)>0:
            for _ in range(len(tasks)):
                task = tasks.popleft()
                resp = requests.get(f"{base_url}result/{task[0]}")
                status = resp.json()['status']

                if status == "COMPLETED":                   
                    result = deserialize(resp.json()['result'])
                    if result != fn_obj(*task[1][0], **task[1][1]):
                        return False
                elif status == "FAILED":
                    return False
                else:
                    tasks.append(task)
        return True

# aux for testing: start popen subprocesses for dispatcher and workers
#                  and execute the round trip function
def aux_test_service(mode, number_processes, port, dispatcher_url, number_workers,
                           fn_object, fn_params,
                           delay=0.1, hb=False, plb=False):

    # create command to deploy dispatcher and workers
    if mode == "pull":
        task_disp_command = f"python task_dispatcher.py -p {port} -m pull"
        worker_command = f"python pull_worker.py {number_processes} {dispatcher_url} --delay {delay}"

    elif mode == "push":
        if hb:
            task_disp_command = f"python task_dispatcher.py -p {port} -m push --h"
            worker_command = f"python push_worker.py {number_processes} {dispatcher_url} --h"

        else:
            if plb:
                task_disp_command = f"python task_dispatcher.py -p {port} -m push --plb"
            else:
                task_disp_command = f"python task_dispatcher.py -p {port} -m push"
                
            worker_command = f"python push_worker.py {number_processes} {dispatcher_url}"
            
    elif mode == "local":   
        task_disp_command = f"python task_dispatcher.py -p {port} -m local -w {number_workers}"

    # start popen subprocesses for dispatcher and workers
    popen_processes = []
    popen_processes.append(subprocess.Popen(task_disp_command.split()))
    time.sleep(1)

    if not mode=="local":
        for _ in range(number_workers):
            popen_processes.append(subprocess.Popen(worker_command.split())),
            time.sleep(1)

    # execute round trips
    test_result = service_test(fn_object, fn_params)

    # kill subprocesses
    for process in popen_processes:
        process.kill()
        process.wait()

    return test_result
#===============================================================================
# Tests
#===============================================================================
r = redis.Redis(host='localhost', port=6379, db=1)
port = 9002
dispatcher_url = f"tcp://{DISPATCHER_IP}:{port}"


def test_pull():
    mode = "pull"
    number_processes = 4
    number_workers = 4
    n_tasks = 5
    fn_object = arithmetic_function
    problem_size = n_tasks * number_workers
    fn_params = params_arithmetic_function(problem_size, 100, 100)
    
    assert aux_test_service(mode, number_processes, port, dispatcher_url, number_workers,
                            fn_object, fn_params) == True

def test_push():
    mode = "push"
    number_processes = 4
    number_workers = 4
    n_tasks = 5
    fn_object = arithmetic_function
    problem_size = n_tasks * number_workers
    fn_params = params_arithmetic_function(problem_size, 100, 100)
    
    assert aux_test_service(mode, number_processes, port, dispatcher_url, number_workers,
                            fn_object, fn_params) == True

def test_local():
    mode = "local"
    number_processes = 4
    number_workers = 4
    n_tasks = 5
    fn_object = arithmetic_function
    problem_size = n_tasks * number_workers
    fn_params = params_arithmetic_function(problem_size, 100, 100)
    
    assert aux_test_service(mode, number_processes, port, dispatcher_url, number_workers,
                            fn_object, fn_params) == True