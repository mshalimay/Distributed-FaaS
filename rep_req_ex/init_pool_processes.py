import multiprocessing

# Obs on using multiprocessing.Pool and when there are shared variables:
# Make the functions executed by the processes in the pool and shared variables 
# global scope if you are using OOP design. If class attributes are passed along 
# with the functions for the processes to execute when there are shared variables, 
# such as 'multiprocessing.Value' and 'multiprocessing.Lock', will give error. 
# Also, use a initializer for the pool as below
# See this issue in:
# https://stackoverflow.com/questions/69907453/lock-objects-should-only-be-shared-between-processes-through-inheritance

# Two alternatives:
 # 1) "manually" create the pool of processes; but requirements asks us to use multiprocessing.Pool
    # This alternative also gives more flexibility on how to check which processes are busy
 # 2) let go of OOP design and create a module with functions only

# TODO: see if there is a way to make this work inside these functions inside the super class


def init_pool_processes(a_lock, a_value):
    global lock
    lock = a_lock
    global busy_workers
    busy_workers = a_value 


# before launching jobs on the pool, initialize pool of processes as:
lock = multiprocessing.Lock()
busy_workers = multiprocessing.Value('i', 0)
pool = multiprocessing.Pool(num_workers, initializer=init_pool_processes, initargs=(lock, busy_workers))

