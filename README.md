# Distributed Function-As-A-Service
This repository implements a Function-As-A-Service platform that allow client's to run arbitrary Python functions in a distributed environment of computing nodes. Including:
- REST protocols for client-server communication.
- REP/REQ and ROUTER/DEALER socket patterns for distributed execution.
- Integration of shared database for task registration and execution. 
- Task dispatching with load balancing and heartbeat strategies for failure identification.
- Parallelization at the level of computing nodes and within computing nodes.
- OOP architechture

Below image shows a sketch of the architechture

<img src="https://github.com/mshalimay/distributed-faas/assets/103153803/188b81ce-9549-4a92-a59b-0a645e942fb0" width="450" height="450">

## Simple summary and example: 
Multiple users may specify arbitrary functions, such as below:
```python
def slow_function():
   time.sleep(100)
```
And send to the service using REST protocols.

In summary, the service:
- Serializes functions sent by users and store an associated task in a `REDIS` database.
- Tasks are distributed to multiple computing nodes / workers for parallel execution.
   - Parallelization happens at the level of computing nodes (many nodes executing different tasks) and within a computing node (a worker sapwns subprocesses to execute its tasks).
- Service notifies users their tasks is complete and send results.

# Operation modes
The system can operate in three modes (see `Implementation` sections for more details):
- Push-mode: `TaskDispatcher` keeps track of alive workers and sends tasks to them performing load-balancing
   - Workers that connect to the system send a message to the `TaskDispatcher` when they are ready to work
   - `TaskDispatcher` adds the worker to list of workers and send tasks if there is any
   - `TaskDispatcher` calculates work remaining from all workers and send tasks accordingly to keep the workload balanced
   - Workers and `TaskDispatcher` send hearbeat signals to each other for failure identification.
     - If a worker stops sending heartbeat signals after some time, it is removed from the list and its work is redistributed among the remaining workers
     - A worker can reconnect if it was disconnected due to delays in communication 

- Pull-mode: `TaskDispatcher` and workers interact via REP/REQ sockets.
  - `TaskDispatcher` listen to tasks via a `REP` socket and worker/computing nodes require work as they become free.

- Local-mode: local implementation for benchmarking of network and synchronization bottlenecks.

# Codebase description
- `helper_functions.py`: contains functions common to all scripts
- `push_worker.py`: implements the `PushWorker` class
- `pull_worker.py`: implements the `PullWorker` class
- `task_dispatcher.py`: implements the `TaskDispatcher` class, including the subclasses `LocalDispatcher`, `PullDispatcher` and `PushDispatcher`
    - `TaskDispatcher` is responsible for starting the Redis client and subscribing to the `tasks` channel. It has variables and methods  common for all dispatchers, such as `query_redis`(.)
- `client_performance.py`: script to simulate a run of the whole service and collect performance metrics
   - whole service includes: the client request, task processing, execution and delivery back to the client
- `test_client.py`: suit of functions to test if service is operating correctly
-  `config.ini`: parameters common to all files

# Local Implementation
## Dispatcher and workers
- The `LocalDispatcher` contains a `multiprocessing.Pool` of workers for which it dispatches jobs to

- The process for dispatching tasks and retrieving is as follow. In an infinite loop, the dispatcher:
	- *if there are available workers*, listen to messages from the Redis channel 
	- If a message is received
		- the payload for the task is queried from the Redis database and sent to a worker in the pool of workers for *asynchronous* execution. The 'result' object is appended to a *dequeue* of results
	- in the *same iteration* and irrespective of available workers, the dispatcher checks if any result is ready 
	- if any result is ready
		- dispatcher updates the redis database and update the number of `busy_workers`

## Comments
-  A `deque` is used for the results to make the loop that searchs for a result faster:
	- The `results`  record is re-utilized, so items are removed from it when ready. To check if any is ready, it is necessary to iterate over `results` because of the async returns
	- For the loop, one could iterate through a data structure changing sizes (not good practice) or copy the whole DS. When the number of tasks is higher, the last approach introduces a loss of performance.
	- Moreover, `remove` operations are expensive for DS like lists, as the items being removed may not necessarily be in the beginning or end of the list due to the async. This might be quadratic in the length of results in worst case
	- the deque is O(1) to pop from the beginning and append to the end of the queue. I iterate over the len(results), pop the leftmost, check if it is ready and append to the end if it is not. No need to copy whole data structures; linear in the size of the results
- The loop to check results is done in every iteration and irrespective of having free workers to mask a little bit of latency; with this workers are freed ASAP to do new work

- One drawback of this dispatcher is that while workers are busy, it does nothing
	- In particular, the dispatcher could be retrieving items from the database while workers are occupied and store their data in a queue for immediate delivery when the worker is ready. 
	- This is not so crutial in our case because the DB is small, local, and the queries are simple. But it could be important to mask latency otherwise.
	- Retrieving items from DB would have its tradeoffs as well. Conidering a more complex system, with possible multiple dispatchers:
		- removing tasks from the redis before processing might introduce reliability issues due to the nature of the Redis channel. Once a message is listened, it cannot be listened again by the dispatcher; if something happened to the dispatcher, the works of that specific task would be lost.
		- if many dispatchers are using the same database, a  mechanism to reduce repeated work would be needed and in case of failure, a mechanisms to put tasks back in the queue

-  A possible upgrade is to make the dispatcher multithreaded. For instance, thread A could be responsible to search for ready results while thread B listen to workers.

# Pull implementation
## General
- Dispatcher and workers communicate through five `types` of messages:
	- `register` messsages from worker to dispatcher: initial message from worker; dispatcher register the worker in its internal list of workers
	- `result` messages from worker to dispatcher: worker send the result of a function to the dispatcher
	- `ready` messages from worker to dispatcher: sent when worker have free processes requiring work from the dispatcher 
	- `wait` messages from dispatcher to worker: if no work in the database, tells worker to wait 
	- `task` messages from dispatcher to worker with the payload of functions to execute

## Pull Dispatcher
- The `PullDispatcher` listens in a loop for messages coming from pull workers using a REP socket
- Since there are multiple workers sending messages to the same socket, a `zmq.Poller` is used for listening to avoid concurrency issues
	- internally, the poller keeps a queue of messages, so even if sockets are sending messaging at the same time we do not have XXXXX
- If a message from a worker is received  the dispatcher parses the message: 
	- if `register` message, the worker is added to the list of workers and a task is sent to it (if any)
	 - if `result` message, the data in Redis is updated and a task is sent to it (if any)
	 - if `ready` message a task is sent to it (if any)

- After parsing, dispatcher listen to `tasks` channel for new tasks
	- if there is a message, dispatcher send it to worker and update the status of the task to "RUNNING"
	- otherwise, dispatcher send a message for the worker to wait

 ## Pull worker
- Workers send message to dispatcher through a REQ socket
- Each pull worker has a `multiprocessing.Pool` of processes 
- Worker send initial message to the dispatcher for registration
- Worker process an infinite loop where:
	- it waits for a pre-specified delay of time for a reply from the dispatcher

- listen in a loop for new messages from dispatcher:
	- if a message is received, worker parses it
		- if type `wait`, does nothing
		- if type `task` and there are free processes, the worker send the task to the pool of processes for async execution, in the same manner as in the local case

- In the same iteration, irrespective of messages received from the dispatcher, iterates through the dequeue of results
	- if a result is ready, send it to dispatcher, listen new message and dispatch work/wait accordingly; note: this is *inside the loop* for results (see below)

- In the same iteration, send `ready` message to dispatcher asking for work if any process is still free
	- this branch will mostly be activated in the beginning phase of communication; otherwise, steps before it will typically occupy the workers

## Comments
- Note that for all types of messages received from worker, the dispatcher immediately try to send new task to the worker
	- This way, a REP-REQ cycle is not wasted. Since in this case the communication is *synchronous*, it is expensive for the dispatcher to send other messages to the worker, since this would create an additional (and unnecessary for our case) blocking in the worker and dispatcher
	- An alternative is sending a message to confirm received result/register and only send tasks when the worker send a specific message (like ready); this would waste one REP-REQ cycle for the worker to send another message requiring jobs, which is unnecessary

- `results` is a dequeue to look for ready results more efficiently, as explained in the local dispatcher

- A dispatch/wait is put inside the loop looking for results so that no iteration of this loop is wasted
	- consider the alternative, for instance: loop through results, once one is ready (say the fifth) go back to the main loop for reading/sending messages; in the next iteration, if the first four results are not ready, they will be scanned again unnecessarily

- The list of workers is not very useful here; I kept it because the specs seems to require it and because it would be useful for other things in a future implementation (for instance, keeping track of workers that disconnected and their respective tasks)

- One drawback of this implementation is the necessity of *syncrhonous* communication: a worker must send a message and the server must answer it immediately after; otherwise, we will get errors.
- In a more complex system, with many nodes distributed geographically, with different specs, different number of concurrent executions and each with functions with different running times, it might be infeasible to keep the synchornous pattern. Comments:
	- One visible restriction is the delay required for the workers to wait for messages back from the server. This delay is increasing in the number of workers and processes per worker
		- In our case, this is less of a problem because all communications are local. With effect, by increasing the delay with the number of worker/processes, it is possible to keep the throughput relatively stable as the number of workers grow. 
		- However, this might be an issue for a more complex system. The delays for responses in this case might not only be high, but volatile
		- One could choose a delay that is "sufficiently long", but this would impair the throughput/latency of the system

	 - Thinking of a complex system, the synchronous communication could also create unexpected blocks in case of any latency/disconnection from server or worker during a REQ-REP loop.  With nodes distributed not locally, it might be difficult to track where the problem came from

	- It woud be difficult to implement multiple task dispatchers communicating with multiple workers, which could improve the performance of the system

	- The sync communication restricts  the worker and the dispatcher to do much other work while each of them is not messaging each other. 
		- In this application, it was possible "to mask" this restriction of sync communication because the work to be done while each of them is occupied is simple (for the worker, scan list of results; for the dispatcher, query redis). 
		- In other situations, it might be desirable the dispatcher and worker do more things while not communicating

	- The sync communication would also be problematic if one wants to implement additional reliability measures, such as heartbeats, since more messaging could lead to more blocks in the system

- More performance could be gained by making either the dispatcher or the worker multithreaded. Reasons are similar to commented in the local dispatcher case

- The load balance could be more refined. In this implementation, the balance is evenly distributed among workers because they send and asks for tasks that are of similar size. A more complex load balancing could take into account the number of resources (CPU, memory, etc) each worker have free.

# Push implementation
## General
- Dispatcher and workers communicate by following `types` of messages:
	- `register` messsages from worker to dispatcher: initial message from worker contaning its ID and the number of processes it runs locally
	- `result` messages from worker to dispatcher: worker send the result of a function to the dispatcher
	- `task` messages from dispatcher to worker with the payload of functions to execute
	- If in heartbeat mode:
		- `reconnect` message from dispatcher to worker: tells the worker to reconnect, telling  the number of free processes it has
		- `reconnect` message from worker to dispatcher: tells the dispatcher it wants to reconnect; dispatcher re-register among its workers with the number of processes received
		- `heartbeat` message from the worker to dispatcher: tells the dispatcher it is alive

## PushDispatcher
- Push Dispatcher uses a ROUTER socket

- It can be deployed in a *non-heartbeat* or *heartbeat* mode:
	- If *heartbeat* mode, dispatcher keeps track of the last time a worker sent a message and remove workers if they become idle for too long
	- In *non-heartbeat* mode, the issue of possibly dead workers is not taken into account

- Has a subclass `PushWorker`, which stores a worker's: number of free processes, last time a heartbeat and a method to see if it is alive

- The `PushDispatcher` keeps two data structures of workers for the load balancing:
	- A dictionary containing  *worker ids* and the *number of free processes*  (and the last heartbeat in case of heartbeat mode)
	- A `dequeue` of *free workers* (`OrderedDict` if heartbeat mode)
		- free workers contains the worker IDs for workers that have at least one process not busy
		- The DS used are dequeue and OrderedDict to efficiently keep track of LRU workers for jov dispatching
			- OrderedDict is useful when removing idle workers with O(1) complexity while keeping the LRU in case of heartbeat mode
		- example: worker_A has 8 processes in total; 3 are not busy; worker B has all processes busy.
		- dispatcher will store workers = {A, 3; B,0} and free_workers = {A}
	
- The number of a *worker's free/busy  processes* is computed *in the task dispatcher*. 
	- Initially, a worker says how many processes it have. Afterwards, # of busy processes are calculated based on the messages sent/received to/from workers 

- Load balancing:
	- load is balanced *among workers* - not among processes. Task is always dispatched to the least recent used (LRU) *worker*.
		- that is, if a worker has 1 process free and was not used for a long time, it will be chosen. Even if another worker has more processes free.
		- My initial strategy was to balance through processes. I changed for this strategy for three reasons:
			- 1) By Professor comments in class, it seemed it was desired for *workers* to not become idle at any point. By balancing through processes it was not guaranteed that workers would have evenly number of tasks always
			- 2) It was more challenging and interesting to implement
				- with effect, for a balancing through processes, a simple implementation is to have a queue of worker_IDs as messages are received from workers. The queue would have repeated elements mapping  to a worker with free processes.
			- 3) In a more complex system, it seems more normal to distribute tasks among the nodes ("workers" here), not based on the threads each one may have.
	- If a worker just connected or reconnected to the server, it is put in the first position of the queue for task dispatch
		- since it is more prone for it to have more resources available

- The Dispatcher Runs in an infinite loop where:
	- Listen to workers using a `ZQM Poller` with zero delay. 
		- Since the communication is async, this is possible and allow the dispatcher to do other work while it is not receiving messages
	- If a message is received, parses the message:
		- if `register`, 
			- add a PushWorker to the list of workers. 
			- If it has free processes, add to the leftmost positon in the queue of *free processes* (i.e., first to be dispatched)
		- if `result`, 
			- update the database; adds a free process for the respective worker in the internal dict of workers; if the worker was not in queue of available workers, add it to it to the end of queue
	
	- In the same iteration, if there are free workers
		- If there are tasks to dispatch, send to the leftmost worker in `free_workers`; updates number of free processes in the `workers` dict; updates status of task to "RUNNING" in Redis DB
		- otherwise, loop

- If Dispatcher is in heartbeat mode, additionally to the above:
	- dispatcher listen to `heartbeat` messages and updates the last heartbeat of worker when received. It also upates the hearbeat whenever messages are received from a worker
	- In every iteration of the infinite loop and before trying to dispatch work, dispatcher executes `purge_workers` to remove any worker that did not send any message for more than `TIME_TO_EXPIRE` 
	- A worker may be disconneceted because of temporary delays in connection. In that case, if dispatcher receive message from this previous connected worker, it sends a `reconnection` message back.
		- When reconnection is sent back from worker, dispatcher put it back along with the number of free process sent by the worker

## Push worker
- Push worker uses a DEALER socket
- Push worker has a `multiprocessing.Pool` of local processes

- It can be deployed in a *non-heartbeat* or *heartbeat* mode:
	- If *heartbeat* mode, will send `heartbeat` messages to the dispatcher every `time_heartbeat` seconds
	
- Push worker sends initial message to *register* its ID with the ROUTER socket and tell the dispatcher how many processes it has locally
- Runs an infinite loop where:
	- Listen to Dispatcher using a `ZQM Poller` with zero delay. 
		- Since the communication is async, this is possible and allow the worker to do other work while it is not receiving messages 
	- If message is of type `task`, send the function payload to be executed  async in the pool of processes
		- notice worker has no checks for busy processes; this is done in the dispatcher side
	- In the same iteration, checks if any result is ready and send to dispatcher
		- notice there is no need to listen/wait after sending result as in the PullWorker; as communication is async, can send all results without blocking and losing communication cyles

## Comments
- The async communication has advantages when there are many tasks, processes and workers. Specially if considering a more complex system. This can be seem by:
	- there is no blocking; dispatcher and workers send their messages and are free to do other tasks
	- no need for a 'delay' for workers to receive messages
	- none of the problems associated with synchronous communcation commented in the `PullImplementation`
	- easier to extend to a "multiple dispatcher multiple workers" pattern
	- when reliability is not implemented, a side-consequence is that implementation is easier: the developer does not have to carefully design the code to deal with sync patterns and for ways to mask it

- However, at least in my implementation, it also introduces some overhead:
	- The task dispatcher has to keep lists of nodes and balance the load it sends to each node 
		- since messages are sent async, there is not the linear structure "receive-result-send-task" as in the sync case (there could be, but would defeat the purpose of async communication)
	- Nodes may be disconnected and kept in the list of nodes. A heartbeat-like schema to see if nodes are alive introduces even more complexity in the dispatcher side and overhead in the system

- In our case,  the benefits of async communication are limited: the jobs are deployed locally and the work besides function executions is little.
	- As a result,  at least in my implementation, one will see a *reduction* in throughput when the number of workers is raised due to the overhead 

- The load balancing could be more refined
	- In particular, load balancing through workers is not optimal, since all workers are sharing the same resources from my local machine.
	- In a more complex system, balancing through workers seems the more reasonable to do, but perhaps with a smarter schema than a simple LRU. A possibility is balancing by the number of resources each worker has available

- The heartbeat schema is only partial:
	- a full heartbeat schema would make the dispatcher send messages to the workers to tell it is alive and workers to disconnected / reconnect based on those messages
	- I did not went further with this because I discovered it was not required and it was overcomplicating the codebase. 


# Other general comments

**Limitations in reliability of implementations:**
- if a task is retrieved from REDIS cchannel and something happens with the worker, the task is gone. There is no mechanism to re-do the task in case something goes wrong.
	- This could be extended by keeping track of what workload was sent to which worker, a mechanism to decided that a worker has "died" and a mechanism to identify that a job has been done if a worker "ressucitates" and send the results again
