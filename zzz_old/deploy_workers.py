import argparse
import subprocess
import signal
import time
import sys

DISPATCHER_IP = "0.0.0.0"

def signal_handler(sig, frame):
    print("Killing processes")
    task_disp.kill()
    for worker in workers:
        worker.kill()
    sys.exit(0)

        
if __name__ == "__main__":
    # # instantiate argument parser
    parser = argparse.ArgumentParser(description='Work/TaskDispatcher deployer')

    # Add the arguments
    parser.add_argument('-m', type=str, choices=['local', 'pull', 'push'],
                        help='The mode of the workers deployed.')

    parser.add_argument('-w', type=int, required=False, 
                        help='The number of workers to deploy. Push and Pull modes only.')

    parser.add_argument('-n', type=int, required=True, 
                        help='The number of processors per worker to use.')

    parser.add_argument('-port', type=int, required=True, 
                        help='The number of processors per worker to use.')

    parser.add_argument("--nh", action="store_true", 
                        help="Run PUSH dispatcher in non-heartbeat mode")

    parser.add_argument("--it", action="store_true", 
                        help="Open a terminal for each worker")

    #parser.add_argument("--url", help="the URL of the task dispatcher", type = str)
    
    # Parse the arguments
    args = parser.parse_args()
    args.url = f"tcp://{DISPATCHER_IP}:{args.port}"

    ## For debugging
    # args = argparse.Namespace()
    # args.m = 'push'
    # args.n = 5
    # args.w = 5
    # args.port = 8001
    # args.url = f"tcp://0.0.0.0:8001"
    # args.nh = True

    workers = []

    if args.m == 'local' and not args.n:
        parser.print_help()
        sys.exit(0)
    elif args.m == 'pull' or args.m == "push":
        if not args.w or not args.url or not args.port:
            parser.print_help()
            sys.exit(0)
    
    if args.m == 'local':
        command = f'python task_dispatcher.py -m local -w {str(args.n)}'
        task_disp = subprocess.Popen(command, shell=True)
        
    elif args.m == 'pull':
        #command = f"python task_dispatcher.py -m pull -p {str(args.port)}"
        #task_disp = subprocess.Popen(command, shell=True)
        #task_disp = subprocess.Popen(["python", "pull_worker.py", str(args.n), args.url])

        if args.it:
            command = f'cmd.exe /c start wsl python ~/DistributedSystems/Project/pull_worker.py {str(args.n)} {args.url}'
        else:
            command = f"python pull_worker.py {str(args.n)} {args.url}"
            
        for _ in range(args.w):
            time.sleep(0)
            workers.append(subprocess.Popen(command, shell=True))
            
    elif args.m == 'push':
        if args.nh:
            #command = f"python task_dispatcher.py -m push -p {str(args.port)} --nh"
            #task_disp = subprocess.Popen(command, shell=True)
            #task_disp = subprocess.Popen(["python", "task_dispatcher.py", "-m", "push", '-p', str(args.port), "--nh"])
            if args.it:
                command = f'cmd.exe /c start wsl python ~/DistributedSystems/Project/push_worker.py {str(args.n)} {args.url} --nh'
            else:
                command = f"python push_worker.py {str(args.n)} {args.url} --nh"
        else:
            # command = f"python task_dispatcher.py -m push -p {str(args.port)}"
            # task_disp = subprocess.Popen(command, shell=False)
            #task_disp = subprocess.Popen(["python", "task_dispatcher.py", "-m", "push", '-p', str(args.port)])

            if args.it:
                command = f'cmd.exe /c start wsl python ~/DistributedSystems/Project/push_worker.py {str(args.n)} {args.url}'
            else:
                command = f"python push_worker.py {str(args.n)} {args.url}"

        for _ in range(args.w):
            time.sleep(0)
            workers.append(subprocess.Popen(command, shell=True))

    signal.signal(signal.SIGINT, signal_handler)
    while True:
        time.sleep(5)
