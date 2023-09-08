import zmq
import sys
import dill
import time


context = zmq.Context()

# Create a DEALER socket
socket = context.socket(zmq.REQ)
identity = sys.argv[1]
socket.connect("tcp://localhost:5555")


# send message to server
message = f"Give me work (worker {identity})".encode('utf-8')
socket.send(message)

# receive message from server
response_from_server = socket.recv_string()

print(f"Time for the job {response_from_server}")
# do some work
time.sleep(int(response_from_server))

# send message to server
socket.send_string(f"Work done (worker {identity})")
