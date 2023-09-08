import zmq
import time
import dill
import random
context = zmq.Context()

# Create a ROUTER socket
socket = context.socket(zmq.REP)
socket.bind("tcp://*:5555")

while True:
    message = socket.recv_string()
    print(f"{message}")

    # Simulate some processing time
    #time.sleep(10)

    # generate random integer between 1 and 10
    
    reply = str(random.randint(1, 11))
    socket.send_string(reply)
