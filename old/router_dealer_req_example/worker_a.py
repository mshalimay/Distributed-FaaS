import zmq

context = zmq.Context.instance()
worker = context.socket(zmq.DEALER)
worker.setsockopt(zmq.IDENTITY, b'A')
worker.connect("tcp://*:5559")

total = 0
while True:
    # We receive one part, with the workload
    request = worker.recv()
    finished = request == b"END"
    if finished:
        print("A received: %s" % total)
        break
    total += 1