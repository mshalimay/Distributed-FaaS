import zmq


context = zmq.Context()
server = context.socket(zmq.REP)
server.bind("tcp://0.0.0.0:8001")

poller = zmq.Poller()
poller.register(server, zmq.POLLIN)


while True:
    sockets = dict(poller.poll(0))
    if server in sockets:
        message = server.recv()
        server.send_string("OK")


