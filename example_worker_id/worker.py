import zmq
import uuid
import sys

context = zmq.Context()

worker = context.socket(zmq.REQ)

worker.setsockopt(zmq.IDENTITY, uuid.uuid4().bytes)

worker.connect("tcp://0.0.0.0:8001")

worker.send(b"Hello")

wait_time = sys.argv[1]


