import time
import zmq
import random

context = zmq.Context()
router = context.socket(zmq.ROUTER)

router.bind("tcp://*:5559")

# send work to workers
for _ in range(10):
    worker_id = random.choice([b'A', b'A', b'B'])
    work = b"This is the workload"
    router.send_multipart([worker_id, work])

    

    