import zmq
import codecs
import dill
import time

def serialize(obj: object) -> str:
        return codecs.encode(dill.dumps(obj), "base64").decode()

def deserialize(ser_obj: str) -> object:
    return dill.loads(codecs.decode(ser_obj.encode(), "base64"))


dealer =  zmq.Context().socket(zmq.DEALER)

dealer.connect("tcp://0.0.0.0:8001")

data = {
    "task_id": "123",
    "fn_payload": "def fn(x): return x**2",
    "param_payload": "2"}


message_to_send = serialize(data)

while True:
    time.sleep(5)
    dealer.send_string(message_to_send)
    #message = deserialize(dealer.recv().decode('utf-8'))
    message = dealer.recv()
    print(message)
