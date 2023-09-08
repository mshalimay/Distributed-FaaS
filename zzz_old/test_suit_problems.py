

import codecs
import dill
import uuid

def serialize(obj: object) -> str:
        return codecs.encode(dill.dumps(obj), "base64").decode()

def deserialize(ser_obj: str) -> object:
    return dill.loads(codecs.decode(ser_obj.encode(), "base64"))




import redis
r = redis.Redis(host='localhost', port=6379, db=1)

subscriber = r.pubsub()

subscriber.subscribe("tasks")

message = subscriber.get_message()

task_id = message['data'].decode('utf-8')        
# retrieve serialized function and parameters
fn_payload = r.hget(task_id, "fn_payload")
param_payload = r.hget(task_id, "param_payload")




