import redis
from helper_functions import serialize, deserialize
import random
import time

def wait_and_print(t, message):
    import time
    time.sleep(t)
    print(message)
    # return None



# redis
r = redis.Redis(host='localhost', port=6379, db=1)
r.flushdb()


# -----------------------------------------------
# Register functions on Redis
# -----------------------------------------------
# function and parameters

task_id = 0
fn_payload = serialize(wait_and_print)

for _ in range(100):
    time.sleep(0.5)
    
    task_id += 1   
    if task_id == 1:
        t = 20
    else:
        t = random.randint(1, 10)
        
    message = f"Waited {t} seconds"
    
    param_payload = serialize(((t, message),{}))

    r.hset(task_id, mapping={"status":"QUEUED",
                            "fn_payload":fn_payload,
                            "param_payload":param_payload, 
                            "result":"None"})

    r.publish("tasks", task_id)



    