import codecs
import dill
import uuid

def serialize(obj: object) -> str:
        return codecs.encode(dill.dumps(obj), "base64").decode()

def deserialize(ser_obj: str) -> object:
    return dill.loads(codecs.decode(ser_obj.encode(), "base64"))

def execute_fn(task_id:uuid.UUID, ser_fn: str, ser_params: str):
        # deserialize function and parameters sent by client
        fn = deserialize(ser_fn)
        params = deserialize(ser_params)

        try:
            # execute function(*args, **kwargs)
            # obs: client must pass parameters as tuple, even if there is only args/kwargs
            # possible make flexible for more formats, but as long as the client is correct
            # this should not be a problem
            
            result = fn(*params[0], **params[1])
            status = "COMPLETED"
        except Exception as e:
            result = None
            status = "FAILED"

        return task_id, status, serialize(result)





if __name__ == "__main__":   

    pass

    # ## example of usage of execute_fn

    # def double(x):
    #     return x * 2

    # number = 2 
    # a = execute_fn("1", serialize(double), serialize(({"x":number})))
    # b = execute_fn("1", serialize(double), serialize(((number,), {})))
    # c = execute_fn("1", serialize(double), serialize(((2,))))
    # d = execute_fn("1", serialize(double), serialize(2))    

   

    # # def wait_and_print(t, message):
    # #     import time
    #     time.sleep(t)
    #     print(message)
    #     return None

    # # function and parameters
    # task_id = 1
    # fn_payload = serialize(wait_and_print)
    # t = 5
    # message = f"Waited {t} seconds"

    # fn_payload = serialize(wait_and_print)
    # param_payload = serialize((5, message))

    # execute_fn("1", fn_payload, param_payload)
