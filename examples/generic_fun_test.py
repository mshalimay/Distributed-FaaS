import dill
import codecs


def serialize(obj) -> str:
    return codecs.encode(dill.dumps(obj), "base64").decode()

def deserialize(obj: str):
    return dill.loads(codecs.decode(obj.encode(), "base64"))



def double(x):
    return x * 2

# client will seralize function; as this:
ser_fn = serialize(double)

# client will serialize parameters; as this:
ser_params = serialize(((2,), {}))


def execute_fn(ser_fn: str, ser_params: str):
    # deserialize fun
    fn = deserialize(ser_fn)

    # deserialize params
    params = deserialize(ser_params)

    # execute the function
    result = fn(params)


def execute_task(fn_payload, param_payload):
    # Execute the function
    exec(fn_payload)
    result = eval(f"dynamic_fun({param_payload})")
    return result
