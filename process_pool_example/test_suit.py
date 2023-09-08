import requests
import codecs
import dill
#from serialize import deserialize, serialize
import logging
import time
import random

def serialize(obj) -> str:
    return codecs.encode(dill.dumps(obj), "base64").decode()


def deserialize(obj: str):
    return dill.loads(codecs.decode(obj.encode(), "base64"))


base_url = "http://127.0.0.1:8000/"

valid_statuses = ["QUEUED", "RUNNING", "COMPLETED", "FAILED"]


# def test_fn_registration():
#     resp = requests.post(base_url + "register_function",
#                          json={"name": "hello",
#                                "payload": "payload"})

#     assert resp.status_code == 200
#     assert "function_id" in resp.json()


def double(x):
    import random
    import time
    #time.sleep(random.randint(10, 50))
    return x * 2


def test_execute_fn():
    resp = requests.post(base_url + "register_function",
                         json={"name": "hello",
                               "payload": serialize(double)})
    fn_info = resp.json()
    assert "function_id" in fn_info

    resp = requests.post(base_url + "execute_function",
                         json={"function_id": fn_info['function_id'],
                               "payload": serialize(((2,), {}))})

    print(resp)
    assert resp.status_code == 200
    assert "task_id" in resp.json()

    task_id = resp.json()["task_id"]

    resp = requests.get(f"{base_url}status/{task_id}")
    print(resp.json())
    assert resp.status_code == 200
    assert resp.json()["task_id"] == task_id
    assert resp.json()["status"] in valid_statuses


def test_roundtrip():
    resp = requests.post(base_url + "register_function",
                         json={"name": "double",
                               "payload": serialize(double)})
    fn_info = resp.json()

    number = random.randint(0, 10000)
    resp = requests.post(base_url + "execute_function",
                         json={"function_id": fn_info['function_id'],
                               "payload": serialize(((number,), {}))})

    assert resp.status_code == 200
    assert "task_id" in resp.json()

    task_id = resp.json()["task_id"]

    for i in range(20):

        resp = requests.get(f"{base_url}result/{task_id}")

        assert resp.status_code == 200
        assert resp.json()["task_id"] == task_id
        if resp.json()['status'] in ["COMPLETED", "FAILED"]:
            print("testing completed result")
            logging.warning(f"Task is now in {resp.json()['status']}")
            s_result = resp.json()
            logging.warning(s_result)
            result = deserialize(s_result['result'])
            assert result == number*2
            break
        time.sleep(0.01)


def test_completed():
    resp = requests.post(base_url + "register_function",
                         json={"name": "double",
                               "payload": serialize(double)})
    fn_info = resp.json()

    number = random.randint(0, 10000)
    resp = requests.post(base_url + "execute_function",
                         json={"function_id": fn_info['function_id'],
                               "payload": serialize(((number,), {}))})

    assert resp.status_code == 200
    assert "task_id" in resp.json()

    task_id = resp.json()["task_id"]

    for i in range(20):

        resp = requests.get(f"{base_url}result/{task_id}")

        assert resp.status_code == 200
        assert resp.json()["task_id"] == task_id

        failed = True
        if resp.json()['status'] in ["COMPLETED"]:
            print("testing completed result")
            logging.warning(f"Task is now in {resp.json()['status']}")
            s_result = resp.json()
            logging.warning(s_result)
            result = deserialize(s_result['result'])
            if result == number*2:
                failed = False
            break
        time.sleep(1)

    assert not failed


def test_many_completed():
    tasks = []
    numbers = []
    for _ in range(100):
        resp = requests.post(base_url + "register_function",
                            json={"name": "double",
                                "payload": serialize(double)})
        fn_info = resp.json()

        number = random.randint(0, 10000)
        numbers.append(number)
        resp = requests.post(base_url + "execute_function",
                            json={"function_id": fn_info['function_id'],
                                "payload": serialize(((number,), {}))})

        assert resp.status_code == 200
        assert "task_id" in resp.json()
        tasks.append(resp.json()["task_id"])

    success = [0]*len(tasks)
    for i in range(len(tasks)):
        task_id = tasks[i]
        number = numbers[i]
        
        for _ in range(20):
            resp = requests.get(f"{base_url}result/{task_id}")

            assert resp.status_code == 200
            assert resp.json()["task_id"] == task_id

            if resp.json()['status'] in ["COMPLETED"]:
                s_result = resp.json()
                result = deserialize(s_result['result'])
                if result == number*2:
                    success[i] = 1
                break
            time.sleep(0.5)

    assert all(success)

