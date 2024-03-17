# Distributed Function-as-a-Service Python
This repository implements the backend of a Function-as-a-Service platform that allow user's to run arbitrary Python functions in a distributed environment of computing nodes.

## Example: 

Users may specify a function as below:
```python
def slow_function():
   time.sleep(100)
```

In summary, the service:
- Serializes the functions sent by the users.
- Create associated tasks and store in an `REDIS` database.
- Computing nodes collect tasks from the shared database, execute and store results in `REDIS`
- Master notifies user their tasks is complete.
