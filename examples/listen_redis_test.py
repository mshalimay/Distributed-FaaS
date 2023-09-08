import redis

# Create a Redis client
r = redis.Redis(host='localhost', port=6379, db=0)

# Create a Redis subscriber
subscriber = r.pubsub()

# Subscribe to a channel
channel_name = "tasks"
subscriber.subscribe(channel_name)

# # Block and wait for messages on the channel
# for message in subscriber.listen():
#     if message['type'] == 'message':
#         print(f"Received message: {message['data'].decode('utf-8')}")


# "loop" will be printed just once, because the subscriber.listen() blocks the thread
# while True:
#     print("new worker")
#     for message in subscriber.listen():
#         if message['type'] == 'message':
#             print(f"Received message: {message['data'].decode('utf-8')}")


while True:
    # listen for incoming messages from workers
    print("new worker")

    # worker is free => retrieves a task from Redis; 
    # loop until the retrieved message is a valid task
    message = None
    while message is None or message['type'] != 'message':
        message = subscriber.get_message()

    print(f"Received message: {message['data'].decode('utf-8')}")
    # Extract data from RedisDB and send to worker
    print("sent to worker")
    # task_id, fn_payload, param_payload = query_redis(message)
    # send_message({"task_id": task_id, "fn_payload": fn_payload, "param_payload": param_payload})
