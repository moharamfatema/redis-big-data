import redis
import time
import random

# Connect to Redis
client = redis.StrictRedis(host='localhost', port=6379, db=0)

# Produce events to the stream
def produce_events():
    print("Producing events...")
    for i in range(150):
        event = {'error_code': random.choice(['200',  '404']),
                 'response_time': random.randint(100, 500)}
        print("event",event)
        client.xadd('logs', event)
        # time.sleep(0.1)

# Consume events and calculate error rate
def consume_events():
    print("Consuming events...")
    events = client.xread({'logs': '0-0'}, count=10, block=5000)
    for stream, entries in events:
        error_count = sum(1 for entry in entries if entry[1][b'error_code'] == b'500')
        print(f"Error Rate: {error_count / len(entries):.2%}")

produce_events()
consume_events()
