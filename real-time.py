import redis
import time
import random
import threading

# Connect to Redis
client = redis.StrictRedis(host='localhost', port=6379, db=0)
client.flushdb()

# Produce events to the stream
def produce_events():
    print("Producing events...")
    while True:
        event = {'error_code': random.choice(['200', '500','404']),
                 'response_time': random.randint(100, 500)}
        client.xadd('logs', event)
        # time.sleep(0.1)

# Consume events and calculate error rate
def consume_events():
    error_count = 0
    num_entries = 0
    num_tries = 10
    print("Consuming events...")
    last_id = '0-0'
    while True:
        events = client.xread({'logs': last_id},block=5000)
        if events:
            print("new entries found")
        for _, entries in events:
            error_count += sum(1 for entry in entries if entry[1][b'error_code'] == b'500')
            num_entries += len(entries)
            last_id = entries[-1][0]
            print(f"Total number of conumed events: {num_entries}")
            print(f"Total Error count: {error_count}")
            print(f"Error Rate: {error_count / num_entries:.2%}")
        time.sleep(1)

producer = threading.Thread(target=produce_events)
consumer = threading.Thread(target=consume_events)

producer.start()
consumer.start()

producer.join()
consumer.join()
