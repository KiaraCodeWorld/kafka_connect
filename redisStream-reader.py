l = r.xread( count=2, streams={stream_key:0} )
print(l)

first_stream = l[0]
print( f"got data from stream: {first_stream[0]}")
fs_data = first_stream[1]
for id, value in fs_data:
    print( f"id: {id} value: {value[b'v']}")

============

import redis
import json

# Connect to Redis server
redis_client = redis.StrictRedis(host='localhost', port=6379, db=0)

# Read from the Redis stream
stream_name = 'your_stream_name'
last_id = '0-0'  # The ID to start reading from, you can adjust this as needed

while True:
    # Read from the stream
    response = redis_client.xread({stream_name: last_id}, block=0, count=1)
    
    if response:
        for stream, messages in response:
            for message_id, message in messages:
                # Extract the JSON record
                record = {k.decode('utf-8'): v.decode('utf-8') for k, v in message.items()}
                
                # Parse the JSON
                record_json = json.loads(record['json_record'])  # Adjust the key name as needed
                
                # Extract the required attributes
                _id = record_json.get('_id')
                recordingname = record_json.get('recordingname')
                
                # Print the attributes
                print(f"_id: {_id}, recordingname: {recordingname}")
                
                # Update last_id to the current message_id
                last_id = message_id

# Ensure to handle exceptions and edge cases as needed



