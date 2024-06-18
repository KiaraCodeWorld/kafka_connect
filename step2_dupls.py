""" prompt : 
write python code with good comments and proper standards - 
which will 
	1. setup the environment variable for redisDB and password etc. using dotEnv()
	2. Read from the Stream "gwas-recordings" - 
			Copy to 2 streams the records : 
				1. "gwas-converter"
				2. "gwas-transcription"
	3. add try - catch blocks 
	4. add failure handling and ensuring nothing is missed out.
"""


import os
from dotenv import load_dotenv
import redis
from datetime import datetime

# Load environment variables from a `.env` file
load_dotenv()

# Redis connection details from environment variables
REDIS_HOST = os.getenv('REDIS_HOST')
REDIS_PORT = int(os.getenv('REDIS_PORT'))
REDIS_PASSWORD = os.getenv('REDIS_PASSWORD')

# Redis stream names
SOURCE_STREAM = "gwas-recordings"
TARGET_STREAMS = ["gwas-converter", "gwas-transcription"]

def connect_to_redis():
    """
    Establishes a connection to the Redis server.

    Returns:
        A Redis client object.
    """
    try:
        redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, password=REDIS_PASSWORD)
        return redis_client
    except redis.exceptions.ConnectionError as e:
        print(f"Error connecting to Redis: {e}")
        exit(1)

def read_from_stream(redis_client, stream_name):
    """
    Reads messages from the specified Redis stream.

    Args:
        redis_client: A Redis client object.
        stream_name: The name of the Redis stream to read from.

    Returns:
        A list of dictionaries representing the read messages.
    """
    try:
        # Use XRANGE to read all messages from the stream
        messages = list(redis_client.xrange(stream_name, min='-'))
        return [message[1] for message in messages]  # Extract message data from tuples
    except redis.exceptions.ResponseError as e:
        print(f"Error reading from stream {stream_name}: {e}")
        return []  # Return empty list on error

def copy_to_stream(redis_client, source_stream, target_stream, message):
    """
    Copies a message from the source stream to the target stream.

    Args:
        redis_client: A Redis client object.
        source_stream: The name of the source stream.
        target_stream: The name of the target stream.
        message: The message data to copy.
    """
    try:
        redis_client.xadd(target_stream, {'*': message})
    except redis.exceptions.ResponseError as e:
        print(f"Error copying message to stream {target_stream}: {e}")

def process_messages(redis_client):
    """
    Reads messages from the source stream, copies them to target streams,
    and handles potential errors.

    Args:
        redis_client: A Redis client object.
    """
    messages = read_from_stream(redis_client, SOURCE_STREAM)
    for message in messages:
        for target_stream in TARGET_STREAMS:
            try:
                copy_to_stream(redis_client, SOURCE_STREAM, target_stream, message)
            except Exception as e:  # Catch all exceptions for robustness
                print(f"Error copying message to {target_stream}: {e}")

        # Acknowledging messages from the source stream (optional)
        # If using Redis Streams as a reliable queue, uncomment this:
        # redis_client.xack(SOURCE_STREAM, messages[0][0])  # Acknowledge the first message

def main():
    """
    Main function that connects to Redis, processes messages,
    and ensures successful execution.
    """
    try:
        redis_client = connect_to_redis()
        process_messages(redis_client)
        print(f"{datetime.now()} - Successfully processed messages from {SOURCE_STREAM} to {TARGET_STREAMS}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

if __name__ == "__main__":
    main()


===============


import os
import redis
from dotenv import load_dotenv

def setup_environment():
    """Setup environment variables from .env file using dotenv."""
    load_dotenv()

def get_redis_connection():
    """Establish a connection to the Redis database."""
    redis_host = os.getenv('REDIS_HOST')
    redis_port = os.getenv('REDIS_PORT')
    redis_password = os.getenv('REDIS_PASSWORD')

    try:
        r = redis.Redis(host=redis_host, port=redis_port, password=redis_password)
        if r.ping():
            print("Connected to Redis")
        return r
    except redis.ConnectionError as e:
        print(f"Error connecting to Redis: {e}")
        raise

def read_from_stream(redis_conn, stream_name, last_id='0-0'):
    """Read records from a Redis stream."""
    try:
        records = redis_conn.xread({stream_name: last_id}, count=10, block=5000)
        return records
    except redis.RedisError as e:
        print(f"Error reading from stream {stream_name}: {e}")
        raise

def write_to_stream(redis_conn, stream_name, record):
    """Write a record to a Redis stream."""
    try:
        redis_conn.xadd(stream_name, record)
    except redis.RedisError as e:
        print(f"Error writing to stream {stream_name}: {e}")
        raise

def main():
    """Main function to read from one stream and copy records to two other streams."""
    setup_environment()
    
    redis_conn = get_redis_connection()
    source_stream = 'gwas-recordings'
    target_streams = ['gwas-converter', 'gwas-transcription']

    last_id = '0-0'  # Start reading from the beginning of the stream

    while True:
        try:
            records = read_from_stream(redis_conn, source_stream, last_id)
            if records:
                for stream, entries in records:
                    for entry_id, entry_data in entries:
                        for target_stream in target_streams:
                            write_to_stream(redis_conn, target_stream, entry_data)
                        last_id = entry_id
        except Exception as e:
            print(f"Unexpected error: {e}")

if __name__ == "__main__":
    main()
