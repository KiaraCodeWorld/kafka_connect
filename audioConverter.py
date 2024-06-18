"""
write python code with good comments and proper standards - 
which will 
	1. setup the environment variable for redisDB and password etc. using dotEnv()
	2. Read messages from the Stream "gwas-converter" and grab the .wav filenae amd s3 path:
	3. download the .wav file to /tmp directory 
	4. convert the .wav to mp3 using pydeb or any other package
	5. add try - catch blocks 
	6. copy the converted .mp3 file to s3 location.
	7. return the pth of mp3 and write to "gwas-collector" redisstram
	
	4. add failure handling and ensuring nothing is missed out.
"""


import os
import redis
import boto3
from dotenv import load_dotenv
from pydub import AudioSegment
import requests

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

def get_s3_client():
    """Get an S3 client using boto3."""
    aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
    aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
    aws_region = os.getenv('AWS_REGION')

    try:
        s3 = boto3.client(
            's3',
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=aws_region
        )
        return s3
    except Exception as e:
        print(f"Error creating S3 client: {e}")
        raise

def download_wav_file(s3_client, s3_path, local_path):
    """Download a .wav file from S3 to the local /tmp directory."""
    try:
        bucket, key = s3_path.split('/', 1)
        s3_client.download_file(bucket, key, local_path)
    except Exception as e:
        print(f"Error downloading file from S3: {e}")
        raise

def convert_wav_to_mp3(wav_path, mp3_path):
    """Convert a .wav file to .mp3 format."""
    try:
        audio = AudioSegment.from_wav(wav_path)
        audio.export(mp3_path, format="mp3")
    except Exception as e:
        print(f"Error converting .wav to .mp3: {e}")
        raise

def upload_file_to_s3(s3_client, local_path, s3_path):
    """Upload a file to S3."""
    try:
        bucket, key = s3_path.split('/', 1)
        s3_client.upload_file(local_path, bucket, key)
    except Exception as e:
        print(f"Error uploading file to S3: {e}")
        raise

def read_from_stream(redis_conn, stream_name, last_id='0-0'):
    """Read messages from a Redis stream."""
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
    """Main function to read from one stream, process and write to another."""
    setup_environment()
    
    redis_conn = get_redis_connection()
    s3_client = get_s3_client()
    
    source_stream = 'gwas-converter'
    target_stream = 'gwas-collector'

    last_id = '0-0'  # Start reading from the beginning of the stream

    while True:
        try:
            records = read_from_stream(redis_conn, source_stream, last_id)
            if records:
                for stream, entries in records:
                    for entry_id, entry_data in entries:
                        wav_filename = entry_data.get(b'wav_filename').decode('utf-8')
                        s3_path = entry_data.get(b's3_path').decode('utf-8')
                        
                        local_wav_path = f"/tmp/{wav_filename}"
                        local_mp3_path = local_wav_path.replace('.wav', '.mp3')
                        s3_mp3_path = s3_path.replace('.wav', '.mp3')

                        # Download the .wav file
                        download_wav_file(s3_client, s3_path, local_wav_path)

                        # Convert the .wav file to .mp3
                        convert_wav_to_mp3(local_wav_path, local_mp3_path)

                        # Upload the .mp3 file to S3
                        upload_file_to_s3(s3_client, local_mp3_path, s3_mp3_path)

                        # Write the result to the target Redis stream
                        result_record = {
                            'mp3_path': s3_mp3_path,
                            'original_wav': s3_path
                        }
                        write_to_stream(redis_conn, target_stream, result_record)

                        # Update the last_id to the current entry_id
                        last_id = entry_id
        except Exception as e:
            print(f"Unexpected error: {e}")

if __name__ == "__main__":
    main()

====================


import os
import redis
import uuid
import subprocess
from dotenv import load_dotenv
from pydub import AudioSegment

# Step 1: Setup environment variables using dotenv
load_dotenv()  # Load environment variables from .env file

# Example environment variables:
REDIS_HOST = os.getenv('REDIS_HOST')
REDIS_PORT = os.getenv('REDIS_PORT')
REDIS_PASSWORD = os.getenv('REDIS_PASSWORD')

# Step 2: Read messages from the "gwas-converter" stream
def read_messages_from_stream():
    try:
        r = redis.StrictRedis(host=REDIS_HOST, port=REDIS_PORT, password=REDIS_PASSWORD, decode_responses=True)
        stream_name = 'gwas-converter'
        messages = r.xrange(stream_name)

        for message_id, message_data in messages:
            # Extract .wav filename and S3 path from the message data
            wav_filename = message_data.get('filename')
            s3_wav_path = message_data.get('s3-file-path')

            # Step 3: Download the .wav file to /tmp directory
            local_wav_path = f'/tmp/{wav_filename}'
            # Your S3 download logic here (e.g., using boto3)

            # Step 4: Convert .wav to .mp3
            audio = AudioSegment.from_wav(local_wav_path)
            mp3_filename = os.path.splitext(wav_filename)[0] + '.mp3'
            local_mp3_path = f'/tmp/{mp3_filename}'
            audio.export(local_mp3_path, format='mp3')

            # Step 5: Add try-catch blocks for exception handling
            try:
                # Your S3 upload logic here (e.g., using boto3)
                # Step 6: Copy the converted .mp3 file to S3 location
                # Step 7: Return the path of the MP3 file and write to "gwas-collector" Redis stream
                # ...
                print(f"Converted {wav_filename} to {mp3_filename}")
            except Exception as e:
                print(f"Error uploading to S3: {e}")

    except Exception as e:
        print(f"Error reading messages from Redis stream: {e}")

# Call the function to read messages
read_messages_from_stream()


==================


import os
from dotenv import load_dotenv
import boto3
import redis
from pydub import AudioSegment  # Assuming pydub for conversion

# Load environment variables from a `.env` file
load_dotenv()

# Redis connection details from environment variables
REDIS_HOST = os.getenv('REDIS_HOST')
REDIS_PORT = int(os.getenv('REDIS_PORT'))
REDIS_PASSWORD = os.getenv('REDIS_PASSWORD')

# S3 connection details (assuming credentials are already set)
S3_BUCKET_NAME = os.getenv('S3_BUCKET_NAME')

# Redis stream names
SOURCE_STREAM = "gwas-converter"
TARGET_STREAM = "gwas-collector"
TMP_DIR = "/tmp"  # Temporary directory for downloaded files

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

def download_wav_file(redis_client, message):
    """
    Downloads the .wav file from S3 based on the information in the message.

    Args:
        redis_client: A Redis client object.
        message: The message data from the Redis stream.

    Returns:
        The path of the downloaded file on disk or None on error.
    """
    try:
        filename = message.get(b'filename')
        s3_path = message.get(b's3-file-pth')

        if not filename or not s3_path:
            print(f"Missing filename or s3-file-pth in message: {message}")
            return None

        s3 = boto3.client('s3')
        download_path = os.path.join(TMP_DIR, filename)
        s3.download_file(S3_BUCKET_NAME, s3_path.decode('utf-8'), download_path)
        return download_path
    except Exception as e:
        print(f"Error downloading file: {e}")
        return None

def convert_wav_to_mp3(wav_path):
    """
    Converts the downloaded .wav file to .mp3 format.

    Args:
        wav_path: The path of the downloaded .wav file.

    Returns:
        The path of the converted .mp3 file or None on error.
    """
    try:
        wav_file = AudioSegment.from_wav(wav_path)
        mp3_path = os.path.splitext(wav_path)[0] + ".mp3"
        wav_file.export(mp3_path, format="mp3")
        return mp3_path
    except Exception as e:
        print(f"Error converting WAV to MP3: {e}")
        return None

def upload_mp3_to_s3(s3_client, mp3_path):
    """
    Uploads the converted .mp3 file to the S3 bucket.

    Args:
        s3_client: A boto3 S3 client object.
        mp3_path: The path of the converted .mp3 file.

    Returns:
        The S3 path of the uploaded file or None on error.
    """
    try:
        filename = os.path.basename(mp3_path)
        s3_client.upload_file(mp3_path, S3_BUCKET_NAME, filename)
        return f"s3://{S3_BUCKET_NAME}/{filename}"
    except Exception as e:
        print(f"Error uploading MP3 to S3: {e}")
        return None

def process_message(redis_client, message):
    """
    Processes a message from the source stream, downloads, converts,
    uploads, and writes information to the target stream.

    Args:
        redis_client: A Redis client object.
        message: The message data from the Redis stream.
    """
    wav_path = download_wav_file(redis_client, message)
    if not wav_path:
        return  # Skip to next message on download failure
