import boto3
from datetime import date, timedelta
import json
import uuid

# Define S3 bucket name and RediStream key
S3_BUCKET_NAME = "your-bucket-name"
REDISTREAM_KEY = "gwas-recordings"


def get_yesterday_directory():
  """
  This function returns the directory path for the previous day in YYYY/MM/DD format.
  """
  today = date.today()
  yesterday = today - timedelta(days = 1)
  return yesterday.strftime("%Y/%m/%d")


def list_s3_files(s3_client, bucket_name, directory):
  """
  This function lists all files within the specified directory in the S3 bucket.

  Args:
      s3_client: A boto3 S3 client object.
      bucket_name: The name of the S3 bucket.
      directory: The directory path within the S3 bucket.

  Returns:
      A list of dictionaries containing file information (key and last_modified).
  """
  paginator = s3_client.get_paginator('list_objects_v2')
  pages = paginator.paginate(Bucket=bucket_name, Prefix=directory, Delimiter='/')
  file_list = []
  for page in pages:
    if 'Contents' in page:
      file_list.extend(page['Contents'])
  return file_list


def create_dataframe(file_list):
  """
  This function creates a Pandas DataFrame from the list of S3 file information.

  Args:
      file_list: A list of dictionaries containing file information (key and last_modified).

  Returns:
      A Pandas DataFrame with columns 'filename' and 's3_file_path'.  
  """
  data = []
  for file_info in file_list:
    filename = file_info['Key']
    s3_file_path = f"s3://{S3_BUCKET_NAME}/{filename}"
    data.append({'filename': filename, 's3_file_path': s3_file_path})
  return pd.DataFrame(data)


def create_redistream_record(dataframe):
  """
  This function creates a JSON record with additional information for the RediStream.

  Args:
      dataframe: A Pandas DataFrame containing file information.

  Returns:
      A list of JSON records for each file in the DataFrame.
  """
  records = []
  for index, row in dataframe.iterrows():
    record = {
        'id': str(uuid.uuid4()),
        'filename': row['filename'],
        's3_file_pth': row['s3_file_path']
    }
    records.append(record)
  return records


def main():
  """
  This is the main function that executes the script.
  """
  # Create S3 client
  s3_client = boto3.client('s3')

  # Get directory path for yesterday
  yesterday_directory = get_yesterday_directory()

  # List files in the S3 directory
  file_list = list_s3_files(s3_client, S3_BUCKET_NAME, yesterday_directory)

  # Create Pandas DataFrame from file list
  dataframe = create_dataframe(file_list)

  # Create RediStream records
  records = create_redistream_record(dataframe)

  # Logic to store records in RediStream (implementation depends on your RediStream library)
  # Replace this with your specific RediStream library call to store the 'records' list
  # For example (replace with your actual library):
  # your_redistream_library.store_data(REDISTREAM_KEY, records)

  print(f"Successfully processed {len(records)} files from S3 directory {yesterday_directory}.")


if __name__ == "__main__":
  main()


==============

import pandas as pd
import boto3
import os
import json
from datetime import datetime, timedelta

# Step 1: Read S3 directory for the previous day
def get_previous_day():
    today = datetime.now()
    previous_day = today - timedelta(days=1)
    return previous_day.strftime('%Y-%m-%d')

def list_s3_files(bucket_name, prefix):
    s3 = boto3.client('s3')
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    files = [obj['Key'] for obj in response.get('Contents', [])]
    return files

# Replace 'your-bucket-name' and 'your-prefix/' with your actual S3 bucket and prefix
bucket_name = 'your-bucket-name'
prefix = 'your-prefix/' + get_previous_day()

file_names = list_s3_files(bucket_name, prefix)

# Create a pandas DataFrame with file names and paths
file_Dataframe = pd.DataFrame({'file_name': file_names})

# Step 2: Read call.csv and agent-roaster.csv as pandas DataFrames
call_df = pd.read_csv(f's3://{bucket_name}/call.csv')
agent_df = pd.read_csv(f's3://{bucket_name}/agent-roaster.csv')

# Step 3: Join DataFrames on ID
merged_df = pd.merge(file_Dataframe, call_df, on='ID', how='inner')

# Step 4: Create JSON records
json_records = []
for _, row in merged_df.iterrows():
    record = {
        'id': str(uuid.uuid4()),
        'filename': row['file_name'],
        's3-file-path': f's3://{bucket_name}/{row["file_name"]}',
        'agent_racf': row['agent_racf'],
        'calldate': row['call_date']
    }
    json_records.append(record)

# Step 5: Store JSON records in Redis stream 'gwas-recordings'
try:
    # Your Redis connection code here
    # Example: r = redis.StrictRedis(host='localhost', port=6379, db=0)
    for record in json_records:
        r.xadd('gwas-recordings', record)
except Exception as e:
    print(f"Error storing records in Redis: {e}")

# Step 6: Use environment variables for S3 secret keys and root bucket directory
s3_access_key = os.getenv('S3_ACCESS_KEY')
s3_secret_key = os.getenv('S3_SECRET_KEY')
s3_root_bucket = os.getenv('S3_ROOT_BUCKET')

# Add try-catch block for exception handling
try:
    # Your S3 operations here
except Exception as e:
    print(f"Error accessing S3: {e}")



===========

import boto3
import json
import uuid
import pandas as pd
from datetime import date, timedelta
import os  # For environment variable access

# Load environment variables for security (replace with your actual mechanism)
try:
    AWS_ACCESS_KEY_ID = os.environ['AWS_ACCESS_KEY_ID']
    AWS_SECRET_ACCESS_KEY = os.environ['AWS_SECRET_ACCESS_KEY']
    S3_BUCKET_ROOT = os.environ['S3_BUCKET_ROOT']
except KeyError as e:
    print(f"Error: Missing environment variables. Please set {e} before running the script.")
    exit(1)

def get_yesterday_directory():
    """
    Calculates the directory path for the previous day in YYYY/MM/DD format.
    """
    today = date.today()
    yesterday = today - timedelta(days=1)
    return yesterday.strftime("%Y/%m/%d")

def list_s3_files(s3_client, bucket_name, directory):
    """
    Lists all files within the specified directory in the S3 bucket.

    Args:
        s3_client: A boto3 S3 client object.
        bucket_name: The name of the S3 bucket (excluding the root directory).
        directory: The directory path within the S3 bucket.

    Returns:
        A Pandas DataFrame containing a 'filename' column.
    """
    paginator = s3_client.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket_name, Prefix=directory, Delimiter='/')
    file_list = []
    for page in pages:
        if 'Contents' in page:
            file_list.extend([{'filename': file_info['Key']} for file_info in page['Contents']])
    return pd.DataFrame(file_list)

def read_s3_csv(s3_client, bucket_name, file_path):
    """
    Reads a CSV file from S3 as a Pandas DataFrame.

    Args:
        s3_client: A boto3 S3 client object.
        bucket_name: The name of the S3 bucket (excluding the root directory).
        file_path: The full path of the CSV file in S3 (including the bucket root).

    Returns:
        A Pandas DataFrame containing the CSV data.
    """
    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=file_path)
        df = pd.read_csv(response['Body'])
        return df
    except Exception as e:
        print(f"Error reading CSV from S3: {e}")
        return None

def create_redistream_record(dataframe):
    """
    Creates a list of JSON records with additional information for the RediStream.

    Args:
        dataframe: A Pandas DataFrame containing file, call, and combined data.

    Returns:
        A list of JSON records.
    """
    records = []
    for index, row in dataframe.iterrows():
        record = {
            'id': str(uuid.uuid4()),
            'filename': row['filename'],
            's3-file-pth': f"s3://{S3_BUCKET_ROOT}/{row['filename']}",
            'agentRAcf': row['agent_racf'] if 'agent_racf' in row else None,
            'calldate': row['calldate'] if 'calldate' in row else None
        }
        records.append(record)
    return records

def main():
    """
    Main function that executes the script.
    """
    # Create S3 client
    session = boto3.Session(aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    s3_client = session.client('s3')

    # Get directory path for yesterday in the S3 root directory
    yesterday_directory = get_yesterday_directory()
    s3_directory = f"{S3_BUCKET_ROOT}/{yesterday_directory}"

    # 1. List all files from yesterday's directory
    try:
        file_dataframe = list_s3_files(s3_client, S3_BUCKET_ROOT.split('/')[0], yesterday_directory)  
