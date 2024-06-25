import boto3
from botocore.exceptions import ClientError


class S3Manager:
    """
    A class that encapsulates common S3 operations using boto3.

    Attributes:
        aws_access_key_id (str): Your AWS access key ID (optional, can be set from environment variables).
        aws_secret_access_key (str): Your AWS secret access key (optional, can be set from environment variables).
        region_name (str): The AWS region where your S3 bucket resides (optional, defaults to 'us-east-1').
    """

    def __init__(self, aws_access_key_id=None, aws_secret_access_key=None, region_name='us-east-1'):
        """
        Initializes the S3Manager object.

        Args:
            aws_access_key_id (str, optional): Your AWS access key ID. Defaults to None.
            aws_secret_access_key (str, optional): Your AWS secret access key. Defaults to None.
            region_name (str, optional): The AWS region where your S3 bucket resides. Defaults to 'us-east-1'.
        """

        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.region_name = region_name

        # Use session to manage credentials and region
        self.session = boto3.Session(
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
            region_name=self.region_name
        )
        self.s3_client = self.session.client('s3')

    def upload_file(self, local_file_path, bucket_name, object_key):
        """
        Uploads a file from the local filesystem to an S3 bucket.

        Args:
            local_file_path (str): The path to the local file to upload.
            bucket_name (str): The name of the S3 bucket to upload the file to.
            object_key (str): The key (name) of the object in the S3 bucket.

        Raises:
            ClientError: If an error occurs during the upload.
        """

        try:
            with open(local_file_path, 'rb') as file:
                self.s3_client.upload_fileobj(file, bucket_name, object_key)
            print(f"File '{local_file_path}' uploaded to S3 object '{object_key}' in bucket '{bucket_name}'.")
        except ClientError as e:
            raise ClientError(f"Error uploading file: {e}")

    def download_file(self, bucket_name, object_key, local_file_path):
        """
        Downloads a file from an S3 bucket to the local filesystem.

        Args:
            bucket_name (str): The name of the S3 bucket to download the file from.
            object_key (str): The key (name) of the object in the S3 bucket.
            local_file_path (str): The path to save the downloaded file on the local filesystem.

        Raises:
            ClientError: If an error occurs during the download.
        """

        try:
            self.s3_client.download_file(bucket_name, object_key, local_file_path)
            print(f"File downloaded from S3 object '{object_key}' in bucket '{bucket_name}' to '{local_file_path}'.")
        except ClientError as e:
            raise ClientError(f"Error downloading file: {e}")

# Example usage
if __name__ == '__main__':
    s3_manager = S3Manager()  # Use default credentials and region

    # Upload a file
    local_file_path = 'path/to/your/local/file.txt'
    bucket_name = 'your-bucket-name'
    object_key = 'file.txt'
    s3_manager.upload_file(local_file_path, bucket_name, object_key)

    # Download a file
    bucket_name = 'your-bucket-name'
    object_key = 'file.txt'
    local_file_


======

import boto3
import pandas as pd

def list_files_in_s3(bucket_name, prefix=''):
    """
    List files in an S3 bucket.

    Parameters:
    - bucket_name: str : Name of the S3 bucket
    - prefix: str : Prefix for the S3 keys (directory path)

    Returns:
    - list : List of file names
    """
    s3 = boto3.client('s3')
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    files = []
    
    for obj in response.get('Contents', []):
        files.append(obj['Key'])
    
    return files

def create_dataframe_from_s3(bucket_name, prefix=''):
    """
    Create a pandas DataFrame with a list of files from an S3 bucket.

    Parameters:
    - bucket_name: str : Name of the S3 bucket
    - prefix: str : Prefix for the S3 keys (directory path)

    Returns:
    - pd.DataFrame : DataFrame containing the list of files
    """
    files = list_files_in_s3(bucket_name, prefix)
    df = pd.DataFrame(files, columns=['File Names'])
    return df

# Example usage:
bucket_name = 'your-bucket-name'
prefix = 'your/prefix/path/'
df = create_dataframe_from_s3(bucket_name, prefix)
print(df)

========

import boto3
import pandas as pd
from io import StringIO

def read_csv_from_s3(bucket_name, file_key):
    """
    Read a CSV file from an S3 bucket and return a pandas DataFrame.

    Parameters:
    - bucket_name: str : Name of the S3 bucket
    - file_key: str : Key of the CSV file in the S3 bucket

    Returns:
    - pd.DataFrame : DataFrame containing the CSV data
    """
    s3 = boto3.client('s3')
    response = s3.get_object(Bucket=bucket_name, Key=file_key)
    csv_content = response['Body'].read().decode('utf-8')
    df = pd.read_csv(StringIO(csv_content))
    return df

def join_csv_files_from_s3(bucket_name, file_key1, file_key2):
    """
    Read two CSV files from an S3 bucket, create pandas DataFrames,
    and join them on the 'id' column.

    Parameters:
    - bucket_name: str : Name of the S3 bucket
    - file_key1: str : Key of the first CSV file in the S3 bucket
    - file_key2: str : Key of the second CSV file in the S3 bucket

    Returns:
    - pd.DataFrame : DataFrame containing the joined data
    """
    # Read the CSV files from S3
    df1 = read_csv_from_s3(bucket_name, file_key1)
    df2 = read_csv_from_s3(bucket_name, file_key2)

    # Join the DataFrames on the 'id' column
    merged_df = pd.merge(df1, df2, on='id', how='inner')
    return merged_df

# Example usage:
bucket_name = 'your-bucket-name'
file_key1 = 'path/to/your/first.csv'
file_key2 = 'path/to/your/second.csv'
merged_df = join_csv_files_from_s3(bucket_name, file_key1, file_key2)
print(merged_df)
