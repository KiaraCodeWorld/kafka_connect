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
