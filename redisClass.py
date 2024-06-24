import redis

class RedisStreamer:
    """
    A class that encapsulates common Redis Stream operations.

    Attributes:
        host (str): The hostname or IP address of the Redis server (defaults to 'localhost').
        port (int): The port number of the Redis server (defaults to 6379).
        db (int): The Redis database ID to use (defaults to 0).
        redis_client (redis.Redis): The connected Redis client object.
    """

    def __init__(self, host='localhost', port=6379, db=0):
        """
        Initializes the RedisStreamer object.

        Args:
            host (str, optional): The hostname or IP address of the Redis server. Defaults to 'localhost'.
            port (int, optional): The port number of the Redis server. Defaults to 6379.
            db (int, optional): The Redis database ID to use. Defaults to 0.
        """

        self.host = host
        self.port = port
        self.db = db

        self.redis_client = redis.Redis(host=self.host, port=self.port, db=self.db)

    def connect(self):
        """
        Connects to the Redis server.

        Raises:
            ConnectionError: If a connection error occurs.
        """

        try:
            self.redis_client.ping()
            print("Connected to Redis server successfully.")
        except redis.exceptions.ConnectionError as e:
            raise ConnectionError(f"Failed to connect to Redis server: {e}") from e

    def write_to_stream(self, stream_name, data_dict):
        """
        Writes a dictionary of data to the specified Redis Stream.

        Args:
            stream_name (str): The name of the Redis Stream to write to.
            data_dict (dict): The dictionary containing the data to be written.

        Raises:
            redis.exceptions.ConnectionError: If a connection error occurs.
        """

        try:
            self.redis_client.xadd(stream_name, data_dict)
            print(f"Data written to stream '{stream_name}'.")
        except redis.exceptions.ConnectionError as e:
            raise redis.exceptions.ConnectionError(f"Error writing to stream: {e}") from e

    def read_from_stream(self, stream_name, count=1, block=0):
        """
        Reads a specified number of messages from the Redis Stream in blocking or non-blocking mode.

        Args:
            stream_name (str): The name of the Redis Stream to read from.
            count (int, optional): The number of messages to read. Defaults to 1.
            block (int, optional): The amount of time (in milliseconds) to block for a message. 
                                    0 for non-blocking, positive value for blocking. Defaults to 0.

        Returns:
            list: A list of dictionaries containing the read messages, or an empty list if no messages are found.

        Raises:
            redis.exceptions.ConnectionError: If a connection error occurs.
        """

        try:
            messages = self.redis_client.xrange(stream_name, block=block, count=count)
            return [message[1] for message in messages]  # Extract data dictionaries from messages
        except redis.exceptions.ConnectionError as e:
            raise redis.exceptions.ConnectionError(f"Error reading from stream: {e}") from e

    def check_consumer_group_exists(self, stream_name, group_name):
        """
        Checks if a Consumer Group with the given name exists for the specified Redis Stream.

        Args:
            stream_name (str): The name of the Redis Stream to check.
            group_name (str): The name of the Consumer Group to look for.

        Returns:
            bool: True if the Consumer Group exists, False otherwise.

        Raises:
            redis.exceptions.ConnectionError: If a connection error occurs.
        """

        try:
            return self.redis_client.xgroup_info(stream_name, group_name) is not None
        except redis.exceptions.ConnectionError as e:
            raise redis.exceptions.ConnectionError(f"Error checking Consumer Group: {e}") from e
