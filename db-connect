"""

write python code with good comments and proper standards - 
which will 
0. setup env using dotenv and use only JDBC driver for connecting to databses
1. read the response from the redisstream "collector" - adn grab calldate / agent_racf from message
2. convert the calldate to UTC timestamp 
3. pass the agent racf to "sql-server" - agentTerminationCall table and join on calldate and agentRACF and get the RCID
4. pass the RCID from above step to DB2 member table and get the member CIP number.
5. add try / catch for exception handling 
6. return original message and member / with all the fields from sql-server, db2 and output stream "collector"



"""


REDIS_HOST=localhost
REDIS_PORT=6379
REDIS_STREAM=collector

SQL_SERVER_JDBC_DRIVER=/path/to/sqlserver/driver/sqljdbc42.jar
SQL_SERVER_JDBC_URL=jdbc:sqlserver://your_sql_server;databaseName=your_sql_database
SQL_SERVER_USERNAME=your_sql_username
SQL_SERVER_PASSWORD=your_sql_password

DB2_JDBC_DRIVER=/path/to/db2/driver/db2jcc4.jar
DB2_JDBC_URL=jdbc:db2://your_db2_hostname:50000/your_db2_database
DB2_USERNAME=your_db2_username
DB2_PASSWORD=your_db2_password
==========================

import os
import redis
import jaydebeapi
import pandas as pd
from datetime import datetime, timezone
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Function to read messages from Redis stream and process them
def process_redis_stream(redis_host, redis_port, redis_stream, sql_server_jdbc_driver, sql_server_jdbc_url, sql_server_username, sql_server_password, db2_jdbc_driver, db2_jdbc_url, db2_username, db2_password):
    try:
        # Initialize Redis client
        redis_client = redis.StrictRedis(host=redis_host, port=redis_port, decode_responses=True)
        
        # Variable to keep track of the last message ID processed
        last_id = '0-0'
        
        while True:
            try:
                # Read messages from Redis stream
                response = redis_client.xread({redis_stream: last_id}, block=5000, count=10)
                
                # Check if there are messages
                if response:
                    # Iterate over the response
                    for stream, messages in response:
                        for message_id, message in messages:
                            # Extract calldate and agent_racf from message
                            calldate = message.get('calldate')
                            agent_racf = message.get('agent_racf')
                            
                            if not agent_racf or not calldate:
                                print("Message does not contain 'calldate' or 'agent_racf' field")
                                continue

                            # Convert calldate to UTC timestamp
                            calldate_utc = convert_to_utc(calldate)

                            # Get RCID from SQL Server
                            rcid, sql_server_data = get_rcid_from_sql_server(sql_server_jdbc_driver, sql_server_jdbc_url, sql_server_username, sql_server_password, agent_racf, calldate_utc)
                            if rcid is None:
                                print(f"Failed to retrieve RCID for agent_racf: {agent_racf}")
                                continue
                            
                            # Get member CIP number from DB2
                            member_cip = get_member_cip_from_db2(db2_jdbc_driver, db2_jdbc_url, db2_username, db2_password, rcid)
                            if member_cip is None:
                                print(f"Failed to retrieve member CIP for RCID: {rcid}")
                                continue

                            # Create output message
                            output_message = message.copy()
                            output_message.update(sql_server_data)
                            output_message['rcid'] = rcid
                            output_message['member_cip'] = member_cip

                            # Publish output message to the same Redis stream
                            redis_client.xadd(redis_stream, output_message)
                            last_id = message_id
            
            except redis.RedisError as e:
                print(f"Redis error: {e}")
                break
            
            except Exception as e:
                print(f"Unexpected error: {e}")
                break

    except Exception as e:
        print(f"Failed to initialize Redis: {e}")

# Function to convert calldate to UTC timestamp
def convert_to_utc(calldate_str):
    try:
        local_datetime = datetime.strptime(calldate_str, "%Y-%m-%d %H:%M:%S")
        local_datetime = local_datetime.replace(tzinfo=timezone.utc)
        return local_datetime.strftime("%Y-%m-%d %H:%M:%S")
    except Exception as e:
        print(f"Error converting calldate to UTC: {e}")
        return None

# Function to get RCID and additional fields from SQL Server using agent_racf and calldate_utc
def get_rcid_from_sql_server(jdbc_driver, jdbc_url, username, password, agent_racf, calldate_utc):
    try:
        conn = jaydebeapi.connect("com.microsoft.sqlserver.jdbc.SQLServerDriver", jdbc_url, [username, password], jdbc_driver)
        query = """
        SELECT RCID, additional_field1, additional_field2 
        FROM agentTermination 
        WHERE agentRACF = ? AND calldate = ?
        """
        df = pd.read_sql(query, conn, params=(agent_racf, calldate_utc))
        conn.close()
        if not df.empty:
            row = df.iloc[0]
            rcid = row['RCID']
            additional_fields = row.to_dict()
            return rcid, additional_fields
        return None, None
    except Exception as e:
        print(f"SQL Server error: {e}")
        return None, None

# Function to get member CIP number from DB2 using RCID
def get_member_cip_from_db2(jdbc_driver, jdbc_url, username, password, rcid):
    try:
        conn = jaydebeapi.connect("com.ibm.db2.jcc.DB2Driver", jdbc_url, [username, password], jdbc_driver)
        query = "SELECT MemberCIP FROM memberTable WHERE RCID = ?"
        df = pd.read_sql(query, conn, params=(rcid,))
        conn.close()
        if not df.empty:
            return df.iloc[0]['MemberCIP']
        return None
    except Exception as e:
        print(f"DB2 error: {e}")
        return None

if __name__ == "__main__":
    # Load configuration from environment variables
    redis_host = os.getenv('REDIS_HOST')
    redis_port = int(os.getenv('REDIS_PORT'))
    redis_stream = os.getenv('REDIS_STREAM')

    sql_server_jdbc_driver = os.getenv('SQL_SERVER_JDBC_DRIVER')
    sql_server_jdbc_url = os.getenv('SQL_SERVER_JDBC_URL')
    sql_server_username = os.getenv('SQL_SERVER_USERNAME')
    sql_server_password = os.getenv('SQL_SERVER_PASSWORD')

    db2_jdbc_driver = os.getenv('DB2_JDBC_DRIVER')
    db2_jdbc_url = os.getenv('DB2_JDBC_URL')
    db2_username = os.getenv('DB2_USERNAME')
    db2_password = os.getenv('DB2_PASSWORD')

    # Start the process
    process_redis_stream(redis_host, redis_port, redis_stream, sql_server_jdbc_driver, sql_server_jdbc_url, sql_server_username, sql_server_password, db2_jdbc_driver, db2_jdbc_url, db2_username, db2_password)


===================
import os
import redis
import jaydebeapi
from datetime import datetime, timezone
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Function to read messages from Redis stream and process them
def process_redis_stream(redis_host, redis_port, redis_stream, sql_server_jdbc_driver, sql_server_jdbc_url, sql_server_username, sql_server_password, db2_jdbc_driver, db2_jdbc_url, db2_username, db2_password):
    try:
        # Initialize Redis client
        redis_client = redis.StrictRedis(host=redis_host, port=redis_port, decode_responses=True)
        
        # Variable to keep track of the last message ID processed
        last_id = '0-0'
        
        while True:
            try:
                # Read messages from Redis stream
                response = redis_client.xread({redis_stream: last_id}, block=5000, count=10)
                
                # Check if there are messages
                if response:
                    # Iterate over the response
                    for stream, messages in response:
                        for message_id, message in messages:
                            # Extract calldate and agent_racf from message
                            calldate = message.get('calldate')
                            agent_racf = message.get('agent_racf')
                            
                            if not agent_racf or not calldate:
                                print("Message does not contain 'calldate' or 'agent_racf' field")
                                continue

                            # Convert calldate to UTC timestamp
                            calldate_utc = convert_to_utc(calldate)

                            # Get RCID from SQL Server
                            rcid, sql_server_data = get_rcid_from_sql_server(sql_server_jdbc_driver, sql_server_jdbc_url, sql_server_username, sql_server_password, agent_racf, calldate_utc)
                            if rcid is None:
                                print(f"Failed to retrieve RCID for agent_racf: {agent_racf}")
                                continue
                            
                            # Get member CIP number from DB2
                            member_cip = get_member_cip_from_db2(db2_jdbc_driver, db2_jdbc_url, db2_username, db2_password, rcid)
                            if member_cip is None:
                                print(f"Failed to retrieve member CIP for RCID: {rcid}")
                                continue

                            # Create output message
                            output_message = message.copy()
                            output_message.update(sql_server_data)
                            output_message['rcid'] = rcid
                            output_message['member_cip'] = member_cip

                            # Publish output message to the same Redis stream
                            redis_client.xadd(redis_stream, output_message)
                            last_id = message_id
            
            except redis.RedisError as e:
                print(f"Redis error: {e}")
                break
            
            except Exception as e:
                print(f"Unexpected error: {e}")
                break

    except Exception as e:
        print(f"Failed to initialize Redis: {e}")

# Function to convert calldate to UTC timestamp
def convert_to_utc(calldate_str):
    try:
        local_datetime = datetime.strptime(calldate_str, "%Y-%m-%d %H:%M:%S")
        local_datetime = local_datetime.replace(tzinfo=timezone.utc)
        return local_datetime.strftime("%Y-%m-%d %H:%M:%S")
    except Exception as e:
        print(f"Error converting calldate to UTC: {e}")
        return None

# Function to get RCID and additional fields from SQL Server using agent_racf and calldate_utc
def get_rcid_from_sql_server(jdbc_driver, jdbc_url, username, password, agent_racf, calldate_utc):
    try:
        conn = jaydebeapi.connect("com.microsoft.sqlserver.jdbc.SQLServerDriver", jdbc_url, [username, password], jdbc_driver)
        cursor = conn.cursor()
        query = """
        SELECT RCID, additional_field1, additional_field2 
        FROM agentTerminationCall 
        WHERE agentRACF = ? AND calldate = ?
        """
        cursor.execute(query, (agent_racf, calldate_utc))
        row = cursor.fetchone()
        conn.close()
        if row:
            rcid, additional_field1, additional_field2 = row
            return rcid, {'additional_field1': additional_field1, 'additional_field2': additional_field2}
        return None, None
    except Exception as e:
        print(f"SQL Server error: {e}")
        return None, None

# Function to get member CIP number from DB2 using RCID
def get_member_cip_from_db2(jdbc_driver, jdbc_url, username, password, rcid):
    try:
        conn = jaydebeapi.connect("com.ibm.db2.jcc.DB2Driver", jdbc_url, [username, password], jdbc_driver)
        cursor = conn.cursor()
        query = "SELECT MemberCIP FROM memberTable WHERE RCID = ?"
        cursor.execute(query, (rcid,))
        row = cursor.fetchone()
        conn.close()
        if row:
            return row[0]
        return None
    except Exception as e:
        print(f"DB2 error: {e}")
        return None

if __name__ == "__main__":
    # Load configuration from environment variables
    redis_host = os.getenv('REDIS_HOST')
    redis_port = int(os.getenv('REDIS_PORT'))
    redis_stream = os.getenv('REDIS_STREAM')

    sql_server_jdbc_driver = os.getenv('SQL_SERVER_JDBC_DRIVER')
    sql_server_jdbc_url = os.getenv('SQL_SERVER_JDBC_URL')
    sql_server_username = os.getenv('SQL_SERVER_USERNAME')
    sql_server_password = os.getenv('SQL_SERVER_PASSWORD')

    db2_jdbc_driver = os.getenv('DB2_JDBC_DRIVER')
    db2_jdbc_url = os.getenv('DB2_JDBC_URL')
    db2_username = os.getenv('DB2_USERNAME')
    db2_password = os.getenv('DB2_PASSWORD')

    # Start the process
    process_redis_stream(redis_host, redis_port, redis_stream, sql_server_jdbc_driver, sql_server_jdbc_url, sql_server_username, sql_server_password, db2_jdbc_driver, db2_jdbc_url, db2_username, db2_password)

===============

import os
import json
from datetime import datetime, timezone
import pytz  # for timezone conversion
import java.sql  # for JDBC connection
import pandas as pd
from dotenv import load_dotenv
import redis

# Load environment variables from a `.env` file
load_dotenv()

# Redis connection details from environment variables
REDIS_HOST = os.getenv('REDIS_HOST')
REDIS_PORT = int(osgetenv('REDIS_PORT'))
REDIS_PASSWORD = os.getenv('REDIS_PASSWORD')

# SQL Server connection details from environment variables
SQLSERVER_JDBC_DRIVER = os.getenv('SQLSERVER_JDBC_DRIVER')
SQLSERVER_URL = osgetenv('SQLSERVER_URL')
SQLSERVER_USERNAME = os.getenv('SQLSERVER_USERNAME')
SQLSERVER_PASSWORD = osgetenv('SQLSERVER_PASSWORD')

# DB2 connection details from environment variables
DB2_JDBC_DRIVER = osgetenv('DB2_JDBC_DRIVER')
DB2_URL = osgetenv('DB2_URL')
DB2_USERNAME = osgetenv('DB2_USERNAME')
DB2_PASSWORD = osgetenv('DB2_PASSWORD')

# Redis stream names
SOURCE_STREAM = "collector"
TARGET_STREAM = "collector"  # Output stream (same as input)

# SQL queries
SQLSERVER_QUERY = "SELECT RCID FROM agentTerminationCall WHERE calldate = ? AND agentRACF = ?"
DB2_QUERY = "SELECT memberCIP FROM member WHERE RCID = ?"


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


def get_jdbc_connection(url, driver, username, password):
    """
    Establishes a JDBC connection to the specified database.

    Args:
        url: The database connection URL.
        driver: The JDBC driver class name.
        username: The database username.
        password: The database password.

    Returns:
        A JDBC connection object.
    """
    try:
        java.lang.Class.forName(driver)
        conn = java.sql.DriverManager.getConnection(url, username, password)
        return conn
    except Exception as e:
        print(f"Error connecting to database: {e}")
        return None


def convert_to_utc_timestamp(calldate):
    """
    Converts the calldate string to a UTC timestamp.

    Args:
        calldate: The calldate string in the format expected by the database.

    Returns:
        A UTC timestamp object.
    """
    # Assuming calldate format is already in a recognized format
    # (modify if needed based on your database's format)
    datetime_obj = datetime.strptime(calldate, "%Y-%m-%d %H:%M:%S")
    return datetime_obj.astimezone(timezone.utc)


def process_message(redis_client, message):
    """
    Processes a message from the Redis stream, retrieves data from databases
    using pandas DataFrames, and enriches the message before writing back to output stream.

    Args:
        redis_client: A Redis client object.
        message: The message data from the Redis stream.
    """
    data = json.loads(message.decode('utf-8'))
    calldate = data.get('calldate')
    agent_racf = data.get('agent_racf')

    if not calldate or not agent_racf:
        print(f"Missing required fields in message: {message}")
        return

    # Convert calldate to UTC timestamp
    utc_calldate = convert_to_utc_timestamp(calldate)

    # Connect to SQL Server and DB2
    sqlserver_conn = None
    db2_conn = None
    try:
        sqlserver_conn = get_jdbc_connection(
            SQLSERVER_URL, SQLSERVER_JDBC_DRIVER, SQLSERVER_USERNAME, SQLSERVER_PASSWORD
        )
        db2_conn = get_jdbc_connection(DB2_URL, DB2_JDBC_DRIVER, DB2_USERNAME, DB2_


============

import os
import pandas as pd
import redis
import pyodbc
from datetime import datetime

# Step 0: Set up environment using dotenv
from dotenv import load_dotenv
load_dotenv()

# Step 1: Read response from the Redis stream "collector"
def read_redis_stream():
    r = redis.Redis(host=os.getenv("REDIS_HOST"), port=int(os.getenv("REDIS_PORT")), db=0)
    stream_name = "collector"
    messages = r.xrange(stream_name)
    return messages

# Step 2: Convert calldate to UTC timestamp
def convert_to_utc_timestamp(calldate):
    local_datetime = datetime.strptime(calldate, "%Y-%m-%d %H:%M:%S")
    utc_datetime = local_datetime.astimezone(datetime.timezone.utc)
    return int(utc_datetime.timestamp())

# Step 3: Pass agent RACF to SQL Server - agentTerminationCall table
def get_rcid(agent_racf):
    conn_str = f"Driver={{ODBC Driver 17 for SQL Server}};Server={os.getenv('SQL_SERVER_HOST')};Database={os.getenv('SQL_SERVER_DATABASE')};UID={os.getenv('SQL_SERVER_USER')};PWD={os.getenv('SQL_SERVER_PASSWORD')};"
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    query = f"SELECT RCID FROM agentTerminationCall WHERE agentRACF = '{agent_racf}'"
    cursor.execute(query)
    rcid = cursor.fetchone()[0]
    conn.close()
    return rcid

# Step 4: Pass RCID to DB2 member table
def get_member_cip(rcid):
    # Add your DB2 connection details here
    # Example: conn = pyodbc.connect("DSN=DB2;UID=myuser;PWD=mypassword;")
    # Execute query to get member CIP based on RCID
    # Example: query = f"SELECT CIP FROM member WHERE RCID = {rcid}"
    # Return the member CIP

# Step 5: Add try/catch for exception handling
try:
    messages = read_redis_stream()
    for message in messages:
        calldate, agent_racf = message["calldate"], message["agent_racf"]
        utc_timestamp = convert_to_utc_timestamp(calldate)
        rcid = get_rcid(agent_racf)
        member_cip = get_member_cip(rcid)
        print(f"Original message: {message}, Member CIP: {member_cip}")
except Exception as e:
    print(f"Error: {str(e)}")

# Step 6: Return original message and member CIP


=============

import os
from dotenv import load_dotenv
import pandas as pd
import pyodbc
import redis
from datetime import datetime, timezone

# Load environment variables from .env file
load_dotenv()

# Set up Redis connection
r = redis.Redis(host=os.environ.get('REDIS_HOST'), port=os.environ.get('REDIS_PORT'), db=0)

# Set up SQL Server connection
sql_server_conn = pyodbc.connect(
   r'DRIVER={ODBC Driver 17 for SQL Server};'
   r'SERVER=' + os.environ.get('SQL_SERVER_HOST') + ';'
   r'DATABASE=' + os.environ.get('SQL_SERVER_DB') + ';'
   r'UID=' + os.environ.get('SQL_SERVER_USER') + ';'
   r'PWD=' + os.environ.get('SQL_SERVER_PASSWORD')
)

# Set up DB2 connection
db2_conn = pyodbc.connect(
   r'DRIVER={IBM DB2 ODBC DRIVER};'
   r'HOSTNAME=' + os.environ.get('DB2_HOST') + ';'
   r'PORT=' + os.environ.get('DB2_PORT') + ';'
   r'DATABASE=' + os.environ.get('DB2_DB') + ';'
   r'UID=' + os.environ.get('DB2_USER') + ';'
   r'PWD=' + os.environ.get('DB2_PASSWORD') + ';'
)

def process_message(message):
   try:
       # Read the response from the Redis stream "collector"
       call_date, agent_racf = message['data'].decode().split('|')

       # Convert the call date to UTC timestamp
       call_date_utc = datetime.strptime(call_date, '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc)

       # Get the RCID from SQL Server
       sql_server_query = "SELECT RCID FROM agentTerminationCall WHERE callDate = ? AND agentRACF = ?"
       sql_server_df = pd.read_sql_query(sql_server_query, sql_server_conn, params=[call_date_utc, agent_racf])
       rcid = sql_server_df['RCID'].values[0]

       # Get the member CIP number from DB2
       db2_query = "SELECT CIP FROM Member WHERE RCID = ?"
       db2_df = pd.read_sql_query(db2_query, db2_conn, params=[rcid])
       cip = db2_df['CIP'].values[0]

       # Return the original message and member information
       return {
           'original_message': message['data'].decode(),
           'callDate': call_date,
           'agentRACF': agent_racf,
           'RCID': rcid,
           'CIP': cip
       }

   except Exception as e:
       # Handle exceptions
       print(f"Error processing message: {e}")
       return None

# Example usage
message = {'data': b'2023-06-15 10:30:00|ABC123'}
result = process_message(message)
if result:
   print(result)

