import logging
from datetime import datetime

def log_message(log_level, message, date_format='%Y-%m-%d %H:%M:%S'):
    """
    Print a log message with the specified log level and current date/time.

    Parameters:
    - log_level: str : The level of the log (e.g., 'INFO', 'ERROR')
    - message: str : The message to log
    - date_format: str : The format for the date/time (default is '%Y-%m-%d %H:%M:%S')
    """
    # Get current date and time
    current_time = datetime.now().strftime(date_format)
    
    # Create the log message
    log_message = f"[{log_level}] {current_time}: {message}"

def extract_filename_from_path(path):
    """
    Extract the filename from a given path using string manipulation.

    Parameters:
    - path: str : The file path

    Returns:
    - str : The extracted filename
    """
    return path.split('/')[-1]

# Example usage:
path = 'some/directory/structure/filename.ext'
filename = extract_filename_from_path(path)
print(filename)  # Output: filename.ext
    
    
    # Print the log message
    print(log_message)

# Example usage:
log_message('INFO', 'This is an info message.')
log_message('ERROR', 'This is an error message.', date_format='%d-%m-%Y %H:%M:%S')
