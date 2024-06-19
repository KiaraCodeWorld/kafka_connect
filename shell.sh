write shell script : 
		1. unzip the zip files having individual .wav files in it
		2. from .zip file name "ACT_YYYYMMDD.zip" - grab the date part and  create date folder in S3 - as destintion pth
		3. start copying the unzipped .wav files to above folder.
		4. add comments and capture the return code to capture error or stop processing if failre.

#!/bin/bash

# Function to extract the date from the filename (assuming valid format)
get_date_from_filename() {
  filename="$1"
  match=$(echo "$filename" | grep -E '^ACT_([0-9]{8})\.zip$')
  if [[ ! "$match" ]]; then
    echo "Invalid zip filename format: $filename" >&2
    return 1
  fi
  date_str=${match:4}
  echo "${date_str:0:4}/${date_str:4:2}/${date_str:6:2}"
}

# Main script body
for zipfile in *.zip; do
  # Extract date from filename
  date_folder=$(get_date_from_filename "$zipfile")
  if [[ $? -ne 0 ]]; then
    echo "Skipping invalid file: $zipfile"
    continue
  fi

  # Unzip the file
  unzip -q "$zipfile" || {
    echo "Error unzipping $zipfile, exiting..."
    exit 1
  }

  # Construct S3 destination path (modify 'your-bucket' as needed)
  s3_path="s3://your-bucket/recordings/$date_folder"

  # Copy .wav files to S3 using aws cli (modify aws CLI command based on your needs)
  aws s3 cp --recursive ./*.wav "$s3_path" || {
    echo "Error copying files to S3 from $zipfile, exiting..."
    exit 1
  }

  # Clean up unzipped files (optional)
  rm -f ./*.wav
done

echo "Script completed!"

=======

#!/bin/bash

# Function to handle errors
handle_error() {
    echo "Error: $1"
    exit 1
}

# Directory where the zip files are located
ZIP_DIR="/path/to/zip/files"
# Temporary directory for unzipping files
TMP_DIR="/tmp/unzipped_files"

# Ensure the temporary directory exists
mkdir -p "$TMP_DIR"

# Iterate over all zip files in the directory
for zip_file in "$ZIP_DIR"/*.zip; do
    # Check if zip files exist
    if [[ ! -f "$zip_file" ]]; then
        handle_error "No zip files found in $ZIP_DIR"
    fi

    # Extract the date part from the zip file name (e.g., ACT_YYYYMMDD.zip)
    base_name=$(basename "$zip_file" .zip)
    date_part=${base_name#ACT_}

    # Create the destination path in S3
    S3_BUCKET="s3://your-s3-bucket"
    DEST_PATH="$S3_BUCKET/$date_part/"

    # Unzip the file into the temporary directory
    unzip -o "$zip_file" -d "$TMP_DIR" || handle_error "Failed to unzip $zip_file"

    # Copy the unzipped .wav files to the S3 destination path
    for wav_file in "$TMP_DIR"/*.wav; do
        if [[ -f "$wav_file" ]]; then
            aws s3 cp "$wav_file" "$DEST_PATH" || handle_error "Failed to upload $wav_file to $DEST_PATH"
        fi
    done

    # Clean up the temporary directory
    rm -f "$TMP_DIR"/*.wav
done

echo "All files processed successfully."

==================


#!/bin/bash

# Set the S3 bucket and path
S3_BUCKET="your-s3-bucket-name"
S3_PATH="/path/in/your/bucket"

# Function to handle errors
handle_error() {
    local error_code=$?
    local error_message=$1

    if [ $error_code -ne 0 ]; then
        echo "Error: $error_message (error code: $error_code)"
        exit $error_code
    fi
}

# Loop through all .zip files in the current directory
for zip_file in *.zip; do
    # Extract the date from the zip file name (e.g., ACT_20230619.zip)
    date=$(echo "$zip_file" | sed 's/^.*_\([0-9]\{8\}\)\.zip$/\1/')
    date_folder=$(date -d "$date" +"%Y%m%d")  # Convert to YYYYMMDD format

    # Create the date folder in S3
    aws s3 mb "s3://$S3_BUCKET/$S3_PATH/$date_folder" || handle_error "Failed to create S3 folder for $date_folder"

    # Create the /mp3 folder under the date folder
    aws s3 mb "s3://$S3_BUCKET/$S3_PATH/$date_folder/mp3" || handle_error "Failed to create /mp3 folder for $date_folder"

    # Unzip the file
    unzip -o "$zip_file" || handle_error "Failed to unzip $zip_file"

    # Copy .wav files to S3
    for wav_file in *.wav; do
        aws s3 cp "$wav_file" "s3://$S3_BUCKET/$S3_PATH/$date_folder/$wav_file" || handle_error "Failed to copy $wav_file to S3"
    done

    # Clean up unzipped files
    rm *.wav
done

========
