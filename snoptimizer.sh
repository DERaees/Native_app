#!/bin/sh

WORKING_DIR=$(pwd)

echo "-----------------------------------------------------"
echo "----------------Step 1: Started----------------------"
echo "-----------------------------------------------------"

FILE_NAME="${WORKING_DIR}/create_codebase.sql"

snowsql -c its_nativeapps_provider -f "$FILE_NAME" -o exit_on_error=True

if [ $? != 0 ]; then
    echo "Step 1: Failed to create Codebase"
    exit 1
else
    echo "Step 1: Codebase Created"
fi
echo "-----------------------------------------------------"
echo "----------------Step 1: SUCCESS----------------------"
echo "-----------------------------------------------------"
echo " "
echo " "
echo " "
echo "-----------------------------------------------------"
echo "----------------Step 2: Started----------------------"
echo "-----------------------------------------------------"

FILE_NAME="readme.md"
PUT_COMMAND="PUT file://${FILE_NAME} @ITS_NATIVEAPPS.SNOPTIMIZER.SNOPTIMIZER_APP_STAGE AUTO_COMPRESS=FALSE OVERWRITE=TRUE;"
echo "$PUT_COMMAND"
snowsql -c its_nativeapps_provider -q "$PUT_COMMAND" -o exit_on_error=True

if [ $? != 0 ]; then
    echo "Step 2: Failed to upload File: $FILE_NAME"
    exit 1
else
    echo "Step 2: File Uploaded: $FILE_NAME"
fi


FILE_NAME="manifest.yml"
PUT_COMMAND="PUT file://${FILE_NAME} @ITS_NATIVEAPPS.SNOPTIMIZER.SNOPTIMIZER_APP_STAGE AUTO_COMPRESS=FALSE OVERWRITE=TRUE;"
echo "$PUT_COMMAND"
snowsql -c its_nativeapps_provider -q "$PUT_COMMAND" -o exit_on_error=True

if [ $? != 0 ]; then
    echo "Step 2: Failed to upload File: $FILE_NAME"
    exit 1
else
    echo "Step 2: File Uploaded: $FILE_NAME"
fi


FILE_NAME="setup_script.yml"
PUT_COMMAND="PUT file://${FILE_NAME} @ITS_NATIVEAPPS.SNOPTIMIZER.SNOPTIMIZER_APP_STAGE AUTO_COMPRESS=FALSE OVERWRITE=TRUE;"
echo "$PUT_COMMAND"
snowsql -c its_nativeapps_provider -q "$PUT_COMMAND" -o exit_on_error=True

if [ $? != 0 ]; then
    echo "Step 2: Failed to upload File: $FILE_NAME"
    exit 1
else
    echo "Step 2: File Uploaded: $FILE_NAME"
fi

# Build Snowflake PUT command
FILE_NAME="snoptimizer.py"
PUT_COMMAND="PUT file://${FILE_NAME} @ITS_NATIVEAPPS.SNOPTIMIZER.SNOPTIMIZER_APP_STAGE/streamlit AUTO_COMPRESS=FALSE OVERWRITE=TRUE;"
echo "$PUT_COMMAND"
# Run Snowflake PUT command using SnowSQL
snowsql -c its_nativeapps_provider -q "$PUT_COMMAND" -o exit_on_error=True

if [ $? != 0 ]; then
    echo "Step 2: Failed to upload File: $FILE_NAME"
    exit 1
else
    echo "Step 2: File Uploaded: $FILE_NAME"
fi

FILE_NAME="environment.yml"
PUT_COMMAND="PUT file://${FILE_NAME} @ITS_NATIVEAPPS.SNOPTIMIZER.SNOPTIMIZER_APP_STAGE/streamlit AUTO_COMPRESS=FALSE OVERWRITE=TRUE;"
echo "$PUT_COMMAND"
snowsql -c its_nativeapps_provider -q "$PUT_COMMAND" -o exit_on_error=True

if [ $? != 0 ]; then
    echo "Step 2: Failed to upload File: $FILE_NAME"
    exit 1
else
    echo "Step 2: File Uploaded: $FILE_NAME"
fi

echo "-----------------------------------------------------"
echo "----------------Step 2: SUCCESS----------------------"
echo "-----------------------------------------------------"
echo " "
echo " "
echo " "
echo "-----------------------------------------------------"
echo "----------------Step 3: Started----------------------"
echo "-----------------------------------------------------"
FILE_NAME="create_application_package.sql"
snowsql -c its_nativeapps_provider -f "$FILE_NAME" -o exit_on_error=True

if [ $? != 0 ]; then
    echo "Step 3: Failed to create Application Package"
    exit 1
else
    echo "Step 3: Application Package Created"
fi

echo "-----------------------------------------------------"
echo "----------------Step 3: SUCCESS----------------------"
echo "-----------------------------------------------------"