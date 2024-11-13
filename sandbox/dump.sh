#!/bin/bash

# Check if an index name is provided
if [ -z "$1" ]; then
  echo "Usage: $0 <index_name>"
  exit 1
fi

# Set the index name from the first argument
INDEX_NAME=$1
DUMP_FILE="./${INDEX_NAME}_dump.json"

# Run elasticdump with the specified index and output file
elasticdump \
  --input="http://localhost:9200/${INDEX_NAME}" \
  --output="${DUMP_FILE}" \
  --type=data

if [ $? -eq 0 ]; then
  echo "Data dump for index '${INDEX_NAME}' saved to ${DUMP_FILE}"
else
  echo "ERROR: Failed to dump data for index '${INDEX_NAME}'."
fi
