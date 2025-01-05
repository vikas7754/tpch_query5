#!/bin/bash

# Set variables
SOURCE_FILE="main.cpp"        # Path to the main C++ file
EXECUTABLE="tpch_query5"      # Name of the output executable
DATA_PATH=${1:-"./data"}            # Path to the data directory
REGION_NAME=${2:-"ASIA"}            # Region filter
START_DATE=${3:-"1994-01-01"}       # Start date for orders
END_DATE=${4:-"1995-01-01"}         # End date for orders
NUM_THREADS=${5:-8}                 # Number of threads to use
RESULT_DIR=${6:-"result"}

RESULT_FILE="$RESULT_DIR/result.tbl"
# Ensure the result directory exists
mkdir -p "$RESULT_DIR"

# Compile the program
echo "Compiling the project..."
g++ "$SOURCE_FILE" -o "$EXECUTABLE" -std=c++17 -pthread
if [ $? -ne 0 ]; then
    echo "Compilation failed! Exiting."
    exit 1
fi
echo "Compilation successful."

# Run the program
echo "Running the project..."
./"$EXECUTABLE" "$DATA_PATH" "$REGION_NAME" "$START_DATE" "$END_DATE" "$NUM_THREADS" "$RESULT_DIR"
if [ $? -ne 0 ]; then
    echo "Execution failed! Exiting."
    exit 1
fi
echo "Execution successful."