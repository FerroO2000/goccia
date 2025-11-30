#!/bin/bash

# Configuration
FILE="${1:-test.txt}"
INTERVAL="${2:-2}"  # seconds between appends

# Create file if it doesn't exist
touch "$FILE"

echo "Appending random strings to $FILE every $INTERVAL seconds"
echo "Press Ctrl+C to stop"
echo ""

counter=1
while true; do
    # Generate random string
    RANDOM_STRING=$(cat /dev/urandom | tr -dc 'a-zA-Z0-9' | fold -w 32 | head -n 1)
    
    # Append with timestamp
    TIMESTAMP=$(date '+%Y-%m-%d %H:%M:%S')
    echo "[$TIMESTAMP] $RANDOM_STRING" >> "$FILE"
    
    echo "[$counter] Appended: $RANDOM_STRING"
    ((counter++))
    
    sleep "$INTERVAL"
done