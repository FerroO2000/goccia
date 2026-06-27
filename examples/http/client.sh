#!/bin/bash

INTERVAL="${1:-1}"

if ! [[ "$INTERVAL" =~ ^[0-9]+([.][0-9]+)?$ ]] || ! awk -v interval="$INTERVAL" 'BEGIN { exit !(interval > 0) }'; then
    echo "period must be a positive number" >&2
    exit 1
fi

trap 'echo ""; echo "Stopped"; exit 0' INT TERM

echo "Sending an echo request every $INTERVAL seconds"
echo "Press Ctrl+C to stop"
echo ""

counter=1
while true; do
    payload="hello from goccia ($counter)"

    echo "[$counter] POST /echo"
    curl --silent --show-error --include \
        --request POST http://localhost:8080/echo \
        --header "Content-Type: text/plain" \
        --data "$payload"
    echo ""

    ((counter++))
    sleep "$INTERVAL"
done
