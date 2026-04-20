#!/bin/sh

INTERVAL=${1:-1}
FILTER=${2:-.}

while true; do
    OUTPUT=$(/usr/lib/rabbitmq/bin/rabbitmqctl list_queues name messages consumers state | grep -E "$FILTER" | grep -v "^Listing" | grep -v "^Timeout" | awk '!/^name/ && NF>=3 {printf("%-30s %-10s %-10s %s\n", $1, $2, $3, $4)}')
    printf '\033[2J\033[H'  # Clear screen and move cursor to top-left
    echo "RabbitMQ Queues - $(date)"
    echo '------------------------------------------------------------'
    echo 'QUEUE NAME                     MESSAGES   CONSUMERS  STATE'
    echo '------------------------------------------------------------'
    echo "$OUTPUT"
    sleep "$INTERVAL"
done