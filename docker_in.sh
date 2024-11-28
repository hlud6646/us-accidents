#! /usr/bin/env bash

# Find the pid of the sedona instance.
instance=$(docker ps | grep sedona | cut -d ' ' -f 1)

# Assert that there is exactly one sedona instance running.
n_instances=$(echo "$instance" | wc -l)
if [ "$n_instances" -eq 0 ]; then
    echo "Error: No sedona instance running."
    exit 1
elif [ "$n_instances" -gt 1 ]; then
    echo "Error: Multiple ($n_instances) sedona instances running."
    exit 1
fi

# Copy the data files accros.
docker cp ./data $instance:/opt/workspace/