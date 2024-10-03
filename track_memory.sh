#!/bin/bash

# Output file
output_file="$1"

# Page size in bytes (16 KB)
page_size=16384

# Function to calculate used memory
calculate_used_memory() {
    vm_stat_output=$(vm_stat)

    # Extracting relevant values from vm_stat output
    pages_free=$(echo "$vm_stat_output" | grep "Pages free" | awk '{print $3}' | sed 's/\.//')
    pages_active=$(echo "$vm_stat_output" | grep "Pages active" | awk '{print $3}' | sed 's/\.//')
    pages_inactive=$(echo "$vm_stat_output" | grep "Pages inactive" | awk '{print $3}' | sed 's/\.//')
    pages_wired=$(echo "$vm_stat_output" | grep "Pages wired down" | awk '{print $4}' | sed 's/\.//')
    pages_compressed=$(echo "$vm_stat_output" | grep "Pages occupied by compressor" | awk '{print $5}' | sed 's/\.//')

    # Calculate used memory in MB
    active_memory=$((pages_active * page_size / 1024 / 1024))
    inactive_memory=$((pages_inactive * page_size / 1024 / 1024))
    wired_memory=$((pages_wired * page_size / 1024 / 1024))
    compressed_memory=$((pages_compressed * page_size / 1024 / 1024))

    used_memory=$((active_memory + inactive_memory + wired_memory + compressed_memory))

    # Get current timestamp
    timestamp=$(date +"%Y-%m-%d %H:%M:%S")

    # Write to output file
    echo "$timestamp : $used_memory MB"
    echo "$timestamp : $used_memory MB" >> "memory_logs/$output_file"
}

# Loop to execute the memory check every 1 second
while true; do
    calculate_used_memory
    sleep 1
done

