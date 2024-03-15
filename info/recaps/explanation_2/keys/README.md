# Here we see the impact of different key distributions on the performance of the jobs

# We reduce the number of emulated sensors (this reduces the number of keys) and increase the generation rate to match the previous throughput

# Checkpoint is enabled at 60 seconds
# Window is 1 second

# Here we have cases with 100000keys at 2 records per second vs 50000keys at 4 records per second
# For 200000keys at 1 record per second, refer to checkpoints/Joins/60secs