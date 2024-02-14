# Test the impact of kafka partitions on the performance of the system

## Test Description

- Map operator with fibonacci value of 14
- 200k records/s injected at the source (2 load generators emulating each 100k sensors)
- No checkpoint

### Test 1 - 1 partition

![Test with 1 partition](1partition.png)

Subtasks handle the same amount of workload.

### Test 2 - 1000 partitions

Higher throughput achieved at parallelism 1. 
At parallelism 2, subtasks handle different amount of workload.

![Test with 1000 partition](1000partitions.png)