# A collection of experiments to define the behavior of stateful rescaling

## [1. kafka partitions](kafka_partitions/README.md)

Description of the experiment:

> Verify the impact on throughput with different number of kafka partitions: 1 partition vs 1000 partitions.

## [2. checkpoints](checkpoints/README.md)

Description of the experiment:

> Verify the impact on state and throughput with different checkpoint intervals: no checkpoints, 10 seconds, 30 seconds,
> 1 minute.

## [3. size of a window](window/README.md)

Description of the experiment:

> Verify the impact that the size of the window has on the throughput and state of the job. 1000ms vs 5000ms.

## [4. amount of keys](keys/README.md)

Description of the experiment:

> Verify the impact that the number of keys has on the throughput during rescale. Key redistribution can introduce data
> skewness.
> 200000 keys at 1 record per second vs 100000 keys at 2 records per second vs 50000 keys at 4 records per second.