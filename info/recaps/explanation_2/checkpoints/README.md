# Test the impact of checkpoints on the performance of the jobs

# We use different checkpoint intervals : no checkpoints, 10 seconds, 30 seconds, 1 minute.

## Map use case

- 2 sets of 100000 sensors (keys) are injected into the broker's topic 'input-topic1'.
- Each sensor emits 1 event every second.
- The job connects its source to the topic 'input-topic1'.

## Join use case

- 1 set of 100000 sensors (keys) is injected into the broker's topic 'input-topic1' and 1 set of 100000 sensors (keys)
  is injected into the broker's topic 'input-topic2'.
- Each sensor emits 1 event every second.
- The job connects its sources to the topics 'input-topic1' and 'input-topic2'.