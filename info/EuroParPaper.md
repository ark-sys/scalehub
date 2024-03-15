# Challenges in modeling performance of elastic stream processing applications

Key points:
- Operators don't behave the same depending on their type (stateless, like map, filter, etc., or stateful, like windowed aggregations, joins, etc.)
- latencies create uncertainty in the performance during rescale
- jitter adds performance loss

What we know:
- The performance of stateless operators is not affected by the number of instances
- Staless operators throughput scales linearly with the number of instances

What we want to show:
- The performance of stateful operators is affected by the number of instances
- Variation in throughput is hard to model when latency impacts stateful scaled operators
- Jitter adds performance loss
- There is a challenge in modeling the performance of elastic stream processing applications

Experiments:
- We propose three use case with two pipelines, one with stateless operator (Map) and two with stateful operators (Windowed join key-key and Windowed join key-value)
- We propose three network conditions: no latency, 25ms latency, and 25ms latency with 10ms jitter
- We categorize each expriment under three labels: absolute performance, scalability, and predictability

Results:
- Map
  - No latency: throughput scales linearly with the number of instances. Variation in throughput is low. Each parallelism level constantly adds around 90% of throughput.
  - 25ms latency: throughput scales linearly with the number of instances. Variation in throughput is low. Each parallelism level constantly adds around 90% of throughput.
  - 25ms latency with 10ms jitter: throughput scales linearly with the number of instances. Variation in throughput is low. Each parallelism level constantly adds around 90% of throughput.
  For all network conditions, the throughput is predictable by the model, with errors below 10%.
- Windowed join key-key
  - No latency: throughput scales linearly with the number of instances. As replicas increase, variation in throughput increases. Each parallelism level adds on average 70% of throughput.
  - 25ms latency: throughput does not scale linearly with the number of instances. As replicas increase, variation in throughput increases. Variations in throughput are hard to predict. The overall throughput decreases when more replicas with latency are added: on nodes not affected by latency, the added throughput per parallelism level is around 70%. But when the application is scaled on nodes with latency, the added throughput per parallelism level is between 30-50%.
  - 25ms latency with 10ms jitter: throughput does not scale linearly with the number of instances. Less throughput variation than 25ms latency. More throughput degradation than 25ms latency. When the application is scaled on nodes with latency, the added throughput per parallelism level is around 30%.
  With no latency, the throughput is predictable by the model, with errors around 10%. With latency, and especially with jitter, the throughput is hard to predict, with predictions that diverge from the actual throughput by more than 20%.
- Windowed join key-value
  - No latency: throughput scales linearly with the number of instances. As replicas increase, variation in throughput increases. Each parallelism level adds on average 50% of throughput.
  - 25ms latency: throughput does not scale linearly with the number of instances. As replicas increase, variation in throughput increases, especially when latency is present. The overall throughput decreases when more replicas with latency are added. Variations in throughput are hard to predict. When the application is scaled on nodes with latency, the added throughput per parallelism level is around 20%.
  - 25ms latency with 10ms jitter: throughput does not scale linearly with the number of instances. Less throughput variation than 25ms latency. When the application is scaled on nodes with latency, the added throughput per parallelism level is around 30%.
  With no latency, the throughput is hard to predict, with big variations of around 30% from the model, especially with higher parallelism levels. With latency, and especially with jitter, the throughput is hard to predict, with predictions that diverge from the actual throughput by more than 40%.

| Operator | Network Condition | Throughput Scaling | Variation in Throughput | Added Throughput per Parallelism Level | Prediction Error |
| --- | --- | --- | --- | --- |-----------------|
| Map | No latency | Linear | Low | ~90% | <10%            |
| Map | 25ms latency | Linear | Low | ~90% | <10%            |
| Map | 25ms latency with 10ms jitter | Linear | Low | ~90% | <10%            |
| Windowed join key-key | No latency | Linear | Increases with replicas | ~70% | ~10%            |
| Windowed join key-key | 25ms latency | Non-linear | Increases with replicas | 30-50% | \>25%           |
| Windowed join key-key | 25ms latency with 10ms jitter | Non-linear | Increases with replicas | ~30% | \>30%           |
| Windowed join key-value | No latency | Linear | Increases with replicas | ~50% | ~30%     |
| Windowed join key-value | 25ms latency | Non-linear | Increases with replicas | ~20% | >40%            |
| Windowed join key-value | 25ms latency with 10ms jitter | Non-linear | Increases with replicas | ~30% | >40%            |


Discussion:
> In our study, we focus on three characteristics that denote the performance of elastic stream processing: absolute performance, scalability, and predictability. These characteristics can help describe the performance of stream processing applications under different network conditions and to identify the challenges in modeling their performance.
>
> Based on the experimental results and observations, it can be concluded that the performance of stateful operators in elastic stream processing applications is significantly impacted by the number of instances. This impact becomes more complex to model when latency is introduced into the system, further complicating the scalability of stateful operators.
>
> In the case of the Windowed join key-key operator, the throughput scales linearly with the number of instances under no latency, with each parallelism level adding on average 70\% of throughput. However, when latency is introduced, the throughput does not scale linearly and the added throughput per parallelism level drops to between 30-50\%. This variation in throughput becomes even more pronounced with the addition of jitter, where the added throughput per parallelism level is around 30\%.
> 
> Similarly, for the Windowed join key-value operator, the throughput scales linearly with the number of instances under no latency, with each parallelism level adding on average 50\% of throughput. However, when latency is introduced, the throughput does not scale linearly and the added throughput per parallelism level drops to around 20\%. This variation in throughput becomes even more pronounced with the addition of jitter, where the added throughput per parallelism level is around 30\%.
> 
> While stateless operators exhibit predictable and scalable performance, stateful operators are more challenging to model and predict. This is especially true when latency and jitter are introduced into the system.
> 
> The introduction of latency into the system causes high variation in throughput, making it difficult to predict the performance of stateful operators. With added jitter, the system incurs into additional performance degradation. This phenomenon is especially pronounced in geo-distributed environments where latency and jitter are inherent characteristics.
> 
> Therefore, it is evident that there exists a substantial challenge in accurately modeling the performance of elastic stream processing applications, particularly when stateful operators are involved and under heterogeneous network conditions. The GESSCALE model provides predictions for each future level of parallelism, but the error in these predictions can be significant, especially under conditions of latency and jitter. For example, in the case of the Windowed join key-key operator, the predictions diverge from the actual throughput by more than 20\% under conditions of latency and jitter. Similarly, for the Windowed join key-value operator, the predictions diverge from the actual throughput by more than 40\% under the same conditions. These results highlight the need for more accurate models that can better account for the impact of network conditions on the performance of elastic stream processing applications.

Remarks:
- latency impacts the arrival of records, which impacts how much the network buffers are filled, which impacts the busyness of the operators (and backpressure of the upstreams), which impacts the throughput
- depending on how replicas are distributed on nodes, network buffers, busyness of the operators, and backpressure are impacted differently
- for stateful use cases, depending on the available number of nodes, the throughput is impacted by the majority of a certain type of nodes (latency or no latency)

Observations:
- Adding replicas of stateful operators, on nodes with latency, decreases the overall throughput
- Many scaling techniques rely on backpressure to trigger the scaling, but with latency, the backpressure is not a reliable signal. Latency makes throughput, backpressure and busyness of the operators drop, which makes the system not scale as expected.

Conclusion: 

> Latency significantly impacts the performance of elastic stream processing applications, particularly those involving stateful operators. The arrival of records is delayed due to latency, which in turn affects the filling of network buffers and the busyness of the operators. This chain reaction ultimately impacts the throughput of the system. 
> 
> Furthermore, the distribution of replicas on nodes also plays a crucial role in the performance of the system. Depending on the distribution, network buffers, operator busyness, and backpressure are affected differently. This is particularly noticeable in stateful use cases where the throughput is influenced by the majority of a certain type of nodes (latency or no latency). 
> 
> Adding replicas of stateful operators on nodes with latency has been observed to decrease the overall throughput. This is a significant finding as many scaling techniques rely on backpressure to trigger scaling. However, with the presence of latency, backpressure is not a reliable signal. Latency causes a drop in throughput, backpressure, and operator busyness, which results in the system not scaling as expected.
> 
> In conclusion, the performance of elastic stream processing applications, especially those involving stateful operators, is significantly affected by factors such as latency and the distribution of replicas on nodes. These factors make it challenging to accurately model and predict the performance of such systems.

Notes for abstract:
% In this work we see 
% large scale experiments
% for stateful operations
% with latency and variable latency 
% on elasticity and performance predictability 
% highlight the limitations and opportunities for effective elastic scaling in geo-distributed environments 

Abstract:
In this work, we investigate the impact of variable latency on the performance predictability and scalability of stateful operators in stream processing applications. 
Our findings highlight the significant influence of these factors in the system's throughput. Furthermore, we observe that many scaling techniques, which rely on backpressure to trigger scaling, are not reliable in the presence of latency. 
These results provide insights into the limitations and opportunities for effective elastic scaling in geo-distributed environments.
