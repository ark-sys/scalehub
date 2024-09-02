![1 - Experimental Environment](../info/images/cluster_stack_paper_slim.png)

Here are defined the playbooks that setup the Experimental environment as shown by Figure 1.

## Playbooks

### 1. Infrastructure

- This playbook installs base dependencies on all nodes.
- Then if necessary, for each platform it will perform additional setup tasks.

### 2. Orchestration
- This playbook installs kubernetes on control and worker nodes.
- Afterward, the nodes are labeled according to their roles.

### 3. Application
- This playbook installs all the microservices that will be used for the experiments.
- The applications are:
    - Network access with Ingress Controller
    - Storage service with MinIO and NFS
    - Monitoring with Prometheus and Grafana
    - Chaos injection with Chaos Mesh
    - Load injection with Theodolite load generators
    - Stream processing with Kafka and Flink
    - Auto-scaling with Transscale 2.0
