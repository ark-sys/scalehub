# Playbooks

Playbooks are grouped by layer and run in that order. Each task carries a `create` or `delete` tag:
`shub deploy <playbook>` runs the `create` tasks, `shub delete` the `delete` tasks, and `shub reload` both. Most
tasks are idempotent, so running `deploy` twice is safe.

```
playbooks/
├── infrastructure/setup.yaml     run by `shub provision`, on every node
├── orchestration/setup.yaml      K3s and node labels
└── application/
    ├── setup.yaml                full stack, run against the cluster from the scalehub container
    └── <component>.yaml          one playbook per component
```

The configuration loaded by `shub` is passed to Ansible as the `shub_config` extra variable, so playbooks read their
settings from the same INI files.

## 1. Infrastructure

`shub provision` runs this playbook automatically on all reserved nodes.

| Role          | Does                                                                                   |
|---------------|----------------------------------------------------------------------------------------|
| `common`      | creates data folders, installs the Tailscale client                                    |
| `grid5000`    | Grid'5000 bare-metal tweaks (IPv6)                                                     |
| `vmong5k`     | IPv6 addresses and forwarding for VMs and their hosts, opens the Grid'5000 firewall     |
| `vagrantg5k`  | same for Vagrant-based VMs                                                             |
| `picocluster` | removes K3s left over from a previous run on the Raspberry Pi nodes, then reboots them |

## 2. Orchestration

```shell
shub deploy orchestration/setup
```

| Role           | Does                                                                                  |
|----------------|---------------------------------------------------------------------------------------|
| `k3s_control`  | installs the K3s server, optionally over Tailscale, and fetches the kubeconfig        |
| `k3s_worker`   | joins agents to the server with the node token                                        |
| `post_install` | creates namespaces and sets role, hardware-type and size labels (see [Architecture](architecture.md#orchestration-layer)) |

## 3. Application

Application playbooks run on `localhost`, inside the scalehub container. They reach the cluster with the kubeconfig
fetched in step 2, mostly through Helm charts and manifests applied with `kubernetes.core`.

```shell
shub deploy application/setup        # network, storage, monitoring, kafka, flink
shub deploy application/chaos        # optional components are deployed separately
```

| Playbook                | Roles                         | Deploys                                                        |
|-------------------------|-------------------------------|----------------------------------------------------------------|
| `network`               | `network`                     | ingress-nginx, cert-manager                                    |
| `storage`               | `storage`                     | MinIO tenant, NFS storage class, PVCs                          |
| `monitoring`            | `monitoring`                  | Prometheus + Grafana, VictoriaMetrics, Consul, Kubernetes dashboard, MQTT broker and experiment monitor |
| `kafka`                 | `kafka`                       | Zookeeper, Kafka, Schema Registry, Redis                       |
| `flink`                 | `flink`                       | JobManager and the TaskManager StatefulSet templates           |
| `load_generators`       | `load_generators`             | Theodolite (or YSB) load generators; deployed by the experiment monitor at each run |
| `chaos`                 | `chaos`                       | Chaos Mesh and the network delay or bandwidth experiments      |
| `transscale`            | `transscale`                  | TransScale autoscaler job                                      |
| `base`                  | `storage`, `network`          | shortcut for the base services                                 |
| `data-stream-apps`      | `kafka`, `flink`              | shortcut for the streaming stack                               |
| `goldpinger`            | (inline)                      | Goldpinger, to check node-to-node connectivity                 |

Image names and versions for each component are in `roles/<role>/vars/main.yaml`.

The TransScale and load generator images used for the published experiments were hosted on the lab's private
registry and are not publicly available. The `load_generators` and `transscale` roles point to placeholders and
the old registry, so set them to your own builds before using those roles.

## Adding a playbook

Create `playbooks/application/<name>.yaml` that applies a role on `localhost`, and give the role's tasks the `create`
and `delete` tags. `shub deploy application/<name>` will then pick it up. See [Extending](extending.md#playbooks).
