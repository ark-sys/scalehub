# Configuration

ScaleHub reads INI files. `conf/defaults.ini` holds every default value, and the files under `conf/provision/` and
`conf/experiment/` override only what they need. Scaling strategies are YAML files under `conf/experiment/strategy/`.

```
conf/
├── defaults.ini                 # all keys with their default values
├── ansible.cfg
├── provision/                   # which nodes to reserve, and where
│   ├── rennes_small.ini
│   ├── rennes_and_pico.ini
│   └── ...
└── experiment/                  # what to run on them
    ├── single_node/  multi_node/  mix_node/  resource_exp/
    └── strategy/                # scaling steps referenced by the experiment files
```

## Provisioning

A provisioning file lists platforms. Each platform has a `type`, plus a count of nodes per role:

- `control`: runs the K3s server and the shared services (ingress, Prometheus, experiment monitor, Flink JobManager).
- `producers`: run Kafka and the load generators.
- `consumers`: run the Flink TaskManagers, i.e. the system under test.

```ini
[platforms]
platforms = g5k_rennes g5k_rennes_vm_small pico

[platforms.g5k_rennes]
type = Grid5000
reservation_name = scalehub_rennes_baremetal
site = rennes
cluster = paravance
control = 1
producers = 3
consumers = 4
queue = default
walltime = 2:00:00
kubernetes_type = k3s

[platforms.g5k_rennes_vm_small]
type = VMonG5k
reservation_name = scalehub_rennes_vm_small
site = rennes
cluster = parasilo
control = 0
producers = 0
consumers = 7
core_per_vm = 2
memory_per_vm = 4096
disk_per_vm = 10
walltime = 2:00:00

[platforms.pico]
type = RaspberryPi
inventory = /app/conf/pico_hosts.yaml
cluster = pico2
control = 0
producers = 0
consumers = 2
kubernetes_type = k3s
```

| `type`        | Backend                                          | Notes                                                  |
|---------------|--------------------------------------------------|--------------------------------------------------------|
| `Grid5000`    | EnOSlib `G5k`, bare-metal nodes                  | `site`, `cluster`, `queue`, `walltime`, `start_time`   |
| `VMonG5k`     | EnOSlib `VMonG5k`, VMs on Grid'5000 hosts        | adds `core_per_vm`, `memory_per_vm` (MiB), `disk_per_vm` (GiB) |
| `VagrantG5k`  | Vagrant VMs on Grid'5000 nodes                   |                                                        |
| `FIT`         | EnOSlib `Iotlab`                                 | registered, not used in the published experiments      |
| `RaspberryPi` | static Ansible inventory, checked over SSH       | see below                                              |

Global options (in `defaults.ini`):

```ini
[scalehub]
lazy_setup = false               ; also run orchestration/setup and application/setup after provisioning
k3s_version = v1.32.2+k3s1

[platforms]
tailscale_backend = true         ; join every node to a Tailscale mesh so testbeds can reach each other
tailscale_auth_key =             ; Tailscale auth key, required when tailscale_backend is on (keep it out of git)
enable_ipv6 = false
```

### Raspberry Pi cluster

The `RaspberryPi` platform reads hosts from the `pico` group of a YAML inventory. It keeps the hosts that answer over
SSH and fails if fewer than `producers + consumers` respond.

```yaml
pico:
  hosts:
    pi1: { ansible_ssh_host: 10.0.0.11 }
    pi2: { ansible_ssh_host: 10.0.0.12 }
```

## Experiments

An experiment file overrides the `[experiment]` section of the defaults:

```ini
[experiment]
name = Join
type = simple                    ; simple | transscale | resource | test
job_file = myjoin-transscale-all.jar
task_name = TumblingEventTimeWindows
runs = 5
comment = "52 taskmanagers on 1 node"

[experiment.scaling]
strategy_path = /app/conf/experiment/strategy/single_node_bm.yaml
max_parallelism = 52
interval_scaling_s = 240         ; time spent at each parallelism level
```

Other sections you can override:

| Section                       | Keys                                                                              |
|-------------------------------|-----------------------------------------------------------------------------------|
| `[experiment]`                | `output_skip_s` (warm-up cut from results), `output_stats`, `output_plot`, `kafka_partitions`, `unchained_tasks` |
| `[experiment.generators.*]`   | `type` (`theodolite-lg`), `topic`, `num_sensors`, `interval_ms`, `replicas`, `value` |
| `[experiment.flink]`          | `checkpoint_interval_ms`, `window_size_ms`, `fibonacci_value`                     |
| `[experiment.transscale]`     | `max_parallelism`, `monitoring_warmup_s`, `monitoring_interval_s`                 |
| `[experiment.chaos]`          | `enable`, `affected_nodes_percentage`, `delay_latency_ms`, `delay_jitter_ms`, `delay_correlation`, `bandwidth_rate_mbps`, `bandwidth_limit`, `bandwidth_buffer` |

Experiment types:

- `simple`: follow a scaling strategy with fixed time steps.
- `transscale`: hand scaling decisions to the TransScale autoscaler.
- `resource`: run the strategy once for every combination of TaskManager CPU and memory limits listed in
  `cpu_values` and `memory_values`.
- `test`: dry run of the state machine.

## Scaling strategies

A strategy is a list of steps. Each step picks the next free consumer node of a given kind and adds TaskManagers to
it:

```yaml
- node: grid5000          # grid5000 | vm_grid5000 | pico
  taskmanager:
    - number: 52          # TaskManagers to add on this node
      type: l             # TaskManager template: s | m | l | xl | xxl | pico ...
      method: block       # linear | exponential | block
- node: vm_grid5000
  type: small             # VM size class: small | medium | large | x-large
  taskmanager:
    - number: 2
      type: s
      method: linear
```

- `linear` adds one TaskManager at a time, `exponential` adds 1, 2, 4, 8…, and `block` adds `number` at once.
- `type` selects one of the TaskManager StatefulSet templates in
  `playbooks/application/roles/flink/templates/taskmanagers/`. Each template sets its own CPU, memory and slot count.
- Nodes are picked through labels set by the orchestration playbook: `node-role.kubernetes.io/tnode`
  (`grid5000`, `vm_grid5000`, `pico`), and for VMs, `node-role.kubernetes.io/vm_grid5000` (a size class computed from
  allocatable CPU × memory).

The steps run in order. Between two scaling actions the monitor waits `interval_scaling_s` seconds, and a node is
marked `FULL` once its TaskManagers are placed.
