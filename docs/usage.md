# Usage

All commands run inside the scalehub container (`./deploy.sh shell`).

## The `shub` CLI

```
shub [-c CONF[,CONF...]] <command> [args]

  provision                 Reserve the platforms listed in the config and run the infrastructure playbook
  destroy                   Release the reservations
  deploy <playbook>         Run the `create` tasks of a playbook, e.g. `application/flink`
  delete <playbook>         Run the `delete` tasks of a playbook
  reload <playbook>         delete, then deploy
  experiment <action>       start | stop | clean | check (sends commands to the in-cluster experiment monitor)
  tokens                    Print the Kubernetes dashboard token
  sync                      Copy experiment data from the cluster to the local experiments folder
  export <path>             Process an experiment folder into CSV files and plots
```

`-c` selects the configuration file. When it is omitted, `shub` uses the runtime configuration saved by the last
`provision` (`/app/runtime/runtime.json`), or falls back to `conf/defaults.ini`. For `experiment`, you can pass several
files separated by commas. They are queued and run one after another.

## Typical session

```shell
# 1. Reserve nodes and prepare them (base packages, Tailscale)
shub -c conf/provision/rennes_small.ini provision

# 2. Install K3s on control and worker nodes, label nodes by role and hardware type
shub deploy orchestration/setup

# 3. Deploy the application stack (ingress, storage, monitoring, Kafka, Flink)
shub deploy application/setup

#    or deploy pieces one at a time
shub deploy application/chaos
shub reload application/flink

# 4. Run an experiment (see configuration.md for the file format)
shub -c conf/experiment/single_node/single_node_bm.ini experiment start
shub experiment check            # current state of the experiment monitor
shub experiment stop             # interrupt; runs that already completed are still processed

# 5. Retrieve and process results
shub sync
shub export 2025-03-14/1         # one experiment
shub export 2025-03-14           # every experiment of that day

# 6. Release the nodes
shub destroy
```

If `lazy_setup = true` is set in the provisioning file, `provision` runs steps 2 and 3 on its own.

## What happens during `experiment start`

1. `shub` serialises the experiment configuration(s) and publishes a `START` message on the MQTT broker running in the
   cluster.
2. The experiment monitor checks the request, deploys the load generators and submits the Flink job.
3. It follows the scaling strategy step by step: take a savepoint to MinIO, stop the job, change the TaskManager
   replicas, restart from the savepoint, wait for `interval_scaling_s` seconds.
4. At the end of each run it resets the TaskManagers, node labels, load generators and Kafka, then starts the next run.
5. When all runs are done (or on `stop`), it pulls the metrics from VictoriaMetrics for each run's time window,
   writes CSV files and plots, and goes back to idle.

Since the monitor runs in the cluster, you can close your laptop while a long campaign runs. See
[Architecture](architecture.md#experiment-monitor) for the state machine.

## Experiment output

Results are stored under `experiments-data/<YYYY-MM-DD>/`:

```
experiments-data/2025-03-14/
├── 1/                      # single-run experiment
└── multi_run_1/            # experiment with runs > 1
    ├── monitor_logs.txt
    ├── 1/  2/  3/          # one folder per run: config + time window, exported metrics, plots
    └── ...                 # aggregated CSV and plots across runs
```

`shub export` can be run again on any of these folders, for example after changing a plot. The processing and
plotting code is described in the [data module README](../src/scalehub/data/README.md).

## Dashboards

With the local DNS set up (see [Getting started](getting-started.md#local-dns-and-dashboards)):

| URL                             | Service                                   |
|---------------------------------|-------------------------------------------|
| http://scalehub.dev             | landing page                              |
| http://grafana.scalehub.dev     | Grafana dashboards                        |
| http://vm.scalehub.dev          | VictoriaMetrics                           |
| http://prometheus.scalehub.dev  | Prometheus                                |
| http://flink.scalehub.dev       | Flink web UI                              |
| http://minio.scalehub.dev       | MinIO console (savepoints, checkpoints)   |
| http://chaos.scalehub.dev       | Chaos Mesh dashboard                      |
| http://kube.scalehub.dev        | Kubernetes dashboard (`shub tokens`)      |
