# Extending ScaleHub

There are three extension points: platform providers (where nodes come from), Ansible roles (what runs on the
cluster), and experiment types (how an experiment drives the cluster).

## Platform providers

Platforms live in `src/scalehub/platforms/`. `PlatformFactory` maps the `type` field of a provisioning entry to a
class:

```python
_PLATFORM_TYPES = {
    "Grid5000": EnosPlatform,
    "VMonG5k": EnosPlatform,
    "FIT": EnosPlatform,
    "VagrantG5k": EnosPlatform,
    "RaspberryPi": RaspberryPiPlatform,
}
```

### Another EnOSlib testbed

EnOSlib also supports Chameleon and other testbeds. To add one, extend the provider map in
`EnosPlatform.get_provider()` along with the matching configuration builder, then register the type in
`PlatformFactory`:

```python
provider_map = {
    # ...
    "Chameleon": lambda: en.Chameleon(en.ChameleonConf.from_dictionary(conf_dict).finalize()),
}
```

### A provider outside EnOSlib

Subclass `Platform` and implement three methods:

```python
from typing import Any, Dict

from src.scalehub.platforms.Platform import Platform


class MyCloudPlatform(Platform):
    def _validate_config(self) -> None:
        for field in ("api_endpoint", "region"):
            if field not in self._platform_config:
                raise ValueError(f"Missing required field: {field}")

    def setup(self, verbose: bool = False) -> Dict[str, Any]:
        # create the machines, then return them as an Ansible inventory
        return {
            "control": {"hosts": {"control-1": {...}}},
            "agents": {"hosts": {"consumer-1": {...}, "producer-1": {...}}},
            "mycloud": {"hosts": {...}},  # all hosts of this platform
        }

    def destroy(self) -> None:
        # release the machines
        ...
```

Every host entry needs at least:

```python
{
    "ansible_host": "203.0.113.10",
    "ansible_user": "ubuntu",
    "cluster_role": "control",        # control | producer | consumer
    "platform_name": self.platform_name,
}
```

Register the class before provisioning:

```python
PlatformFactory.register_platform("MyCloud", MyCloudPlatform)
```

and reference it from a provisioning file:

```ini
[platforms]
platforms = mycloud

[platforms.mycloud]
type = MyCloud
api_endpoint = https://api.example.com
region = eu-west-1
control = 1
producers = 2
consumers = 3
```

`src/scalehub/platforms/examples/CustomCloudPlatform.py` is a complete example with simulated instances.

The orchestration playbook sets the hardware-type label (`node-role.kubernetes.io/tnode`) from the inventory groups
`G5k`, `VMonG5k` and `pico`. To use a new platform's nodes in a scaling strategy, add its group to the labeling tasks
in `playbooks/orchestration/roles/k3s_*/tasks/install.yaml` and `post_install/tasks/labeling.yaml`.

## Playbooks

1. Create a role in `playbooks/application/roles/<name>/`. Tag tasks `create` for deployment and `delete` for
   removal. Read settings from the `shub_config` variable when you need values from the INI files.
2. Add `playbooks/application/<name>.yaml`:

   ```yaml
   ---
   - name: Deploy <name>
     hosts: localhost
     gather_facts: false
     roles:
       - <name>
   ```

3. Run `shub deploy application/<name>`, `shub delete application/<name>` or `shub reload application/<name>`.
4. To include it in the full stack, add the role to `playbooks/application/setup.yaml`.

Use node selectors on `node-role.kubernetes.io/worker` (`producer` or `consumer`) so the new workload lands on the
right side of the cluster.

## Experiment types

Experiment types live in `src/monitor/experiments/exp_types/` and subclass `Experiment`. The base class already
handles the multi-run loop, cleanup between runs and result export. Most types only implement `exp()`:

```python
from src.monitor.experiments.Experiment import Experiment


class MyExperiment(Experiment):
    exp_type = "mine"

    def exp(self):
        # set up whatever the experiment needs, then reuse the default run loop
        self.do_multi_run()
```

Add the new type to `ExperimentFSM.__create_experiment_instance()`, rebuild the monitor image, and set
`type = mine` in the experiment file. `ResourceExperiment` and `TransscaleExperiment` are examples that override
more of the lifecycle (`finishing`, `cleaning`).
