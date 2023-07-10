import os.path
import enoslib as en
from .utils.Defaults import DefaultValues as Value

class Playbooks:

    def deploy(self, playbook):
        playbook_filename = f"{Value.System.PLAYBOOKS_PATH}/{playbook}.yaml"
        if not os.path.exists(playbook_filename):
            # Raise an error with the file path
            raise FileNotFoundError(f"The file doesn't exist: {playbook_filename}")

        en.run_ansible(playbooks=[playbook_filename], inventory_path=Value.System.INVENTORY_PATH,tags=["create"],
                       extra_vars={
                           "kubeconfig_path": "/tmp/kubeconfig"})


    def delete(self, playbook):
        playbook_filename = f"{Value.System.PLAYBOOKS_PATH}/{playbook}.yaml"
        if not os.path.exists(playbook_filename):
            # Raise an error with the file path
            raise FileNotFoundError(f"The file doesn't exist: {playbook_filename}")
        en.run_ansible(playbooks=[playbook_filename], inventory_path=Value.System.INVENTORY_PATH, tags=["delete"],
                       extra_vars={
                           "kubeconfig_path": "/tmp/kubeconfig"})