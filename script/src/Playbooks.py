import enoslib as en
from .utils.Defaults import DefaultValues as Value

class Playbooks:
    def __init__(self):
        self.playbook_basepath=Value.System.PLAYBOOKS_PATH
    def run(self, playbook):
        playbook_filename = f"{Value.System.PLAYBOOKS_PATH}/{playbook}.yaml"
        en.run_ansible(playbooks=[playbook_filename],inventory_path=Value.System.INVENTORY_PATH, tags=None, basedir=Value.System.PLAYBOOKS_PATH)
