import os

import enoslib as en

from .utils.Defaults import DefaultValues as Value


class Playbooks:
    def run_playbook(self, playbook, **kwargs):
        playbook_filename = f"{Value.System.PLAYBOOKS_PATH}/{playbook}.yaml"
        if not os.path.exists(playbook_filename):
            # Raise an error with the file path
            raise FileNotFoundError(f"The file doesn't exist: {playbook_filename}")
        arg_tags = kwargs.get("tags", None)
        extra_vars = kwargs.get("extra_vars", None)
        if not extra_vars:
            extra_vars = {"kubeconfig_path": os.environ["KUBECONFIG"]}
        else:
            extra_vars.update({"kubeconfig_path": os.environ["KUBECONFIG"]})
        # Run the playbook with additional tags and extra vars
        en.run_ansible(
            playbooks=[playbook_filename],
            tags=arg_tags,
            extra_vars=extra_vars,
            inventory_path=Value.System.INVENTORY_PATH
        )

    def deploy(self, playbook, **kwargs):

        # Extract the 'additional_tag' from kwargs (if it exists)
        additional_tags = kwargs.get("tags", None)

        # Create a list of tags, including the 'create' tag and the 'additional_tag' (if provided)
        arg_tags = ["create"]
        if additional_tags:
            arg_tags.append(additional_tags)

        self.run_playbook(playbook, tags=arg_tags, **kwargs)

    def delete(self, playbook, **kwargs):
        # Extract the 'additional_tag' from kwargs (if it exists)
        additional_tags = kwargs.get("tags", None)

        # Create a list of tags, including the 'create' tag and the 'additional_tag' (if provided)
        arg_tags = ["delete"]
        if additional_tags:
            arg_tags.append(additional_tags)

        self.run_playbook(playbook, tags=arg_tags, **kwargs)
