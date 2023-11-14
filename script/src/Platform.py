class Platform:
    def pre_setup(self):
        raise NotImplementedError()

    def post_setup(self):
        raise NotImplementedError()

    def pre_teardown(self):
        raise NotImplementedError()

    def post_teardown(self):
        raise NotImplementedError()

    def setup(self) -> str:
        raise NotImplementedError()

    def deploy_playbook(self):
        raise NotImplementedError()

    def delete_playbook(self):
        raise NotImplementedError()
