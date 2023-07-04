class Platform:
    def pre_setup(self):
        raise NotImplementedError()

    def post_setup(self):
        raise NotImplementedError()

    def pre_teardown(self):
        raise NotImplementedError()

    def post_teardown(self):
        raise NotImplementedError()

    def get_platform_metadata(self) -> dict:
        raise NotImplementedError()

    def setup(self) -> dict:
        raise NotImplementedError()


