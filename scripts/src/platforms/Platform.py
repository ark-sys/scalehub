class Platform:
    def setup(self):
        raise NotImplementedError()

    def destroy(self):
        raise NotImplementedError()
