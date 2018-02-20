import os

class EnvUtils:

    @classmethod
    def get(self, var_name, default=None):
        value = os.environ.get(var_name)
        if value is None:
            return default
        return value

