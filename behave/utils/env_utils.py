import os

class EnvUtils:

    @classmethod
    def get_env_variable(self, var_name, default='UNSET'):
        value = os.environ.get(var_name)
        if value is None:
            return default
        return value

