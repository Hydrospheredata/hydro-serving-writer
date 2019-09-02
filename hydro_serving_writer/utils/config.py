import os

__all__ = ["get_config"]


def get_config(config_map_mount_path="/etc/config", **kwargs):
    """ 
    Get configuration for current deployment.
    Prioritize ConfigMap over Environment variables. 

    Parameters
    ----------
    config_map_mount_path: str
        Path, where ConfigMap was mounted. 
    
    Returns
    -------
    dict
        A dictionary with a key corresponding to a filename and a value 
        corresponding to the file contents. All values are strings. 
    """

    # [(NAME, TYPE)]
    variables = [
        ("DEBUG",                     int),
        ("SERVER_PORT",               int),
        ("SERVER_MAX_WORKERS",        int),
        ("STORAGE_TYPE",              str),              
        ("STORAGE_BUCKET",            str),
        ("STORAGE_ACCESS_KEY",        str),
        ("STORAGE_SECRET_ACCESS_KEY", str),
        ("BUFFER_SIZE",               int),
    ]

    config = dict()
    for variable, type in variables:
        mounted_path = os.path.join(config_map_mount_path, variable)
        if os.path.exists(mounted_path):
            with open(mounted_path, "r") as content:
                config[variable] = type(content)
        else:
            env_variable = os.environ.get(variable)
            if not env_variable:
                raise ValueError("{} cannot be found neither as mounted " \
                    "ConfigMap nor as EnvVar".format(variable))
            config[variable] = type(env_variable)
    return config