"""
This file contains utils files related to setting and
loading configurations.
"""

from src.imports import *

banyanconfig = dict() # Global variable representing configuration

def load_config(banyanconfig_path:Optional[str] = None):
    """ Loads configuration from given file

    Arguments:
    - banyanconfig_path:Optional[str], defaults to None
        filepath to Banyan configuration file
        if None (recommended), will use "$HOME/.banyan/banyanconfig.toml"
    """
    global banyanconfig 
    if banyanconfig_path is None:
        banyanconfig_path = os.path.join(os.path.expanduser('~'), ".banyan/banyanconfig.toml")
    if not os.path.exists(banyanconfig_path):
        return dict()
    banyanconfig = toml.load(banyanconfig_path)
    return banyanconfig

def write_config(banyanconfig_path:Optional[str] = None):
    """ Writes configuration to given file

    Arguments:
    - banyanconfig_path:Optional[str], defaults to None
        filepath to Banyan configuration file
        if None (recommended), will use "$HOME/.banyan/banyanconfig.toml"
    """
    global banyanconfig 
    if banyanconfig_path is None:
        banyanconfig_path = os.path.join(os.path.expanduser('~'), ".banyan/banyanconfig.toml")
    
    os.makedirs(os.path.join(os.path.expanduser('~'), ".banyan/"), exist_ok=True)
    
    with open(banyanconfig_path, "w") as f:
        toml.dump(banyanconfig, f)
    return banyanconfig

def configure(
        user_id:Optional[str]=None, 
        api_key:Optional[str]=None, 
        ec2_key_pair_name:Optional[str]=None, 
        banyanconfig_path:Optional[str]=None
    ):
    """ Sets configuration.
    If any provided configuration parameter is None, 
    will check environment variable, then last used/loaded configuration

    Arguments:
    - user_id:Optional[str], defaults to None
    - api_key:Optional[str], defaults to None
    - ec2_key_pair_name:Optional[str], defaults to None
    - banyanconfig_path:Optional[str], defaults to None
        file to save configurations to
        if None (recommended), will use "$HOME/.banyan/banyanconfig.toml"
    """
    global banyanconfig
    loaded_config = load_config(banyanconfig_path)
    config_change = False

    # Find user_id
    if user_id is None:
        user_id_env = os.getenv("BANYAN_USER_ID", default = None)
        if user_id_env is None and "user_id" in loaded_config:
            user_id = loaded_config["user_id"]
        else:
            user_id = user_id_env
    if "user_id" not in loaded_config or user_id != loaded_config["user_id"]:
        config_change = True
    banyanconfig["user_id"] = user_id

    # Find api_key
    if api_key is None:
        api_key_env = os.getenv("BANYAN_API_KEY", default = None)
        if api_key_env is None and "api_key" in loaded_config:
            api_key = loaded_config["api_key"]
        else:
            api_key = api_key_env
    if "api_key" not in loaded_config or api_key != loaded_config["api_key"]:
        config_change = True
    banyanconfig["api_key"] = api_key

    # Find ec2_key_pair_name
    if ec2_key_pair_name is None:
        ec2_key_pair_name_env = os.getenv("EC2_KEY_PAIR_NAME", default = None)
        if ec2_key_pair_name_env is None and "ec2_key_pair_name" in loaded_config:
            ec2_key_pair_name = loaded_config["ec2_key_pair_name"]
        else:
            ec2_key_pair_name = ec2_key_pair_name_env
    if "ec2_key_pair_name" not in loaded_config or ec2_key_pair_name != loaded_config["ec2_key_pair_name"]:
        config_change = True
    banyanconfig["ec2_key_pair_name"] = ec2_key_pair_name

    # Update toml file, if needed
    if config_change:
        write_config(banyanconfig_path)
    return banyanconfig
    