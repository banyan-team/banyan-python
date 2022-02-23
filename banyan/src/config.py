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

banyan_config = {}

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
    
    load_config(banyanconfig_path)
    global banyan_config

    # Check environment variables
    user_id_env = os.getenv("BANYAN_USER_ID", default = None)
    if user_id == None and not user_id_env == None:
        user_id = user_id_env

    api_key_env = os.getenv("BANYAN_API_KEY", default = None)
    if api_key == None and not api_key_env == None:
        api_key = api_key_env

    ec2_env = os.getenv("BANYAN_EC2_KEY_PAIR_NAME")
    if ec2_key_pair_name == None and not ec2_env == None:
        ec2_key_pair_name = ec2_env  

   # Check banyanconfig file
    if user_id == None and 'banyan' in banyan_config and "user_id" in banyan_config["banyan"]:
        user_id = banyan_config["banyan"]["user_id"]
    if api_key == None and "banyan" in banyan_config and "api_key" in banyan_config["banyan"]:
        api_key = banyan_config["banyan"]["api_key"]


    # Initialize
    is_modified = False
    is_valid = True

    # Ensure a configuration has been created or can be created. Otherwise,
    # return nothing
    if banyan_config == None or banyan_config == {}:
        if not user_id == None and not api_key == None:
            banyan_config = {
                "banyan":
                    {"user_id" : user_id, "api_key" : api_key},
                "aws" : {}
            }
            is_modified = True
        else:
            raise("Your user ID and API key must be specified using either keyword arguments, environment variables, or banyanconfig.toml")
        
    # Check for changes in required
    if not user_id == None and not user_id == banyan_config["banyan"]["user_id"]:
        banyan_config["banyan"]["user_id"] = user_id
        is_modified = True
    if not api_key == None and not api_key == banyan_config["banyan"]["api_key"]:
        banyan_config["banyan"]["api_key"] = api_key
        is_modified = True
    

    # Check for changes in other args

    # aws.ec2_key_pair_name
    if ec2_key_pair_name == None:
        del banyan_config["aws"]["ec2_key_pair_name"]
    elif (ec2_key_pair_name != 0) and (not "ec2_key_pair_name" in banyan_config["aws"] or not ec2_key_pair_name == banyan_config["aws"]["ec2_key_pair_name"]):
        banyan_config["aws"]["ec2_key_pair_name"] = ec2_key_pair_name
        is_modified = True

    # Update config file if it was modified
    if is_modified:
        write_config(banyanconfig_path)

    return banyan_config