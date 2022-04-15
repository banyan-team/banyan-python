"""
This file contains utils files related to setting and
loading configurations.
"""

from banyan.imports import *
from copy import deepcopy

banyan_config = dict()  # Global variable representing configuration


def load_config(banyanconfig_path: Optional[str] = None):
    """Loads configuration from given file

    Arguments:
        banyanconfig_path:Optional[str], defaults to None
            filepath to Banyan configuration file
            if None (recommended), will use "$HOME/.banyan/banyanconfig.toml"
    """
    global banyan_config

    if banyan_config is None:
        if banyanconfig_path is None:
            banyanconfig_path = os.path.join(
                os.path.expanduser("~"), ".banyan/banyanconfig.toml"
            )
        if os.path.exists(banyanconfig_path):
            banyan_config = toml.load(banyanconfig_path)
    return banyan_config


def write_config(banyanconfig_path: Optional[str] = None):
    """Writes configuration to given file

    Arguments:
        banyanconfig_path:Optional[str], defaults to None
            filepath to Banyan configuration file
            if None (recommended), will use "$HOME/.banyan/banyanconfig.toml"
    """
    global banyan_config
    if banyanconfig_path is None:
        banyanconfig_path = os.path.join(
            os.path.expanduser("~"), ".banyan/banyanconfig.toml"
        )

    os.makedirs(os.path.join(os.path.expanduser("~"), ".banyan/"), exist_ok=True)

    with open(banyanconfig_path, "w") as f:
        toml.dump(banyan_config, f)
    return banyan_config


def configure(
    user_id: Optional[str] = None,
    api_key: Optional[str] = None,
    ec2_key_pair_name: Optional[str] = None,
    banyanconfig_path: Optional[str] = None,
):
    """Sets configuration.
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

    c = load_config(banyanconfig_path)
    global banyan_config
    banyan_config = c

    # Check environment variables
    user_id_env = os.getenv("BANYAN_USER_ID", default=None)
    if user_id is None and not user_id_env is None:
        user_id = user_id_env

    api_key_env = os.getenv("BANYAN_API_KEY", default=None)
    if api_key is None and not api_key_env is None:
        api_key = api_key_env

    ec2_env = os.getenv("BANYAN_EC2_KEY_PAIR_NAME")
    if ec2_key_pair_name is None and not ec2_env == None:
        ec2_key_pair_name = ec2_env

    # Check banyan_config file
    banyan_config_has_info = not (banyan_config is None or banyan_config == {})
    if (
        user_id is None
        and banyan_config_has_info
        and "banyan" in banyan_config
        and "user_id" in banyan_config["banyan"]
    ):
        user_id = banyan_config["banyan"]["user_id"]
    if (
        api_key is None
        and banyan_config_has_info
        and "banyan" in banyan_config
        and "api_key" in banyan_config["banyan"]
    ):
        api_key = banyan_config["banyan"]["api_key"]
    if (
        ec2_key_pair_name is None
        and banyan_config_has_info
        and "aws" in banyan_config
        and "ec2_key_pair_name" in banyan_config["aws"]
    ):
        ec2_key_pair_name = banyan_config["aws"]["ec2_key_pair_name"]

    # Ensure a configuration has been created or can be created. Otherwise,
    # return nothing
    existing_banyan_config = deepcopy(banyan_config)
    if banyan_config is None or banyan_config == {}:
        if not user_id is None and not api_key is None:
            banyan_config = {
                "banyan": {"user_id": user_id, "api_key": api_key},
                "aws": {},
            }
            is_modified = True
        else:
            raise Exception(
                "Your user ID and API key must be specified using either keyword arguments, environment variables, or banyanconfig.toml"
            )
    if not existing_banyan_config == banyan_config:
        write_config(banyanconfig_path)

    return banyan_config
