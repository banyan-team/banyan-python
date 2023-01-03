import os

import banyan as bn

user_id = "user1"
api_key = "12345"


def test_args():
    banyanconfig_path = "tempfile_args.toml"
    config = bn.configure(
        user_id=user_id, api_key=api_key, banyanconfig_path=banyanconfig_path
    )
    print(config)
    assert (
        config["banyan"]["user_id"] == user_id
        and config["banyan"]["api_key"] == api_key
    )

    config = bn.config.load_config(banyanconfig_path)
    assert (
        config["banyan"]["user_id"] == user_id
        and config["banyan"]["api_key"] == api_key
    )

    try:
        os.remove(banyanconfig_path)
    except FileNotFoundError:
        pass


def test_environ():
    banyanconfig_path = "tempfile_environ.toml"
    os.environ["BANYAN_USER_ID"] = user_id
    os.environ["BANYAN_API_KEY"] = api_key

    config = bn.configure(banyanconfig_path=banyanconfig_path)
    assert (
        config["banyan"]["user_id"] == user_id
        and config["banyan"]["api_key"] == api_key
    )

    config = bn.config.load_config(banyanconfig_path)
    assert (
        config["banyan"]["user_id"] == user_id
        and config["banyan"]["api_key"] == api_key
    )

    try:
        os.remove(banyanconfig_path)
    except FileNotFoundError:
        pass


def test_toml():
    banyanconfig_path = "tempfile_toml.toml"
    bn.configure(
        user_id=user_id, api_key=api_key, banyanconfig_path=banyanconfig_path
    )

    del os.environ["BANYAN_USER_ID"]
    del os.environ["BANYAN_API_KEY"]
    config = bn.configure(banyanconfig_path=banyanconfig_path)
    assert (
        config["banyan"]["user_id"] == user_id
        and config["banyan"]["api_key"] == api_key
    )

    config = bn.config.load_config(banyanconfig_path)
    assert (
        config["banyan"]["user_id"] == user_id
        and config["banyan"]["api_key"] == api_key
    )

    try:
        os.remove(banyanconfig_path)
    except FileNotFoundError:
        pass
