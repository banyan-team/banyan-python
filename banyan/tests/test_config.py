from banyan.config import *

user_id = "user1"
api_key = "12345"
banyanconfig_path = "tempfile.toml"


def test_args():
    config = configure(
        user_id=user_id, api_key=api_key, banyanconfig_path=banyanconfig_path
    )
    assert config["user_id"] == user_id and config["api_key"] == api_key

    config = load_config(banyanconfig_path)
    assert config["user_id"] == user_id and config["api_key"] == api_key

    os.remove(banyanconfig_path)


def test_environ():
    os.environ["BANYAN_USER_ID"] = user_id
    os.environ["BANYAN_API_KEY"] = api_key

    config = configure(banyanconfig_path=banyanconfig_path)
    assert config["user_id"] == user_id and config["api_key"] == api_key

    config = load_config(banyanconfig_path)
    assert config["user_id"] == user_id and config["api_key"] == api_key

    os.remove(banyanconfig_path)


def test_toml():
    configure(user_id=user_id, api_key=api_key, banyanconfig_path=banyanconfig_path)

    del os.environ["BANYAN_USER_ID"]
    del os.environ["BANYAN_API_KEY"]
    config = configure(banyanconfig_path=banyanconfig_path)
    assert config["user_id"] == user_id and config["api_key"] == api_key

    config = load_config(banyanconfig_path)
    assert config["user_id"] == user_id and config["api_key"] == api_key

    os.remove(banyanconfig_path)
