import codecs

import cloudpickle


def to_str(obj):
    return codecs.encode(cloudpickle.dumps(obj), "base64").decode()


def from_str(pickled):
    return cloudpickle.loads(codecs.decode(pickled.encode(), "base64"))
