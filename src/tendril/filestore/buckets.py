

from tendril import config
from .db.controller import register_bucket

from tendril.utils import log
logger = log.get_logger(__name__, log.DEFAULT)

import sys
mod = sys.modules[__name__]


class FilestoreBucket(object):
    def __init__(self, uri, name, accept_ext=None, allow_delete=False):
        self._uri = uri
        self._name = name
        self._accept_ext = accept_ext or []
        self._allow_delete = allow_delete
        self._create_in_db()

    def _create_in_db(self):
        register_bucket(name=bucket_name)

    def check_accepts(self, filename):
        pass


_available_buckets = {}

def available_buckets():
    return _available_buckets.keys()


def _bucket_config(bucket_name):
    bucket_name = bucket_name.upper()
    enabled = getattr(config, "FILESTORE_{}_ENABLED".format(bucket_name))
    accept_ext = getattr(config, "FILESTORE_{}_ACCEPT_EXT".format(bucket_name))
    allow_delete = getattr(config, "FILESTORE_{}_ALLOW_DELETE".format(bucket_name))
    actual_uri = getattr(config, "FILESTORE_{}_ACTUAL_URI".format(bucket_name))
    return enabled, accept_ext, allow_delete, actual_uri


def init():
    for bucket_name in config.FILESTORE_BUCKETS:
        enabled, accept_ext, allow_delete, actual_uri = _bucket_config(bucket_name)
        if not enabled:
            logger.debug("Bucket '{}' not enabled. Skipping.".format(bucket_name))
            continue
        logger.info("Creating filestore bucket '{}' at {}".format(bucket_name, actual_uri))
        bucket = FilestoreBucket(actual_uri, bucket_name, accept_ext, allow_delete)
        _available_buckets[bucket_name] = bucket
        setattr(mod, bucket_name, bucket)


init()
