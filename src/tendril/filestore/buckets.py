

import asyncio
from tendril import config
from tendril.filestore.actual import FilestoreBucket
from tendril.filestore.remote import FilestoreBucketRemote
from tendril.filestore.remote import get_remote_bucket_list

from tendril.utils import log
logger = log.get_logger(__name__, log.DEFAULT)


_available_buckets = {}


def available_buckets():
    return list(_available_buckets.keys())


def get_bucket(bucket_name):
    return _available_buckets[bucket_name]


def _bucket_config(bucket_name):
    bucket_name = bucket_name.upper()
    enabled = getattr(config, "FILESTORE_{}_ENABLED".format(bucket_name))
    accept_ext = getattr(config, "FILESTORE_{}_ACCEPT_EXT".format(bucket_name))
    allow_delete = getattr(config, "FILESTORE_{}_ALLOW_DELETE".format(bucket_name))
    allow_overwrite = getattr(config, "FILESTORE_{}_ALLOW_OVERWRITE".format(bucket_name))
    actual_uri = getattr(config, "FILESTORE_{}_ACTUAL_URI".format(bucket_name))
    expose_uri = getattr(config, "FILESTORE_{}_EXPOSE_URI".format(bucket_name))
    return enabled, accept_ext, expose_uri, allow_delete, allow_overwrite, actual_uri


def init_remote():
    if not config.FILESTORE_REMOTE_URI:
        logger.warning("Filestore is not enabled and a remote filestore "
                       "has not been configured. Filestore operations "
                       "should be executed via the appropriate API on "
                       "the filestore component. ")
    else:
        logger.info("Attempting to make a connection to the remote filestore.")
        uri = config.FILESTORE_REMOTE_URI
        remote_bucket_list = asyncio.run(get_remote_bucket_list(uri))
        for bucket_name in config.FILESTORE_BUCKETS:
            enabled, accept_ext, expose_uri, allow_delete, allow_overwrite, _ = _bucket_config(bucket_name)
            if enabled and bucket_name in remote_bucket_list:
                logger.info(f"Creating proxy to the remote filestore bucket {bucket_name} at {uri}.")
                bucket = FilestoreBucketRemote(uri, bucket_name, expose_uri, accept_ext, allow_delete, allow_overwrite)
                _available_buckets[bucket_name] = bucket


def init_actual():
    for bucket_name in config.FILESTORE_BUCKETS:
        enabled, accept_ext, expose_uri, allow_delete, allow_overwrite, actual_uri = _bucket_config(bucket_name)
        if not enabled:
            logger.debug("Bucket '{}' not enabled. Skipping.".format(bucket_name))
            continue
        logger.info("Creating filestore bucket '{}' at {}".format(bucket_name, actual_uri))
        bucket = FilestoreBucket(actual_uri, bucket_name, expose_uri, accept_ext, allow_delete, allow_overwrite)
        _available_buckets[bucket_name] = bucket


def init():
    if not config.FILESTORE_ENABLED:
        logger.info("Filestore actual not enabled on this component.")
        init_remote()
    else:
        logger.info("Filestore actual enabled on this component. Initializing.")
        init_actual()


init()
