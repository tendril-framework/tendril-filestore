

import os
from fs import move
from fs import open_fs
from tendril import config
from .db.controller import register_bucket
from .db.controller import register_stored_file
from .db.controller import change_file_bucket

from tendril.utils import log
logger = log.get_logger(__name__, log.DEFAULT)


class FilestoreBucket(object):
    def __init__(self, uri, name, accept_ext=None, allow_delete=False):
        self._id = None
        self._uri = uri
        self._name = name
        self._accept_ext = accept_ext or []
        self._allow_delete = allow_delete
        self._create_in_db()
        self._prep_fs()

    def _prep_fs(self):
        if self._uri.startswith("osfs://"):
            path = self._uri[7:]
            if path.startswith('~'):
                path = os.path.expanduser(path)
            path = os.path.normpath(path)
            os.makedirs(path, exist_ok=True)
        self._fs = open_fs(self._uri)

    @property
    def id(self):
        return self._id

    @property
    def name(self):
        return self._name

    @property
    def uri(self):
        return self._uri

    @property
    def fs(self):
        return self._fs

    def _create_in_db(self):
        b = register_bucket(name=self.name)
        self._id = b.id

    def check_accepts(self, filename):
        name, ext = os.path.splitext(filename)
        return ext in self._accept_ext

    def upload(self, file, user, overwrite=True):
        filename = file.filename

        if self._fs.exists(filename):
            if not overwrite:
                raise FileExistsError(f'{filename} already exists in the bucket. Delete it first.')
            logger.warning(f"Overwriting file {filename} in bucket {self.name}.")
            self._fs.remove(filename)

        with self._fs.open(filename, 'wb') as target:
            logger.debug(f"Writing file {filename} to bucket {self.name}")
            target.write(file.file.read())

        info = self._fs.getinfo(filename, namespaces=['details'])

        created = info.created
        if created:
            created = info.created.isoformat()

        modified = info.modified
        if modified:
            modified = info.modified.isoformat()

        # TODO Get file hash

        fileinfo = {'props': {'size': info.size, 'created': created, 'modified': modified}}
        sf = register_stored_file(filename, self._id, user, fileinfo)

        return sf

    def move(self, filename, target_bucket, user):
        if not self._fs.exists(filename):
            raise FileNotFoundError(f"Move of nonexisting file {filename} "
                                    f"from bucket {self.name} requested.")
        move.move_file(self.fs, filename, target_bucket.fs, filename)
        return change_file_bucket(filename, self.id, target_bucket.id, user)

    def __repr__(self):
        return "<FilestoreBucket {} at {}>".format(self.name, self.uri)


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


init()
