

import os


class FilestoreBucketBase(object):
    def __init__(self, uri, name, accept_ext=None, allow_delete=False, allow_overwrite=False):
        self._id = None
        self._uri = uri
        self._name = name
        self._accept_ext = accept_ext or []
        self._allow_delete = allow_delete
        self._allow_overwrite = allow_overwrite

    @property
    def id(self):
        return self._id

    @property
    def name(self):
        return self._name

    @property
    def uri(self):
        return self._uri

    def check_accepts(self, filename):
        name, ext = os.path.splitext(filename)
        return ext in self._accept_ext

    def upload(self, file, user, overwrite=False):
        raise NotImplementedError

    def move(self, filename, target_bucket, user, overwrite=False):
        raise NotImplementedError

    def delete(self, filename, user):
        raise NotImplementedError

    def list(self):
        raise NotImplementedError

    def purge(self, user):
        raise NotImplementedError

    def prune(self, user):
        raise NotImplementedError

    def find(self, spec):
        raise NotImplementedError