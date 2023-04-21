

import os


class FilestoreBucketBase(object):
    def __init__(self, uri, name, expose_uri=None,
                 accept_ext=None, allow_delete=False, allow_overwrite=False):
        self._id = None
        self._uri = uri
        self._name = name
        self._expose_uri = expose_uri or ''
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

    @property
    def allow_overwrite(self):
        return self._allow_overwrite

    @property
    def allow_delete(self):
        return self._allow_delete

    @property
    def accept_ext(self):
        return self._accept_ext

    def check_accepts(self, filename):
        name, ext = os.path.splitext(filename)
        return ext in self._accept_ext

    def upload(self, file, user, overwrite=False):
        raise NotImplementedError

    def move(self, filename, target_bucket, user, overwrite=False):
        raise NotImplementedError

    def list(self, page=None):
        raise NotImplementedError

    def list_info(self, include_owner=False, filenames=None):
        raise NotImplementedError

    def find(self, spec):
        raise NotImplementedError

    def delete(self, filename, user):
        raise NotImplementedError

    def purge(self, user):
        raise NotImplementedError

    def prune(self, user):
        raise NotImplementedError
