

import os
from sqlalchemy.orm.exc import NoResultFound

from tendril.filestore.db.controller import get_stored_file
from tendril.filestore.db.controller import get_storedfile_owner
from tendril.utils.db import with_db


class FilestoreBucketBase(object):
    def __init__(self, uri, name, expose_uri=None,
                 accept_ext=None, allow_delete=False, allow_overwrite=False):
        self._id = None
        self._uri = uri
        self._name = name
        self._expose_uri = expose_uri or ''
        self._x_sendfile_prefix = '/protected/'
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
    def expose_uri(self):
        return self._expose_uri

    @property
    def x_sendfile_prefix(self):
        return self._x_sendfile_prefix

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

    def _check_ownership(self, owner, user):
        if owner['user'].puid == user:
            return True
        if 'interest' not in owner.keys() or not owner['interest']:
            return False
        if owner['interest'].check_user_access(user, 'delete_artefact'):
            return True
        return False

    def _check_access(self, owner, user):
        if owner['user'].puid == user.id:
            return True
        if 'interest' not in owner.keys() or not owner['interest']:
            return False
        if owner['interest'].check_user_access(user.id, 'read_artefact'):
            return True
        return False

    @with_db
    def expose(self, filename, user, session=None):

        try:
            owner = get_storedfile_owner(filename, self._id, session=session)
        except NoResultFound:
            raise FileNotFoundError(f"Requested file {filename} does not exist in "
                                    f"the bucket {self.name}.")

        if not self._check_access(owner, user):
            raise PermissionError(f"Access to the file {filename} is not "
                                  f"granted to user {user.id}")

        sf = get_stored_file(filename=filename, bucket=self.id, session=session)
        return sf.x_sendfile_uri
