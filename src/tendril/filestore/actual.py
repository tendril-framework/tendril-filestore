

import hashlib
import os

from fs import open_fs
from fs import move
from fs.osfs import OSFS
from sqlalchemy.exc import NoResultFound

# Causes a circular import issue. Does not actually seem to be needed.
# from tendril.authn.users import get_user_stub
from tendril.filestore.base import FilestoreBucketBase
from tendril.filestore.db.controller import register_bucket
from tendril.filestore.db.controller import get_storedfile_owner
from tendril.filestore.db.controller import register_stored_file
from tendril.filestore.db.controller import change_file_bucket
from tendril.filestore.db.controller import delete_stored_file
from tendril.filestore.db.controller import get_paginated_stored_files

from tendril.utils.db import with_db
from tendril.utils.db import get_session
from tendril.utils import log
logger = log.get_logger(__name__, log.DEFAULT)


class FilestoreBucket(FilestoreBucketBase):
    _exclude_filenames = []

    _exclude_directories = [
        'lost+found'
    ]

    def __init__(self, *args, **kwargs):
        super(FilestoreBucket, self).__init__(*args, **kwargs)
        self._fs = None
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
    def fs(self) -> OSFS:
        return self._fs

    def _create_in_db(self):
        b = register_bucket(name=self.name)
        self._id = b.id

    @with_db
    def _prep_for_upload(self, bucket, filename, user, interest=None, overwrite=False, auto_prune=True, session=None):
        subdir, _ = os.path.split(filename)
        if subdir:
            self.fs.makedirs(subdir, recreate=True)
        if bucket.fs.exists(filename):
            # File exists in the filesystem
            if not overwrite and not auto_prune:
                # We have no way to remove the existing file.
                raise FileExistsError(f'{filename} already exists in the {bucket.name} bucket. Delete it first.')
            try:
                owner = get_storedfile_owner(filename, bucket.id, session=session)
            except NoResultFound:
                # File exists in fs but not in the DB.
                if not auto_prune:
                    # We aren't allowed to remove it.
                    raise FileExistsError(f"'{filename}' already exists in the '{bucket.name}' filesystem but "
                                          f"not in the database. This needs to be manually resolved.")
                logger.warning(f"'{filename}' exists in the '{bucket.name}' filesystem but "
                               f"not in the database. Pruning. Possible Data Loss.")
                bucket.fs.remove(filename)
            else:
                # File also exists in the database.
                if not overwrite:
                    raise FileExistsError(f'{filename} already exists in the {bucket.name} bucket. Delete it first.')
                # We may still be able to overwrite it.
                if not bucket.allow_overwrite:
                    # Bucket forbids overwriting
                    raise FileExistsError(f'{filename} already exists in the {bucket.name} bucket '
                                          f'and the bucket prohibits overwrites.')
                if not self._check_ownership(owner, user):
                    # Exisiting file is owned by someone else.
                    raise FileExistsError(f'{filename} already exists in the {bucket.name} bucket '
                                      f'and owned by someone else.')
                logger.warning(f"Overwriting file {filename} in bucket {bucket.name}.")
                bucket.delete(filename, user, interest, session=session)

    @with_db
    def upload(self, file, user, interest=None, overwrite=False, session=None):
        filename = file.filename
        self._prep_for_upload(self, filename, user, interest, overwrite, session=session)

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

        file.file.seek(0)
        sha256hash = hashlib.sha256()
        chunk = 0
        while True:
            chunk = file.file.read(2**10)
            if not chunk: break
            sha256hash.update(chunk)

        fileinfo = {'props': {'size': info.size, 'created': created, 'modified': modified},
                    'hash': {'sha256': sha256hash.hexdigest()},
                    'ext': ''.join(info.suffixes)}

        sf = register_stored_file(filename, self._id, user, interest, fileinfo, session=session)

        return sf

    @with_db
    def move(self, filename, target_bucket, user, overwrite=False, session=None):
        if not self._fs.exists(filename):
            raise FileNotFoundError(f"Move of nonexisting file {filename} "
                                    f"from bucket {self.name} requested.")

        self._prep_for_upload(target_bucket, filename, user, overwrite)

        logger.debug(f"Moving file {filename} from bucket {self.name} to {target_bucket.name}")
        move.move_file(self.fs, filename, target_bucket.fs, filename)
        return change_file_bucket(filename, self.id, target_bucket.id, user, session=session)

    def _list(self, path='/', page=None):
        for f in self.fs.filterdir(path, page=page,
                                   exclude_files=self._exclude_filenames,
                                   exclude_dirs=self._exclude_directories):
            yield f.name

    def list(self, path='/', page=None):
        return list(self._list(path=path, page=page))

    def list_info(self, include_owner=False, filenames=None,
                  pagination_params=None, page=None):
        kwargs = {}
        if filenames:
            kwargs['filenames'] = filenames
        return get_paginated_stored_files(
            pagination_params=pagination_params,
            bucket=self.id, include_owner=include_owner,
            **kwargs
        )

    @with_db
    def delete(self, filename, user, session=None):
        if not self._fs.exists(filename):
            raise FileNotFoundError(f"Delete of nonexisting file {filename} "
                                    f"from bucket {self.name} requested.")

        if not self._allow_delete:
            owner = get_storedfile_owner(filename, self._id, session=session)
            if not self._check_ownership(owner, user):
                raise PermissionError(f"Deletion of file {filename} "
                                      f"not permitted from bucket {self.name}")

        logger.info(f"Deleting {filename} from bucket {self.name}")
        self.fs.remove(filename)
        delete_stored_file(filename, self.id, user, session=session)

    def purge(self, user):
        if not self._allow_delete:
            raise PermissionError(f"Deletion of files from bucket {self.name} "
                                  f"is not permitted")
        logger.warning(f"Purging all files from bucket {self.name}")
        for filename in self.list():
            with get_session() as session:
                logger.info(f"Deleting file {filename} from bucket {self.name}")
                self.fs.remove(filename)
                delete_stored_file(filename, self.id, user, session=session)

    def __repr__(self):
        return "<FilestoreBucket {} at {}>".format(self.name, self.uri)
