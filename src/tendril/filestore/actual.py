

import hashlib
import os

from fs import open_fs
from fs import move

from tendril.filestore.base import FilestoreBucketBase
from tendril.filestore.db.controller import register_bucket
from tendril.filestore.db.controller import get_storedfile_owner
from tendril.filestore.db.controller import register_stored_file
from tendril.filestore.db.controller import change_file_bucket
from tendril.filestore.db.controller import delete_stored_file

from tendril.utils.db import with_db
from tendril.utils.db import get_session
from tendril.utils import log
logger = log.get_logger(__name__, log.DEFAULT)


class FilestoreBucket(FilestoreBucketBase):
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
    def fs(self):
        return self._fs

    def _create_in_db(self):
        b = register_bucket(name=self.name)
        self._id = b.id

    @with_db
    def upload(self, file, user, overwrite=False, session=None):
        filename = file.filename

        if self._fs.exists(filename):
            if not overwrite:
                raise FileExistsError(f'{filename} already exists in the {self.name} bucket. Delete it first.')
            owner = get_storedfile_owner(filename, self._id, session=session)
            if not self._allow_overwrite and owner.puid != user:
                raise FileExistsError(f'{filename} already exists in the {self.name} bucket and owned by someone else.')
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

        file.file.seek(0)
        sha256hash = hashlib.sha256()
        chunk = 0
        while True:
            chunk = file.file.read(2**10)
            if not chunk: break
            sha256hash.update(chunk)

        fileinfo = {'props': {'size': info.size, 'created': created, 'modified': modified},
                    'hash': {'sha256': sha256hash.hexdigest()}}

        sf = register_stored_file(filename, self._id, user, fileinfo, session=session)

        return sf

    @with_db
    def move(self, filename, target_bucket, user, overwrite=False, session=None):
        if not self._fs.exists(filename):
            raise FileNotFoundError(f"Move of nonexisting file {filename} "
                                    f"from bucket {self.name} requested.")

        if target_bucket.fs.exists(filename):
            if not overwrite:
                raise FileExistsError(f'{filename} already exists in the {target_bucket.name} bucket. Delete it first.')
            owner = get_storedfile_owner(filename, target_bucket.id, session=session)
            if not target_bucket._allow_overwrite and owner.puid != user:
                raise FileExistsError(f'{filename} already exists in the {target_bucket.name} bucket and owned by someone else.')
            logger.warning(f"Overwriting file {filename} in bucket {target_bucket.name}.")
            target_bucket.delete(filename)

        logger.debug(f"Moving file {filename} from bucket {self.name} to {target_bucket.name}")
        move.move_file(self.fs, filename, target_bucket.fs, filename)
        return change_file_bucket(filename, self.id, target_bucket.id, user, session=session)

    @with_db
    def delete(self, filename, user, session=None):
        if not self._fs.exists(filename):
            raise FileNotFoundError(f"Delete of nonexisting file {filename} "
                                    f"from bucket {self.name} requested.")

        if not self._allow_delete:
            owner = get_storedfile_owner(filename, self._id, session=session)
            if owner.puid != user:
                raise PermissionError(f"Deletion of file {filename} "
                                      f"not permitted from bucket {self.name}")

        logger.info(f"Deleting {filename} from bucket {self.name}")
        self.fs.remove(filename)
        delete_stored_file(filename, self.id, user, session=session)

    def list(self):
        return self.fs.listdir('/')

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
