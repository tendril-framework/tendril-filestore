

from sqlalchemy.orm.exc import NoResultFound
from tendril.utils.db import with_db
from tendril.authn.db.controller import preprocess_user
from tendril.artefacts.db.controller import get_artefact_owner
from tendril import config

from .model import FilestoreBucketModel
from .model import StoredFileModel

from tendril.utils import log
logger = log.get_logger(__name__, log.DEFAULT)


@with_db
def get_bucket(name, session=None):
    q = session.query(FilestoreBucketModel).filter_by(name=name)
    return q.one()


@with_db
def register_bucket(name, must_create=False, session=None):
    if not config.FILESTORE_ENABLED:
        raise EnvironmentError("Filestore not enabled on this component. "
                               "Use the filestore API on the filestore component instead.")

    if name is None:
        raise AttributeError("name cannot be None")

    try:
        existing = get_bucket(name, session=session)
    except NoResultFound:
        bucket = FilestoreBucketModel(name=name)
    else:
        if must_create:
            raise ValueError("Filestore Bucket Already Exists")
        else:
            bucket = existing
    session.add(bucket)
    return bucket


@with_db
def preprocess_bucket(bucket, session=None):
    if bucket is None:
        raise AttributeError("bucket cannot be None")
    if isinstance(bucket, int):
        bucket_id = bucket
    elif isinstance(bucket, FilestoreBucketModel):
        bucket_id = bucket.id
    else:
        try:
            bucket = get_bucket(bucket, session=session)
            bucket_id = bucket.id
        except NoResultFound:
            raise AttributeError(f"Filestore bucket {bucket} does not seem to exist.")
    return bucket_id


@with_db
def get_stored_file(filename, bucket, session=None):
    q = session.query(StoredFileModel).filter_by(filename=filename)
    if bucket:
        bucket_id = preprocess_bucket(bucket, session=session)
        q.filter_by(bucket_id=bucket_id)
    return q.one()


@with_db
def register_stored_file(filename, bucket, user, fileinfo=None, overwrite=True, session=None):
    if not config.FILESTORE_ENABLED:
        raise EnvironmentError("Filestore not enabled on this component. "
                               "Use the filestore API on the filestore component instead.")

    if filename is None:
        raise AttributeError("name cannot be None")

    bucket_id = preprocess_bucket(bucket, session=session)
    user_id = preprocess_user(user, session=session)

    try:
        existing = get_stored_file(filename, bucket_id, session=session)
    except NoResultFound:
        storedfile = StoredFileModel(filename=filename,
                                     bucket_id=bucket_id,
                                     fileinfo=fileinfo,
                                     user_id=user_id,
                                     type='stored_file')
        # TODO Create Log Entry?
    else:
        if not overwrite:
            raise ValueError(f"File {filename} seems to already exist in bucket "
                             f"{bucket_id}. For now, you might have to manually "
                             f"delete it from the database if this is something "
                             f"you really want to do.")
        else:
            existing.fileinfo = fileinfo
            storedfile = existing
            # TODO Create Log Entry?
    session.add(storedfile)
    return storedfile


@with_db
def change_file_bucket(filename, bucket, target_bucket, user, session=None):
    if not config.FILESTORE_ENABLED:
        raise EnvironmentError("Filestore not enabled on this component. "
                               "Use the filestore API on the filestore component instead.")

    storedfile: StoredFileModel = get_stored_file(filename, bucket, session=session)

    target_bucket = preprocess_bucket(target_bucket)
    storedfile.bucket_id = target_bucket

    # TODO Create Log Entry?

    session.add(storedfile)
    return storedfile


@with_db
def get_storedfile_owner(filename, bucket, session=None):
    sf = get_stored_file(filename=filename, bucket=bucket, session=session)
    user = get_artefact_owner(sf.id, session=None)
    return user


@with_db
def delete_stored_file(filename, bucket, user, session=None):
    sf = get_stored_file(filename=filename, bucket=bucket, session=session)

    # TODO Create Log Entry and archive log?

    session.delete(sf)
    return