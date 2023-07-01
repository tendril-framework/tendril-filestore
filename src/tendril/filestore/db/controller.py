

from functools import partial
from sqlalchemy import select
from fastapi_pagination.ext.sqlalchemy import paginate
from sqlalchemy.orm.exc import NoResultFound

from tendril import config
from tendril.utils.db import with_db
from tendril.authn.db.model import User
from tendril.authn.db.controller import preprocess_user
from tendril.artefacts.db.controller import get_artefact_owner
from tendril.db.controllers.interests import preprocess_interest

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
def get_stored_files(bucket, filenames=None, session=None):
    bucket_id = preprocess_bucket(bucket, session=None)
    filters = [StoredFileModel.bucket_id == bucket_id]

    if filenames:
        filters.append(StoredFileModel.filename.in_(filenames))

    q = session.query(StoredFileModel).filter(*filters)
    return q.all()


def _stored_file_transformer(items, include_owner=False):
    if not include_owner:
        return [{'filename': x[0], 'fileinfo': x[1]}
                for x in items]
    return [
        {'filename': x[0],
         'fileinfo': x[1],
         'puid': x[2]}
        for x in items
    ]


@with_db
def get_paginated_stored_files(bucket, pagination_params=None, filenames=None, include_owner=False, session=None):
    bucket_id = preprocess_bucket(bucket, session=None)
    filters = [StoredFileModel.bucket_id == bucket_id]

    if filenames:
        filters.append(StoredFileModel.filename.in_(filenames))

    if include_owner:
        stmt = select(StoredFileModel.filename, StoredFileModel.fileinfo, User.puid)\
            .join(StoredFileModel.user)\
            .filter(*filters)
    else:
        stmt = select(StoredFileModel.filename, StoredFileModel.fileinfo)\
            .filter(*filters)

    if pagination_params:
        return paginate(query=stmt, conn=session, unique=False,
                        params=pagination_params,
                        transformer=partial(_stored_file_transformer,
                                            include_owner=include_owner))
    else:
        return _stored_file_transformer(session.execute(stmt).all(),
                                        include_owner=include_owner)


@with_db
def register_stored_file(filename, bucket, user, interest=None, fileinfo=None, overwrite=True, session=None):
    if not config.FILESTORE_ENABLED:
        raise EnvironmentError("Filestore not enabled on this component. "
                               "Use the filestore API on the filestore component instead.")

    if filename is None:
        raise AttributeError("name cannot be None")

    bucket_id = preprocess_bucket(bucket, session=session)
    user_id = preprocess_user(user, session=session)
    if interest:
        interest_id = preprocess_interest(interest)
    else:
        interest_id = None

    try:
        existing = get_stored_file(filename, bucket_id, session=session)
    except NoResultFound:
        storedfile = StoredFileModel(filename=filename,
                                     bucket_id=bucket_id,
                                     fileinfo=fileinfo,
                                     user_id=user_id,
                                     interest_id=interest_id,
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
def change_file_bucket(filename, bucket, target_bucket, user, interest=None, session=None):
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
    if not sf.interest:
        return {'user': user}
    try:
        from tendril.interests import type_codes
        interest = type_codes[sf.interest.type](sf.interest, can_create=False)
        return {'user': user, 'interest': interest}
    except ImportError:
        return {'user': user}


@with_db
def delete_stored_file(filename, bucket, user, session=None):
    sf = get_stored_file(filename=filename, bucket=bucket, session=session)

    # TODO Create Log Entry and archive log?

    session.delete(sf)
    return
