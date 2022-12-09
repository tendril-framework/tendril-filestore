

from sqlalchemy.orm.exc import NoResultFound
from tendril.utils.db import with_db

from .model import FilestoreBucket

from tendril.utils import log
logger = log.get_logger(__name__, log.DEFAULT)


@with_db
def get_bucket(name, session=None):
    q = session.query(FilestoreBucket).filter_by(name=name)
    return q.one()


@with_db
def register_bucket(name, must_create=False, session=None):
    if name is None:
        raise AttributeError("name cannot be None")

    try:
        existing = get_bucket(name)
    except NoResultFound:
        bucket = FilestoreBucket(name=name)
    else:
        if must_create:
            raise ValueError("Filestore Bucket Already Exists")
        else:
            bucket = existing
    session.add(bucket)
    return bucket
