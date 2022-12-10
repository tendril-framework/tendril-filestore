

from sqlalchemy import Column
from sqlalchemy import String
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.mutable import MutableDict

from tendril.utils.db import DeclBase
from tendril.utils.db import BaseMixin
from tendril.utils.db import TimestampMixin

from tendril.utils import log
logger = log.get_logger(__name__, log.DEFAULT)


class FilestoreBucket(DeclBase, BaseMixin):
    name = Column(String(50), nullable=False, unique=True)


class StoredFile(DeclBase, BaseMixin, TimestampMixin):
    pass
