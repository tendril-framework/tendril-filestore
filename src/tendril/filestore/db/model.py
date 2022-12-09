

from sqlalchemy import Column
from sqlalchemy import String

from tendril.utils.db import DeclBase
from tendril.utils.db import BaseMixin
from tendril.utils.db import TimestampMixin

from tendril.utils import log
logger = log.get_logger(__name__, log.DEFAULT)


class FilestoreBucket(DeclBase, BaseMixin):
    name = Column(String(50), nullable=False, unique=True)
