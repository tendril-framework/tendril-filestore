

from urllib.parse import urljoin
from sqlalchemy import Column
from sqlalchemy import String
from sqlalchemy import Integer
from sqlalchemy import ForeignKey
from sqlalchemy import UniqueConstraint
from sqlalchemy.orm import relationship
from sqlalchemy_json import mutable_json_type
from sqlalchemy.dialects.postgresql import JSONB

from tendril.artefacts.db.model import ArtefactModel

from tendril.utils.db import DeclBase
from tendril.utils.db import BaseMixin


from tendril.utils import log
logger = log.get_logger(__name__, log.DEFAULT)


class FilestoreBucketModel(DeclBase, BaseMixin):
    name = Column(String(50), nullable=False, unique=True)
    files = relationship("StoredFileModel", back_populates="bucket")

    @property
    def actual(self):
        if not hasattr(self, '_actual'):
            from tendril.filestore.buckets import get_bucket
            self._actual = get_bucket(self.name)
        return self._actual


class StoredFileModel(ArtefactModel):
    _type_name = 'stored_file'
    id = Column(Integer, ForeignKey("Artefact.id"), primary_key=True)
    filename = Column(String(255), nullable=False)
    fileinfo = Column(mutable_json_type(dbtype=JSONB, nested=True))
    bucket_id = Column(Integer(),
                       ForeignKey('FilestoreBucket.id'), nullable=False)
    bucket = relationship("FilestoreBucketModel", back_populates="files")

    @property
    def expose_uri(self):
        return urljoin(self.bucket.actual.expose_uri, self.filename)

    @property
    def x_sendfile_uri(self):
        return urljoin(self.bucket.actual.x_sendfile_prefix, self.filename)

    __mapper_args__ = {
        "polymorphic_identity": _type_name,
    }

    __table_args__ = (
        UniqueConstraint('filename', 'bucket_id'),
    )

