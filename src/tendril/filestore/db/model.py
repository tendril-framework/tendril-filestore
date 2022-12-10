

from sqlalchemy import Column
from sqlalchemy import String
from sqlalchemy import Integer
from sqlalchemy import ForeignKey
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
    files = relationship("StoredFile", back_populates="bucket")


class StoredFileModel(ArtefactModel):
    _type_name = 'stored_file'
    id = Column(Integer, ForeignKey("Artefact.id"), primary_key=True)
    filename = Column(String(255))
    fileinfo = Column(mutable_json_type(dbtype=JSONB, nested=True))
    bucket_id = Column(Integer(),
                       ForeignKey('FilestoreBucket.id'), nullable=False)
    bucket = relationship("FilestoreBucket", back_populates="files")

    __mapper_args__ = {
        "polymorphic_identity": _type_name,
    }
