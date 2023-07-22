

import json
import datetime
from typing import Union
from pydantic import Field
from pydantic import root_validator

from tendril.utils.pydantic import TendrilTBaseModel
from tendril.authn.pydantic import UserStubTMixin

from tendril.filestore.buckets import available_buckets
_available_buckets = available_buckets()


class BucketName(str):
    @classmethod
    def _get_validators__(cls):
        yield cls.validate

    @classmethod
    def validate(cls, v):
        if v not in _available_buckets:
            raise ValueError(f"'{v}' is not in {_available_buckets}")
        return cls(v)


class MoveRequest(TendrilTBaseModel):
    to_bucket: BucketName
    filename: str
    overwrite: bool = False


class StoredFilePropsTModel(TendrilTBaseModel):
    size: int = Field(..., example=714794)
    created: Union[datetime.datetime, None]
    modified: Union[datetime.datetime, None]


class StoredFileHashTModel(TendrilTBaseModel):
    sha256: str = Field(..., example='e4dd9b81d05aec0ce7f3a66b9efd15a13da5dae6e6672b84c7a75b3504c22d43')


class StoredFileInfoTModel(TendrilTBaseModel):
    ext: str = Field(..., example='.jpg')
    hash: StoredFileHashTModel
    props: StoredFilePropsTModel


class StoredFileTModel(UserStubTMixin(out='owner'), TendrilTBaseModel):
    filename: str = Field(..., example="some_filename.jpg")
    fileinfo: StoredFileInfoTModel
