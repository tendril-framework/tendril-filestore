

import json
import datetime
from fastapi import APIRouter
from fastapi import Depends
from fastapi import Request
from fastapi import File
from fastapi import UploadFile
from fastapi import HTTPException
from fastapi import Body
from fastapi_pagination import Page
from fastapi_pagination import Params
from fastapi_pagination import add_pagination

from typing import Union
from pydantic import Field
from pydantic import validator

from tendril.authn.users import auth_spec
from tendril.authn.users import AuthUserModel
from tendril.authn.users import authn_dependency
from tendril.authn.pydantic import UserStubTMixin

from tendril.filestore.buckets import available_buckets
from tendril.filestore.buckets import get_bucket
from tendril.filestore.actual import FilestoreBucket

from tendril.utils.pydantic import TendrilTBaseModel

from tendril.config import FILESTORE_ENABLED


filestore = APIRouter(prefix='/filestore',
                      tags=["File Management API"],
                      dependencies=[Depends(authn_dependency),
                                    auth_spec(scopes=['file_management:common'])])


filestore_management = APIRouter(prefix='/filestore',
                                 tags=["File Management Administration API"],
                                 dependencies=[Depends(authn_dependency),
                                               auth_spec(scopes=['file_management:admin'])])


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


class StoredFile(str):
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
    overwrite : bool = False


@filestore.get("/buckets")
async def get_available_buckets():
    return {'available_buckets': available_buckets()}


@filestore.post("/{bucket}/upload")
async def upload_file_to_bucket(
        request: Request,
        bucket: BucketName,
        overwrite: bool = False,
        file: UploadFile = File(...),
        user: AuthUserModel = auth_spec()):
    try:
        bucket: FilestoreBucket = get_bucket(bucket)
    except KeyError:
        raise HTTPException(
            status_code=404,
            detail=f'{bucket} is not a recognized filestore bucket'
        )
    if not bucket.check_accepts(file.filename):
        raise HTTPException(
            status_code=415,
            detail=f"This bucket does not allow uploads with this extension"
        )

    try:
        sf = bucket.upload(file, user.id, overwrite=overwrite)
    except FileExistsError as e:
        raise HTTPException(
            status_code=409,
            detail=str(e)
        )
    return {'storedfileid': sf.id}
    # print(request.headers.get('authorization'))
    # print(user)
    # print(file, file.filename)


@filestore_management.post("/{bucket}/move")
async def move_file_from_bucket(
        request: Request,
        bucket: BucketName,
        move_request: MoveRequest,
        user: AuthUserModel = auth_spec()):

    try:
        source_bucket: FilestoreBucket = get_bucket(bucket)
    except KeyError:
        raise HTTPException(
            status_code=404,
            detail=f'{bucket} is not a recognized filestore bucket'
        )

    try:
        target_bucket: FilestoreBucket = get_bucket(move_request.to_bucket)
    except KeyError:
        raise HTTPException(
            status_code=404,
            detail=f'{move_request.to_bucket} is not a recognized filestore bucket'
        )

    try:
        sf = source_bucket.move(filename=move_request.filename,
                                target_bucket=target_bucket,
                                user=user.id,
                                overwrite=move_request.overwrite)
    except FileNotFoundError:
        raise HTTPException(
            status_code=404,
            detail=f'{move_request.filename} does not exist in the source bucket'
        )
    return {'storedfileid': sf.id}


@filestore_management.post("/{bucket}/delete")
async def delete_file_from_bucket(
        request: Request,
        bucket: BucketName,
        filename: str,
        user: AuthUserModel = auth_spec()):
    try:
        bucket: FilestoreBucket = get_bucket(bucket)
    except KeyError:
        raise HTTPException(
            status_code=404,
            detail=f'{bucket} is not a recognized filestore bucket'
        )

    try:
        bucket.delete(filename, user.id)
    except FileNotFoundError as e:
        raise HTTPException(
            status_code=404,
            detail=str(e)
        )
    except PermissionError as e:
        raise HTTPException(
            status_code=403,
            detail=str(e)
        )
    return {'deleted': filename}


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


@filestore_management.post("/{bucket}/ls",
                           response_model=Page[StoredFileTModel])
async def list_files_in_bucket(
        request: Request,
        bucket: BucketName,
        params: Params = Depends(),
        user: AuthUserModel = auth_spec()):
    try:
        bucket: FilestoreBucket = get_bucket(bucket)
    except KeyError:
        raise HTTPException(
            status_code=404,
            detail=f'{bucket} is not a recognized filestore bucket'
        )
    return bucket.list_info(params)


@filestore_management.post("/{bucket}/purge")
async def purge_all_files_in_bucket(
        request: Request,
        bucket: BucketName,
        user: AuthUserModel = auth_spec()):
    try:
        bucket: FilestoreBucket = get_bucket(bucket)
    except KeyError:
        raise HTTPException(
            status_code=404,
            detail=f'{bucket} is not a recognized filestore bucket'
        )
    return bucket.purge(user.id)


if FILESTORE_ENABLED:
    routers = [
        filestore,
        filestore_management
    ]
else:
    routers = []
