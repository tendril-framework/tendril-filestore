

from fastapi import APIRouter
from fastapi import Depends
from fastapi import Request
from fastapi import File
from fastapi import UploadFile
from fastapi import HTTPException
from fastapi import Body

from pydantic import BaseModel

from tendril.authn.users import authn_dependency
from tendril.authn.users import AuthUserModel
from tendril.authn.users import auth_spec
from tendril.filestore.buckets import available_buckets
from tendril.filestore.buckets import get_bucket
from tendril.filestore.buckets import FilestoreBucket

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


# class MoveRequest(BaseModel):
#     bucket: BucketName
#     filename: str


@filestore.get("/buckets")
async def available_buckets():
    return {'available_buckets': available_buckets()}


@filestore.post("/{bucket}/upload")
async def upload_file_to_bucket(
        request: Request,
        bucket: BucketName,
        file: UploadFile = File(...),
        user: AuthUserModel = auth_spec()):
    bucket: FilestoreBucket = get_bucket(bucket)
    if not bucket.check_accepts(file.filename):
        raise HTTPException(
            status_code=415,
            detail=f"This bucket does not allow uploads with this extension"
        )
    sf = bucket.upload(file, user.id)
    return {'storedfileid': sf.id}
    # print(request.headers.get('authorization'))
    # print(user)
    # print(file, file.filename)


# @filestore.post("/move")
# async def move(request: Request, response: Response,
#                to_bucket: BucketName,
#                user: AuthUserModel = auth_spec()):
#     return {'available_buckets': available_buckets()}


if FILESTORE_ENABLED:
    routers = [
        filestore,
        filestore_management
    ]
else:
    routers = []
