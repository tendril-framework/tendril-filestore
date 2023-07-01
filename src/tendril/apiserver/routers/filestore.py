

from typing import List
from typing import Optional
from fastapi import APIRouter
from fastapi import Depends
from fastapi import Request
from fastapi import Response
from fastapi import File
from fastapi import UploadFile
from fastapi import HTTPException
from fastapi_pagination import Page
from fastapi_pagination import Params

from tendril.authn.users import auth_spec
from tendril.authn.users import AuthUserModel
from tendril.authn.users import authn_dependency
from tendril.authn.pydantic import UserReferenceTModel

from tendril.filestore.buckets import get_bucket
from tendril.filestore.buckets import available_buckets
from tendril.filestore.actual import FilestoreBucket

from tendril.common.filestore.formats import BucketName
from tendril.common.filestore.formats import MoveRequest
from tendril.common.filestore.formats import StoredFileTModel

from tendril.config import FILESTORE_ENABLED
from tendril.config import FILESTORE_EXPOSE_ENABLED
from tendril.utils import log
logger = log.get_logger(__name__, log.DEFAULT)


filestore = APIRouter(prefix='/filestore',
                      tags=["Filestore Common API"],
                      dependencies=[Depends(authn_dependency),
                                    auth_spec(scopes=['file_management:common'])])


filestore_management = APIRouter(prefix='/filestore',
                                 tags=["File Administration API"],
                                 dependencies=[Depends(authn_dependency),
                                               auth_spec(scopes=['file_management:admin'])])


expose = APIRouter(prefix='/filestore',
                   tags=['Filestore Protected Expose API'],
                   dependencies=[Depends(authn_dependency)])


@filestore.get("/buckets")
async def get_available_buckets():
    return {'available_buckets': available_buckets()}


@filestore.post("/{bucket}/upload")
async def upload_file_to_bucket(
        request: Request,
        bucket: BucketName,
        overwrite: bool = False,
        actual_user: Optional[UserReferenceTModel] = None,
        file: UploadFile = File(...),
        interest: Optional[int] = None,
        user: AuthUserModel = auth_spec()):

    try:
        bucket: FilestoreBucket = get_bucket(bucket)
    except KeyError:
        logger.info(f"Got upload request with bad bucket '{bucket}' ({file.filename})")
        raise HTTPException(
            status_code=404,
            detail=f'{bucket} is not a recognized filestore bucket'
        )

    if not bucket.check_accepts(file.filename):
        logger.info(f"Got upload request with bad extension ({file.filename})")
        raise HTTPException(
            status_code=415,
            detail=f"This bucket does not allow uploads with this extension"
        )

    try:
        actual_user = actual_user or user.id
        sf = bucket.upload(file, actual_user, interest=interest, overwrite=overwrite)
    except FileExistsError as e:
        logger.info(e)
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
        actual_user: Optional[UserReferenceTModel] = None,
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
                                user=actual_user or user.id,
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
        actual_user: Optional[UserReferenceTModel] = None,
        user: AuthUserModel = auth_spec()):

    try:
        bucket: FilestoreBucket = get_bucket(bucket)
    except KeyError:
        raise HTTPException(
            status_code=404,
            detail=f'{bucket} is not a recognized filestore bucket'
        )

    try:
        bucket.delete(filename, actual_user or user.id)
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


@filestore_management.get("/{bucket}/ls_fs",
                          response_model=List[str])
async def list_files_in_bucket_fs(
        request: Request,
        bucket: BucketName, path: str = '/',
        user: AuthUserModel = auth_spec()):

    try:
        bucket: FilestoreBucket = get_bucket(bucket)
    except KeyError:
        raise HTTPException(
            status_code=404,
            detail=f'{bucket} is not a recognized filestore bucket'
        )
    return bucket.list(path=path)


@filestore_management.get("/{bucket}/ls",
                          response_model=Page[StoredFileTModel],
                          response_model_exclude_none=True)
async def list_files_in_bucket(
        request: Request,
        bucket: BucketName,
        include_owner: bool = False,
        params: Params = Depends(),
        user: AuthUserModel = auth_spec()):

    try:
        bucket: FilestoreBucket = get_bucket(bucket)
    except KeyError:
        raise HTTPException(
            status_code=404,
            detail=f'{bucket} is not a recognized filestore bucket'
        )
    return bucket.list_info(include_owner=include_owner,
                            pagination_params=params)


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


@expose.get("/{bucket}/expose/{filepath:path}")
async def get_exposed_file(request: Request, bucket: BucketName,
                           filepath: str, response: Response,
                           user: AuthUserModel = auth_spec()):
    try:
        bucket: FilestoreBucket = get_bucket(bucket)
    except KeyError:
        raise HTTPException(
            status_code=404,
            detail=f'{bucket} is not a recognized filestore bucket'
        )
    response.headers['X-Accel-Redirect'] = bucket.expose(filepath, user=user)
    return


if FILESTORE_ENABLED:
    routers = [
        filestore,
        filestore_management,
    ]
else:
    routers = []


if FILESTORE_EXPOSE_ENABLED:
    routers.append(expose)
