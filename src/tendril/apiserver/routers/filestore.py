

from fastapi import APIRouter
from fastapi import Depends

from tendril.authn.users import authn_dependency
from tendril.authn.users import AuthUserModel
from tendril.authn.users import auth_spec
from tendril.filestore.buckets import available_buckets

from tendril.config import FILESTORE_ENABLED


filestore = APIRouter(prefix='/filestore',
                      tags=["File Management API"],
                      dependencies=[Depends(authn_dependency),
                                    auth_spec(scopes=['file_management:common'])])


filestore_management = APIRouter(prefix='/filestore',
                                 tags=["File Management Administration API"],
                                 dependencies=[Depends(authn_dependency),
                                               auth_spec(scopes=['file_management:admin'])])


@filestore.get("/buckets")
async def buckets():
    return {'available_buckets': available_buckets()}


if FILESTORE_ENABLED:
    routers = [
        filestore,
        filestore_management
    ]
else:
    routers = []
