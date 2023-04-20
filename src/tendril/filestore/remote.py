

from urllib.parse import urljoin
from tendril.authn.client import IntramuralAuthenticator
from tendril.utils.www import async_client

from tendril.config import FILESTORE_REMOTE_AUDIENCE
from tendril.config import FILESTORE_REMOTE_CLIENT_ID
from tendril.config import FILESTORE_REMOTE_CLIENT_SECRET

from .base import FilestoreBucketBase

from tendril.utils import log
logger = log.get_logger(__name__, log.DEFAULT)


_authenticator = IntramuralAuthenticator(
    FILESTORE_REMOTE_AUDIENCE,
    FILESTORE_REMOTE_CLIENT_ID,
    FILESTORE_REMOTE_CLIENT_SECRET
)


async def get_remote_bucket_list(remote_uri):
    async with async_client(base_url=remote_uri, auth=_authenticator) as client:
        response = await client.get('/v1/filestore/buckets')
        return response.json()["available_buckets"]


class FilestoreBucketRemote(FilestoreBucketBase):
    async def upload(self, file, user, overwrite=False):
        pass

    async def move(self, filename, target_bucket, user, overwrite=False):
        pass

    async def delete(self, filename, user):
        pass

    async def list(self):
        pass

    async def purge(self, user):
        pass

    async def prune(self, user):
        pass

    async def find(self, spec):
        pass
