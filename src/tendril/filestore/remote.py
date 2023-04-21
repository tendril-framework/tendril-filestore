

from tendril.authn.client import IntramuralAuthenticator
from tendril.utils.www import async_client
from tendril.utils.www import with_async_client_cl

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
    # return['incoming', 'cdn', 'outgoing']


class FilestoreBucketRemote(FilestoreBucketBase):
    def _async_http_client_args(self):
        return {
            'base_url': self.uri,
            'auth': _authenticator
        }

    async def upload(self, file, user, overwrite=False):
        raise NotImplementedError

    async def move(self, filename, target_bucket, user, overwrite=False):
        raise NotImplementedError

    async def delete(self, filename, user):
        raise NotImplementedError

    async def list(self):
        raise NotImplementedError

    @with_async_client_cl()
    async def list_info(self, include_owner=False, filenames=None, client=None):
        response = await client.post(f'/v1/filestore/{self.name}/ls',
                                     params={'include_owner': include_owner})
        return response.json()

    async def purge(self, user):
        raise NotImplementedError

    async def prune(self, user):
        raise NotImplementedError

    async def find(self, spec):
        raise NotImplementedError
