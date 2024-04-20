

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


def get_remote_bucket_list(remote_uri):
    # TODO This causes issue with the asyncio loop because
    #  asyncio.run() causes a loop to start and stop, and this
    #  breaks things when something else wants a loop.
    #  We aren't using this in any case because it uses up M2M
    #  access tokens for every component.
    #  This should be reconsidered seriously before multiple
    #  file management servers can be supported,
    #
    # async with async_client(base_url=remote_uri, auth=_authenticator) as client:
    #     response = await client.get('/v1/filestore/buckets')
    #     return response.json()["available_buckets"]
    return['incoming', 'cdn', 'outgoing']


class FilestoreBucketRemote(FilestoreBucketBase):
    def _async_http_client_args(self):
        return {
            'base_url': self.uri,
            'auth': _authenticator
        }

    @with_async_client_cl()
    async def upload(self, file, actual_user=None, interest=None, label=None, overwrite=False, client=None):
        params = {}
        if actual_user:
            params['actual_user'] = actual_user
        if interest:
            params['interest'] = interest
        if label:
            params['label'] = label
        response = await client.post(f'/v1/filestore/{self.name}/upload',
                                     files={'file': file}, params=params)
        response.raise_for_status()
        return response.json()

    @with_async_client_cl()
    async def move(self, filename, target_bucket, actual_user=None, overwrite=False, client=None):
        params = {}
        if actual_user:
            params['actual_user'] = actual_user
        data = {"to_bucket": target_bucket,
                "filename": filename,
                "overwrite": overwrite}
        response = await client.post(f'/v1/filestore/{self.name}/move',
                                     json=data, params=params)
        response.raise_for_status()
        return response.json()

    @with_async_client_cl()
    async def list(self, client=None):
        response = await client.get(f'/v1/filestore/{self.name}/ls_fs')
        response.raise_for_status()
        return response.json()

    @with_async_client_cl()
    async def list_info(self, include_owner=False, filenames=None, client=None):
        response = await client.get(f'/v1/filestore/{self.name}/ls',
                                    params={'include_owner': include_owner})
        response.raise_for_status()
        return response.json()['items']

    @with_async_client_cl()
    async def find(self, spec, client=None):
        raise NotImplementedError

    @with_async_client_cl()
    async def delete(self, filename, user, client=None):
        raise NotImplementedError

    @with_async_client_cl()
    async def prune(self, user, client=None):
        raise NotImplementedError

    @with_async_client_cl()
    async def purge(self, user, client=None):
        raise NotImplementedError
