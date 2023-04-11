import os.path

from tendril.utils.config import ConfigOption
from tendril.utils.config import ConfigOptionConstruct
from tendril.utils import log
logger = log.get_logger(__name__, log.DEFAULT)

depends = ['tendril.config.core',
           'tendril.config.filestore_core']


class FileStoreActualURI(ConfigOptionConstruct):
    @property
    def value(self):
        bucket_uri = self.ctx['FILESTORE_{}_ACTUAL'.format(self._parameters)]
        if not bucket_uri:
            bucket_uri = os.path.join(self.ctx['FILESTORE_ACTUAL'], self._parameters)
        parts = bucket_uri.split('://')
        if len(parts) == 1:
            scheme = "osfs"
            path = bucket_uri
        else:
            scheme = parts[0]
            path = parts[1]
        if "osfs" == scheme and not (path.startswith('/') or path.startswith('~')):
            path = os.path.join(self.ctx['INSTANCE_ROOT'], path)
        bucket_uri = "{}://{}".format(scheme, path)
        return bucket_uri


def _filestore_config_template(filestore_name):
    return [
        ConfigOption(
            'FILESTORE_{}_ENABLED'.format(filestore_name),
            "True",
            "Whether this filestore bucket is enabled in this instance / component."
        ),
        ConfigOption(
            'FILESTORE_{}_ACCEPT_EXT'.format(filestore_name),
            "[]",
            "List of file extensions this filestore bucket should accept through the "
            "filestore API. Tendril internals can still move files into this file store "
            "irrespective of this setting."
        ),
        ConfigOption(
            'FILESTORE_{}_ALLOW_DELETE'.format(filestore_name),
            "False",
            "Whether the filestore API should allow deletion of files from this bucket. "
            "Users can still delete files owned by them, and Tendril internals can still "
            "delete files from this file store irrespective of this setting."
        ),
        ConfigOption(
            'FILESTORE_{}_ALLOW_OVERWRITE'.format(filestore_name),
            "False",
            "Whether the filestore API should allow overwwriting of files in this bucket. "
            "Users can still overwrite files owned by them, and Tendril internals can "
            "still overwrite files in this file store irrespective of this setting."
        ),
        ConfigOption(
            'FILESTORE_{}_EXPOSE_URI'.format(filestore_name),
            "None",
            "URI at which the filestore content is exposed. Tendril filestore code will "
            "simply construct URIs for individual files using string operations. The "
            "URI may be public, private or entirely invalid depending on the bucket and "
            "ingress configurations, outside of Tendril."
        ),
        ConfigOption(
            'FILESTORE_{}_ACTUAL'.format(filestore_name),
            "None",
            "Path to store this filestore bucket. If not provided, it will be placed within "
            "the default FILESTORE_ACTUAL path. This should either be a local file path or "
            "a pyfilesystems2 supported URI."
        ),
        FileStoreActualURI(
            'FILESTORE_{}_ACTUAL_URI'.format(filestore_name),
            filestore_name,
            "Constructed Filestore Actual URI string. This option is created by "
            "the code, and should not be set directly in any config file."
        ),
    ]


def load(manager):
    logger.debug("Loading {0}".format(__name__))
    config_elements_filestore = []
    for code in manager.FILESTORE_BUCKETS:
        config_elements_filestore += _filestore_config_template(code.upper())
    manager.load_elements(config_elements_filestore,
                          doc="Filestore Configuration")
