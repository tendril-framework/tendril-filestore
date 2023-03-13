

from tendril.config import FILESTORE_ENABLED


if FILESTORE_ENABLED:
    scopes = {
        'file_management:common': "Standard file management API access",
        'file_management:admin': "Administrative file management API access"
    }
else:
    scopes = {}
