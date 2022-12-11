# Copyright (C) 2019 Chintalagiri Shashank
#
# This file is part of Tendril.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
"""
Core Filestore Configuration Options
====================================
"""


from tendril.utils.config import ConfigOption
from tendril.utils import log
logger = log.get_logger(__name__, log.DEFAULT)

depends = ['tendril.config.core']

config_elements_filestore_core = [
    ConfigOption(
        'FILESTORE_BUCKETS',
        "[]",
        "List of names of filestore buckets to be created."
    ),
    ConfigOption(
        'FILESTORE_ENABLED',
        "False",
        "Whether the filestore is enabled in this instance / component. Generally, "
        "multi-component Tendril deployments will have the filestore enabled only on "
        "a single component, and the Filestore API on that component is to be used by "
        "other components to manipulate the singular filestore. For such deployments, "
        "this parameter is generally best set through environment variables. In instances "
        "where only some buckets should be enabled on a component, set this to True and "
        "individual bucket enabled config parameters to False for the other buckets."
    ),
    ConfigOption(
        'FILESTORE_REMOTE_URI',
        "None",
        "Location of the actual filestore component, as a network URL. When set, this "
        "option will be used in components where FILESTORE_ENABLED is False to provide "
        "filestore functionality by proxying to the remote."
    ),
    ConfigOption(
        'FILESTORE_ACTUAL',
        "os.path.join(INSTANCE_ROOT, 'filestore')",
        "Default path to create filestore folders at. This may "
        "be overridden by individual filestore bucket configurations."
    )
]


def load(manager):
    logger.debug("Loading {0}".format(__name__))
    manager.load_elements(config_elements_filestore_core,
                          doc="Tendril Filestore Core Configuration")
