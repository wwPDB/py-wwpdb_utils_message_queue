# Common setup for tests as unittest runner loads files in any order

import os
import platform

HERE = os.path.abspath(os.path.dirname(__file__))
TOPDIR = os.path.dirname(HERE)

TESTOUTPUT = os.path.join(HERE, "test-output", platform.python_version())
if not os.path.exists(TESTOUTPUT):
    os.makedirs(TESTOUTPUT)
mockTopPath = os.path.join(TOPDIR, "wwpdb", "mock-data")

# Must create config file before importing ConfigInfo
from wwpdb.utils.testing.SiteConfigSetup import SiteConfigSetup  # noqa: E402

mockTopPath = os.path.join(TOPDIR, "wwpdb", "mock-data")
SiteConfigSetup().setupEnvironment(TESTOUTPUT, mockTopPath)

# from wwpdb.utils.config.ConfigInfo import ConfigInfo, getSiteId


class commonsetup:
    def __init__(self):
        pass
