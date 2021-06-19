##
# File: ImportTests.py
# Date:  06-Oct-2018  E. Peisach
#
# Updates:
##
"""Test cases for message_queue - simply import everything to ensure imports work
"""

#  pylint: disable=unused-import

__docformat__ = "restructuredtext en"
__author__ = "Ezra Peisach"
__email__ = "peisach@rcsb.rutgers.edu"
__license__ = "Creative Commons Attribution 3.0 Unported"
__version__ = "V0.01"

import unittest

if __package__ is None or __package__ == "":
    import sys
    from os import path

    sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
    from commonsetup import TESTOUTPUT  # pylint: disable=import-error
else:
    from .commonsetup import TESTOUTPUT  # noqa: F401

import wwpdb.utils.message_queue.DetachedMessageConsumerExample
import wwpdb.utils.message_queue.MessageConsumerBase
import wwpdb.utils.message_queue.MessagePublisher
import wwpdb.utils.message_queue.MessageQueueConnection  # noqa: F401


class ImportTests(unittest.TestCase):
    def setUp(self):
        pass

    def testPass(self):
        pass
