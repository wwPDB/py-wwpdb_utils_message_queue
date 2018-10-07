##
# File: ImportTests.py
# Date:  06-Oct-2018  E. Peisach
#
# Updates:
##
"""Test cases for message_queue - simply import everything to ensure imports work
"""

__docformat__ = "restructuredtext en"
__author__ = "Ezra Peisach"
__email__ = "peisach@rcsb.rutgers.edu"
__license__ = "Creative Commons Attribution 3.0 Unported"
__version__ = "V0.01"

import unittest

import wwpdb.utils.message_queue.DetachedMessageConsumerExample
import wwpdb.utils.message_queue.MessageConsumerBase
import wwpdb.utils.message_queue.MessagePublisher
import wwpdb.utils.message_queue.MessageQueueConnection

class ImportTests(unittest.TestCase):
    def setUp(self):
        pass

    def testPass(self):
        pass

    
