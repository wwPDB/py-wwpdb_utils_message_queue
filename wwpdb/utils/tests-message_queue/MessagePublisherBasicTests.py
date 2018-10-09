#
# File: MessagePublisherBasicTests.py
# Date:  31-Aug-2016  J. Westbrook
#
# Updates:
#    9-Sep-2016 jdw refactor - use include connection class -
##
"""
Illustrative tests of message queue publisher methods.

This software was developed as part of the World Wide Protein Data Bank
Common Deposition and Annotation System Project

Copyright (c) wwPDB

This software is provided under a Creative Commons Attribution 3.0 Unported
License described at http://creativecommons.org/licenses/by/3.0/.

"""
from __future__ import division, absolute_import, print_function

__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Creative Commons Attribution 3.0 Unported"
__version__ = "V0.07"


import unittest
import time
import logging

from wwpdb.utils.message_queue.MessagePublisher import MessagePublisher
#

logging.basicConfig(level=logging.INFO, format='\n[%(levelname)s]-%(module)s.%(funcName)s: %(message)s')
logger = logging.getLogger()

from wwpdb.utils.testing.Features import Features

@unittest.skipUnless(Features().haveRbmqTestServer(), 'require Rbmq Test Environment')
class MessagePublisherBasicTests(unittest.TestCase):

    def setUp(self):
        self.__numMessages = 50

    def testPublishMessages(self):
        """  Publish numMessages messages to the test queue -
        """
        startTime = time.time()
        logger.debug("Starting")
        try:
            mp = MessagePublisher()
            #
            for ii in xrange(1, self.__numMessages + 1):
                message = "Test message %5d" % ii
                mp.publish(message, exchangeName="test_exchange", queueName="test_queue", routingKey="text_message")
            #
            #  Send a quit message to shutdown an associated test consumer -
            mp.publish("quit", exchangeName="test_exchange", queueName="test_queue", routingKey="text_message")
        except:
            logger.exception("Publish request failing")
            self.fail()

        endTime = time.time()
        logger.debug("Completed (%f seconds)" % (endTime - startTime))


def suitePublishRequest():
    suite = unittest.TestSuite()
    suite.addTest(MessagePublisherBasicTests('testPublishMessages'))
    #
    return suite

if __name__ == '__main__':
    runner = unittest.TextTestRunner(failfast=True)
    runner.run(suitePublishRequest())
