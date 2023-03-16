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
import argparse

if __package__ is None or __package__ == "":
    import sys
    from os import path

    sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
    from commonsetup import TESTOUTPUT  # pylint: disable=import-error,unused-import
else:
    from .commonsetup import TESTOUTPUT  # noqa: F401

from wwpdb.utils.message_queue.MessagePublisher import MessagePublisher
from wwpdb.utils.testing.Features import Features

#

logging.basicConfig(level=logging.INFO, format="\n[%(levelname)s]-%(module)s.%(funcName)s: %(message)s")
logger = logging.getLogger()


# This test could be run from main - it will load up a queue
inmain = True if __name__ == "__main__" else False


@unittest.skipUnless(Features().haveRbmqTestServer() and inmain, "require Rbmq Test Environment and started from command line")
class MessagePublisherBasicTests(unittest.TestCase):
    LOCAL = False

    def setUp(self):
        self.__numMessages = 50

    def testPublishMessages(self):
        """Publish numMessages messages to the test queue -"""
        startTime = time.time()
        logger.debug("Starting")
        try:
            mp = MessagePublisher(local=self.LOCAL)
            #
            for ii in range(1, self.__numMessages + 1):
                message = "Test message %5d" % ii
                mp.publish(message, exchangeName="test_exchange", queueName="test_queue", routingKey="text_message")
            #
            #  Send a quit message to shutdown an associated test consumer -
            mp.publish("quit", exchangeName="test_exchange", queueName="test_queue", routingKey="text_message")
        except Exception:
            logger.exception("Publish request failing")
            self.fail()

        endTime = time.time()
        logger.debug("Completed (%f seconds)", (endTime - startTime))


def suitePublishRequest():
    suite = unittest.TestSuite()
    suite.addTest(MessagePublisherBasicTests("testPublishMessages"))
    #
    return suite


if __name__ == "__main__":
    parser = argparse.ArgumentParser(formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument('-l', '--local', action='store_true', help='run on local host')
    args = parser.parse_args()
    LOCAL = False
    if args.local:
        LOCAL = True
    MessagePublisherBasicTests.LOCAL = LOCAL
    runner = unittest.TextTestRunner(failfast=True)
    runner.run(suitePublishRequest())
