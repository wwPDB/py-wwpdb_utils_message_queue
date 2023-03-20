#
# File: MessagePublishSubscribeTests.py
# Date:  Feb-2023  James Smith
#
# Updates:
#
##
"""
Test of direct exchange using MessageSubscriber class.
Subscriber variation on previous publish consume tests.
"""

__docformat__ = "restructuredtext en"
__author__ = "James Smith"
__email__ = "james.smith@rcsb.org"
__license__ = "Creative Commons Attribution 3.0 Unported"
__version__ = "V0.07"

import unittest
import time
import logging
import argparse
import sys

if __package__ is None or __package__ == "":
    from os import path

    sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))

from wwpdb.utils.message_queue.MessagePublisher import MessagePublisher
from wwpdb.utils.message_queue.MessageQueueConnection import MessageQueueConnection
from wwpdb.utils.message_queue.MessageSubscriberBase import MessageSubscriberBase
from wwpdb.utils.testing.Features import Features

#
logging.basicConfig(level=logging.INFO, format="\n[%(levelname)s]-%(module)s.%(funcName)s: %(message)s")
logger = logging.getLogger()


class MessageSubscriber(MessageSubscriberBase):
    def __init__(self, url, local=False):
        super().__init__(url, local=local)

    def workerMethod(self, msgBody, deliveryTag=None):
        logger.info("Message body %r", msgBody)
        return True


@unittest.skipUnless((len(sys.argv) > 1 and sys.argv[1] == '--local') or Features().haveRbmqTestServer(), "require Rbmq Test Environment")
class MessageSubscriberTests(unittest.TestCase):
    LOCAL = False

    def testPublishSubscribe(self):
        self.initialize()
        self.publishMessages()
        self.consumeMessages()

    def initialize(self):
        """Test case:  publish single text message basic authentication"""
        self.__exchange_name = 'test_subscriber_exchange'
        self.__exchange_type = 'direct'
        self.__routing_key = 'subscriber_routing_key'

        startTime = time.time()
        logger.debug("Starting")

        try:
            if self.LOCAL:
                url = None
            else:
                mqc = MessageQueueConnection()
                url = mqc._getDefaultConnectionUrl()

            self.__subscriber = MessageSubscriber(url, local=self.LOCAL)
            self.__subscriber.add_exchange(self.__exchange_name)

        except Exception:
            logger.exception("Basic consumer failing")
            self.fail()

        endTime = time.time()
        logger.debug("Completed (%f seconds)", (endTime - startTime))

    def publishMessages(self):
        """Publish numMessages messages to the test queue -"""
        numMessages = 10
        startTime = time.time()
        logger.debug("Starting")
        try:
            mp = MessagePublisher(local=self.LOCAL)
            #
            for ii in range(1, numMessages + 1):
                message = "Test message %5d" % ii
                mp.publishDirect(message, exchangeName=self.__exchange_name)
            #
            #  Send a quit message to shutdown an associated test consumer -
            mp.publishDirect("quit", exchangeName=self.__exchange_name)
        except Exception:
            logger.exception("Publish request failing")
            self.fail()

        endTime = time.time()
        logger.debug("Completed (%f seconds)", (endTime - startTime))

    def consumeMessages(self):
        """Test case:  publish single text message basic authentication"""
        startTime = time.time()
        logger.debug("Starting")

        try:
            self.__subscriber.run()
        except Exception:
            logger.exception("Basic Subscriber failing")
            self.fail()

        endTime = time.time()
        logger.debug("Completed (%f seconds)", (endTime - startTime))


def suitePublishSubscribeRequest():
    suite = unittest.TestSuite()
    suite.addTest(MessageSubscriberTests("testPublishSubscribe"))
    #
    return suite


if __name__ == "__main__":
    parser = argparse.ArgumentParser(formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument('--local', action='store_true', help='run on local host')
    args = parser.parse_args()
    LOCAL = False
    if args.local:
        LOCAL = True
    MessageSubscriberTests.LOCAL = LOCAL
    runner = unittest.TextTestRunner(failfast=True)
    runner.run(suitePublishSubscribeRequest())
