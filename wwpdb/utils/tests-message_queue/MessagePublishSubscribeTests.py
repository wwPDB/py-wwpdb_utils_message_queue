#
# File: MessagePublishSubscribeTests.py
# Date:  Feb-2023  James Smith
#
# Updates:
#
##
"""
Test of direct exchange without using MessageSubscriber class.
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
import pika
import argparse
import sys

if __package__ is None or __package__ == "":
    import sys
    from os import path
    sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))

from wwpdb.utils.message_queue.MessagePublisher import MessagePublisher
from wwpdb.utils.message_queue.MessageQueueConnection import MessageQueueConnection
from wwpdb.utils.testing.Features import Features

#
logging.basicConfig(level=logging.INFO, format="\n[%(levelname)s]-%(module)s.%(funcName)s: %(message)s")
logger = logging.getLogger()


@unittest.skipUnless((len(sys.argv) > 1 and sys.argv[1] == '--local') or Features().haveRbmqTestServer(), "require Rbmq Test Environment")
class MessagePublishSubscribeBasicTests(unittest.TestCase):
    LOCAL = False

    def setUp(self):
        self.__exchange_name = 'test_subscriber_exchange'
        self.__exchange_type = 'direct'
        self.__routing_key = 'subscriber_routing_key'
        self.__channel = None
        self.__queue_name = None
        
    def testPublishSubscribe(self):
        self.initialize()
        self.publishMessages()
        self.consumeMessages()

    def initialize(self):
        """Test case:  publish single text message basic authentication"""

        startTime = time.time()
        logger.debug("Starting")

        try:
            if self.LOCAL:
                connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
            else:
                mqc = MessageQueueConnection()
                parameters = mqc._getConnectionParameters()  # pylint: disable=protected-access
                connection = pika.BlockingConnection(parameters)

            self.__channel = connection.channel()

            self.__channel.exchange_declare(exchange=self.__exchange_name, exchange_type=self.__exchange_type, durable=True, auto_delete=False)

            result = self.__channel.queue_declare(queue='', exclusive=True, durable=True, arguments={'x-max-priority': 10})
            self.__queue_name = result.method.queue

            self.__channel.queue_bind(exchange=self.__exchange_name, queue=self.__queue_name, routing_key=self.__routing_key)

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
                mp.publishDirect(message, exchangeName=self.__exchange_name, priority=ii)
            #
            #  Send a quit message to shutdown an associated test consumer -
            mp.publishDirect("quit", exchangeName=self.__exchange_name, priority=1)
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

            self.__channel.basic_consume(on_message_callback=messageHandler, queue=self.__queue_name, consumer_tag="test_consumer_tag")

            self.__channel.start_consuming()

        except Exception:
            logger.exception("Basic consumer failing")
            self.fail()

        endTime = time.time()
        logger.debug("Completed (%f seconds)", (endTime - startTime))


def messageHandler(channel, method, header, body):  # pylint: disable=unused-argument
    channel.basic_ack(delivery_tag=method.delivery_tag)

    if body == b"quit":
        channel.basic_cancel(consumer_tag="test_consumer_tag")
        channel.stop_consuming()
        logger.info("Message body %r -- done ", body)
    else:
        logger.info("Message body %r", body)
        time.sleep(0.25)
    #
    return


def suitePublishSubscribeRequest():
    suite = unittest.TestSuite()
    suite.addTest(MessagePublishSubscribeBasicTests("testPublishSubscribe"))
    #
    return suite


if __name__ == "__main__":
    parser = argparse.ArgumentParser(formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument('--local', action='store_true', help='run on local host')
    args = parser.parse_args()
    LOCAL = False
    if args.local:
        LOCAL = True
    MessagePublishSubscribeBasicTests.LOCAL = LOCAL
    runner = unittest.TextTestRunner(failfast=True)
    runner.run(suitePublishSubscribeRequest())
