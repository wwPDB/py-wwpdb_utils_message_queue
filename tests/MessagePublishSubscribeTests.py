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


import argparse
import logging
import sys
import time
import unittest
from typing import Optional

import pika
from pika import spec
from pika.adapters.blocking_connection import BlockingChannel

if __package__ is None or __package__ == "":
    from os import path

    sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))

from wwpdb.utils.testing.Features import Features

from wwpdb.utils.message_queue.MessagePublisher import MessagePublisher
from wwpdb.utils.message_queue.MessageQueueConnection import MessageQueueConnection

logging.basicConfig(level=logging.INFO, format="\n[%(levelname)s]-%(module)s.%(funcName)s: %(message)s")
logger = logging.getLogger()


@unittest.skipUnless(
    (len(sys.argv) > 1 and sys.argv[1] == "--local") or Features().haveRbmqTestServer(), "require Rbmq Test Environment"
)
class MessagePublishSubscribeBasicTests(unittest.TestCase):
    LOCAL = False

    def setUp(self) -> None:
        self.__exchange_name = "test_subscriber_exchange"
        self.__exchange_type = "direct"
        self.__routing_key = "subscriber_routing_key"
        self.__channel: Optional[BlockingChannel] = None
        self.__queue_name: Optional[str] = None

    def testPublishSubscribe(self) -> None:
        self.initialize()
        self.publishMessages()
        self.consumeMessages()

    def initialize(self) -> None:
        """Test case:  publish single text message basic authentication"""

        startTime = time.time()
        logger.debug("Starting")

        try:
            if self.LOCAL:
                connection = pika.BlockingConnection(pika.ConnectionParameters("localhost"))
            else:
                mqc = MessageQueueConnection()
                parameters = mqc._getConnectionParameters()  # noqa: SLF001 pylint: disable=protected-access
                connection = pika.BlockingConnection(parameters)

            self.__channel = connection.channel()

            self.__channel.exchange_declare(
                exchange=self.__exchange_name, exchange_type=self.__exchange_type, durable=True, auto_delete=False
            )

            result = self.__channel.queue_declare(queue="", exclusive=True, durable=True)
            self.__queue_name = result.method.queue
            if self.__queue_name is None:
                self.fail("Broker did not assign a queue name")

            self.__channel.queue_bind(
                exchange=self.__exchange_name, queue=self.__queue_name, routing_key=self.__routing_key
            )

        except Exception:
            logger.exception("Basic consumer failing")
            self.fail()

        endTime = time.time()
        logger.debug("Completed (%f seconds)", (endTime - startTime))

    def publishMessages(self) -> None:
        """Publish numMessages messages to the test queue -"""
        numMessages = 10
        startTime = time.time()
        logger.debug("Starting")
        try:
            mp = MessagePublisher(local=self.LOCAL)
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

    def consumeMessages(self) -> None:
        """Test case:  publish single text message basic authentication"""
        startTime = time.time()
        logger.debug("Starting")
        try:
            if self.__channel is None or self.__queue_name is None:
                self.fail("Channel not initialized")
            self.__channel.basic_consume(
                on_message_callback=messageHandler, queue=self.__queue_name, consumer_tag="test_consumer_tag"
            )

            self.__channel.start_consuming()

        except Exception:
            logger.exception("Basic consumer failing")
            self.fail()

        endTime = time.time()
        logger.debug("Completed (%f seconds)", (endTime - startTime))


def messageHandler(
    channel: BlockingChannel,
    method: spec.Basic.Deliver,
    header: spec.BasicProperties,  # pylint: disable=unused-argument
    body: bytes,
) -> None:
    channel.basic_ack(delivery_tag=method.delivery_tag)  # type: ignore[arg-type]  # always set on a delivered message

    if body == b"quit":
        channel.basic_cancel(consumer_tag="test_consumer_tag")
        channel.stop_consuming()
        logger.info("Message body %r -- done ", body)
    else:
        logger.info("Message body %r", body)
        time.sleep(0.25)


def suitePublishSubscribeRequest() -> unittest.TestSuite:
    suite = unittest.TestSuite()
    suite.addTest(MessagePublishSubscribeBasicTests("testPublishSubscribe"))
    return suite


if __name__ == "__main__":
    parser = argparse.ArgumentParser(formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument("--local", action="store_true", help="run on local host")
    args = parser.parse_args()
    LOCAL = False
    if args.local:
        LOCAL = True
    MessagePublishSubscribeBasicTests.LOCAL = LOCAL
    runner = unittest.TextTestRunner(failfast=True)
    runner.run(suitePublishSubscribeRequest())
