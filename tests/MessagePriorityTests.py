#
# File: MessagePriorityTests.py
# Author: James Smith
#
# Updates:
#
##
"""
A series of tests for automatic integration testing in which a published and consumer are created and consume messages.
"""

__docformat__ = "restructuredtext en"
__author__ = "Ezra Peisach"
__email__ = "peisach@rcsb.rutgers.edu"
__license__ = "Creative Commons Attribution 3.0 Unported"
__version__ = "V0.07"


import argparse
import logging
import sys
import time
import unittest

import pika
from pika import spec
from pika.adapters.blocking_connection import BlockingChannel

if __package__ is None or __package__ == "":
    from os import path

    sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
    from commonsetup import TESTOUTPUT  # type: ignore[import-not-found] # pylint: disable=import-error,unused-import
else:
    from .commonsetup import TESTOUTPUT  # noqa: F401,TID252

from wwpdb.utils.testing.Features import Features

from wwpdb.utils.message_queue.MessagePublisher import MessagePublisher
from wwpdb.utils.message_queue.MessageQueueConnection import MessageQueueConnection

logging.basicConfig(level=logging.INFO, format="\n[%(levelname)s]-%(module)s.%(funcName)s: %(message)s")
logger = logging.getLogger()


@unittest.skipUnless(
    (len(sys.argv) > 1 and sys.argv[1] == "--local") or Features().haveRbmqTestServer(), "require Rbmq Test Environment"
)
class MessagePriorityTests(unittest.TestCase):
    LOCAL = False
    exchangeName = "test_priority_exchange"
    queueName = "test_priority_queue"
    routingKey = "text_message"
    exchangeType = "topic"
    consumerTag = "test_consumer_tag"

    def setup(self) -> None:
        if self.LOCAL:
            connection = pika.BlockingConnection(pika.ConnectionParameters("localhost"))
        else:
            mqc = MessageQueueConnection()
            parameters = mqc._getDefaultConnectionParameters()  # noqa: SLF001 pylint: disable=protected-access
            connection = pika.BlockingConnection(parameters)
        channel = connection.channel()
        channel.queue_delete(queue=self.queueName)
        channel.exchange_delete(exchange=self.exchangeName)

    def teardown(self) -> None:
        pass

    def testPublishConsume(self) -> None:
        self.publishMessages()
        self.consumeMessages()

    def publishMessages(self) -> None:
        """Publish numMessages messages to the test queue -"""
        numMessages = 10
        startTime = time.time()
        logger.debug("Starting")
        try:
            mp = MessagePublisher(local=self.LOCAL)
            for ii in range(1, numMessages + 1):
                message = "Test message %5d" % ii
                mp.publish(
                    message,
                    exchangeName=self.exchangeName,
                    queueName=self.queueName,
                    routingKey=self.routingKey,
                    priority=ii,
                )
            #
            #  Send a quit message to shutdown an associated test consumer -
            mp.publish(
                "quit", exchangeName=self.exchangeName, queueName=self.queueName, routingKey=self.routingKey, priority=1
            )

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
            if self.LOCAL:
                connection = pika.BlockingConnection(pika.ConnectionParameters("localhost"))
            else:
                mqc = MessageQueueConnection()
                parameters = mqc._getConnectionParameters()  # noqa: SLF001 pylint: disable=protected-access
                connection = pika.BlockingConnection(parameters)

            channel = connection.channel()

            channel.exchange_declare(
                exchange=self.exchangeName, exchange_type=self.exchangeType, durable=True, auto_delete=False
            )

            result = channel.queue_declare(queue=self.queueName, durable=True, arguments={"x-max-priority": 10})  # type: ignore[arg-type]  # pika-stubs mistypes arguments
            channel.queue_bind(exchange=self.exchangeName, queue=result.method.queue, routing_key=self.routingKey)  # type: ignore[arg-type]  # set by the broker

            channel.basic_consume(
                on_message_callback=messageHandler, queue=result.method.queue, consumer_tag=self.consumerTag
            )

            channel.start_consuming()

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
        channel.basic_cancel(consumer_tag=MessagePriorityTests.consumerTag)
        channel.stop_consuming()
        logger.info("Message body %r -- done ", body)
    else:
        logger.info("Message body %r", body)
        time.sleep(0.25)


def suitePublishConsumeRequest() -> unittest.TestSuite:
    suite = unittest.TestSuite()
    suite.addTest(MessagePriorityTests("testPublishConsume"))
    return suite


if __name__ == "__main__":
    parser = argparse.ArgumentParser(formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument("--local", action="store_true", help="run on local host")
    args = parser.parse_args()
    LOCAL = False
    if args.local:
        LOCAL = True
    MessagePriorityTests.LOCAL = LOCAL
    runner = unittest.TextTestRunner(failfast=True)
    runner.run(suitePublishConsumeRequest())
