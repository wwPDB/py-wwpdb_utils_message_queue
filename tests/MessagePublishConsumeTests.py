#
# File: MessagePublishConsumeTests.py
# Date:  13-Aug-2019  E. PeisachJ. Westbrook
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

if __package__ is None or __package__ == "":
    from os import path

    sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
    from commonsetup import TESTOUTPUT  # pylint: disable=import-error,unused-import
else:
    from .commonsetup import TESTOUTPUT  # noqa: F401

from wwpdb.utils.message_queue.MessagePublisher import MessagePublisher
from wwpdb.utils.message_queue.MessageQueueConnection import MessageQueueConnection
from wwpdb.utils.testing.Features import Features

logging.basicConfig(level=logging.INFO, format="\n[%(levelname)s]-%(module)s.%(funcName)s: %(message)s")
logger = logging.getLogger()


@unittest.skipUnless((len(sys.argv) > 1 and sys.argv[1] == "--local") or Features().haveRbmqTestServer(), "require Rbmq Test Environment")
class MessagePublishConsumeBasicTests(unittest.TestCase):
    LOCAL = False

    def testPublishConsume(self):
        self.publishMessages()
        self.consumeMessages()

    def publishMessages(self):
        """Publish numMessages messages to the test queue -"""
        numMessages = 50
        startTime = time.time()
        logger.debug("Starting")
        try:
            mp = MessagePublisher(local=self.LOCAL)
            for ii in range(1, numMessages + 1):
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

    def consumeMessages(self):
        """Test case:  publish single text message basic authentication"""
        startTime = time.time()
        logger.debug("Starting")
        try:
            if self.LOCAL:
                connection = pika.BlockingConnection(pika.ConnectionParameters("localhost"))
            else:
                mqc = MessageQueueConnection()
                parameters = mqc._getConnectionParameters()  # pylint: disable=protected-access
                connection = pika.BlockingConnection(parameters)

            channel = connection.channel()

            channel.exchange_declare(exchange="test_exchange", exchange_type="topic", durable=True, auto_delete=False)

            result = channel.queue_declare(queue="test_queue", durable=True)
            channel.queue_bind(exchange="test_exchange", queue=result.method.queue, routing_key="text_message")

            channel.basic_consume(on_message_callback=messageHandler, queue=result.method.queue, consumer_tag="test_consumer_tag")

            channel.start_consuming()

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


def suitePublishConsumeRequest():
    suite = unittest.TestSuite()
    suite.addTest(MessagePublishConsumeBasicTests("testPublishConsume"))
    return suite


if __name__ == "__main__":
    parser = argparse.ArgumentParser(formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument("--local", action="store_true", help="run on local host")
    args = parser.parse_args()
    LOCAL = False
    if args.local:
        LOCAL = True
    MessagePublishConsumeBasicTests.LOCAL = LOCAL
    runner = unittest.TextTestRunner(failfast=True)
    runner.run(suitePublishConsumeRequest())
