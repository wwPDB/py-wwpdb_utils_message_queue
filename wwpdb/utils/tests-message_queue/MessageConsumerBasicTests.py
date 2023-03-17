#
# File: MessageConsumerBasicTests.py
# Date:  31-Aug-2016  J. Westbrook
#
# Updates:
##
"""
Illustrative tests for a basic message queue consumer.

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
import pika
import time
import logging
import argparse
import sys

if __package__ is None or __package__ == "":
    import sys
    from os import path

    sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
    from commonsetup import TESTOUTPUT  # pylint: disable=import-error,unused-import
else:
    from .commonsetup import TESTOUTPUT  # noqa: F401

from wwpdb.utils.message_queue.MessageQueueConnection import MessageQueueConnection
from wwpdb.utils.testing.Features import Features

#

logging.basicConfig(level=logging.INFO, format="\n[%(levelname)s]-%(module)s.%(funcName)s: %(message)s")
logger = logging.getLogger()


# This test needs to run from main - it blocks and must be tested by hand
inmain = True if __name__ == "__main__" else False


def messageHandler(channel, method, header, body):  # pylint: disable=unused-argument
    channel.basic_ack(delivery_tag=method.delivery_tag)

    if body == b"quit":
        channel.basic_cancel(consumer_tag="test_consumer_tag")
        channel.stop_consuming()
        logger.info("Message body %r -- done ", body)
    else:
        logger.info("Message body %r", body)
        time.sleep(1)
    #
    return


@unittest.skipUnless((len(sys.argv) > 1 and sys.argv[1] == '--local') or Features().haveRbmqTestServer() and inmain, "require Rbmq Test Environment and run from commandline")
class MessageConsumerBasicTests(unittest.TestCase):
    LOCAL = False

    def testConsumeBasic(self):
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

            channel = connection.channel()

            channel.exchange_declare(exchange="test_exchange", exchange_type="topic", durable=True, auto_delete=False)

            result = channel.queue_declare(queue="test_queue", durable=True)
            channel.queue_bind(exchange="test_exchange", queue=result.method.queue, routing_key="text_message")

            channel.basic_consume(on_message_callback=messageHandler, queue=result.method.queue, consumer_tag="test_consumer_tag")

            channel.start_consuming()

        except Exception:
            logger.exception("Basic consumer failing")

        endTime = time.time()
        logger.debug("Completed (%f seconds)", (endTime - startTime))

    def testConsumeSSL(self):
        """Test case:  publish single text message basic authentication"""
        startTime = time.time()
        logger.debug("Starting")
        try:
            if self.LOCAL:
                connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
            else:
                mqc = MessageQueueConnection()
                url = mqc._getSslConnectionUrl()  # pylint: disable=protected-access
                parameters = pika.URLParameters(url)
                connection = pika.BlockingConnection(parameters)

            channel = connection.channel()

            channel.exchange_declare(exchange="test_exchange", exchange_type="topic", durable=True, auto_delete=False)

            result = channel.queue_declare(queue="test_queue", durable=True)
            channel.queue_bind(exchange="test_exchange", queue=result.method.queue, routing_key="text_message")

            channel.basic_consume(on_message_callback=messageHandler, queue=result.method.queue, consumer_tag="test_consumer_tag")

            channel.start_consuming()

        except Exception:
            logger.exception("Basic consumer failing")

        endTime = time.time()
        logger.debug("Completed (%f seconds)", (endTime - startTime))


def suiteConsumeRequest():
    suite = unittest.TestSuite()
    suite.addTest(MessageConsumerBasicTests('testConsumeBasic'))
    # suite.addTest(MessageConsumerBasicTests("testConsumeSSL"))
    #
    return suite


if __name__ == "__main__":
    parser = argparse.ArgumentParser(formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument('--local', action='store_true', help='run on local host')
    args = parser.parse_args()
    LOCAL = False
    if args.local:
        LOCAL = True
    MessageConsumerBasicTests.LOCAL = LOCAL
    runner = unittest.TextTestRunner(failfast=True)
    runner.run(suiteConsumeRequest())
