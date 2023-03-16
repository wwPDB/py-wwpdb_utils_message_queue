#
# File: MessageQueueConnectionTests.py
# Date:  31-Aug-2016  J. Westbrook
#
# Updates:
#      8-Sep-2016  jdw parameterize the connection details
##
"""
Illustrative tests of message queue BASIC and SSL connection modes.

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

#
if __package__ is None or __package__ == "":
    import sys
    from os import path

    sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
    from commonsetup import TESTOUTPUT  # pylint: disable=unused-import,import-error
else:
    from .commonsetup import TESTOUTPUT  # noqa: F401
#
from wwpdb.utils.message_queue.MessageQueueConnection import MessageQueueConnection
from wwpdb.utils.testing.Features import Features

logging.basicConfig(level=logging.WARN, format="\n[%(levelname)s]-%(module)s.%(funcName)s: %(message)s")
logger = logging.getLogger()


@unittest.skipUnless(Features().haveRbmqTestServer(), "require Rbmq Test Environment")
class MessageQueueConnectionTests(unittest.TestCase):
    LOCAL = False

    def testPublishRequestAuthBasic(self):
        """Test case:  create connection with basic authenication and publish single text message."""
        startTime = time.time()
        logger.debug("Starting")

        try:
            if self.LOCAL:
                connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
            else:
                mqc = MessageQueueConnection()
                parameters = mqc._getConnectionParameters()  # pylint: disable=protected-access
                self.assertIsNotNone(parameters)
                connection = pika.BlockingConnection(parameters)

            self.assertIsNotNone(connection)

            channel = connection.channel()

            channel.exchange_declare(exchange="test_exchange", exchange_type="topic", passive=False, durable=True, auto_delete=False)

            result = channel.queue_declare(queue="test_queue", durable=True)
            channel.queue_bind(exchange="test_exchange", queue=result.method.queue, routing_key="text_message")
            message = "Test message"
            #
            channel.basic_publish(
                exchange="test_exchange",
                routing_key="text_message",
                body=message,
                properties=pika.BasicProperties(
                    delivery_mode=2,  # make message persistent
                ),
            )
            #
            connection.close()
        except Exception:
            logger.exception("Publish request failing")
            self.fail()

        endTime = time.time()
        logger.debug("Completed (%f seconds)", (endTime - startTime))

    @unittest.skip("Having issues with self signed SSL certificates on local host")
    def testPublishRequestAuthSSL(self):
        """Test case:  create SSL connection and publish a test message"""
        startTime = time.time()
        logger.debug("Starting")
        try:
            if self.LOCAL:
                connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
            else:
                mqc = MessageQueueConnection()
                parameters = mqc._getSslConnectionParameters()  # pylint: disable=protected-access
                self.assertIsNotNone(parameters)
                connection = pika.BlockingConnection(parameters)

            self.assertIsNotNone(connection)
            channel = connection.channel()
            channel.exchange_declare(exchange="test_exchange", exchange_type="topic", passive=False, durable=True, auto_delete=False)

            result = channel.queue_declare(queue="test_queue", durable=True)
            channel.queue_bind(exchange="test_exchange", queue=result.method.queue, routing_key="text_message")
            message = "Test message"

            #
            channel.basic_publish(
                exchange="",
                routing_key="test_queue",
                body=message,
                properties=pika.BasicProperties(
                    delivery_mode=2,  # make message persistent
                ),
            )
            #
            connection.close()
        except Exception:
            logger.exception("Publish request failing")
            self.fail()

        endTime = time.time()
        logger.debug("Completed (%f seconds)", (endTime - startTime))


def suitePublishRequest():
    suite = unittest.TestSuite()
    suite.addTest(MessageQueueConnectionTests("testPublishRequestAuthBasic"))
    # suite.addTest(MessageQueueConnectionTests("testPublishRequestAuthSSL"))
    #
    return suite


if __name__ == "__main__":
    parser = argparse.ArgumentParser(formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument('-l', '--local', action='store_true', help='run on local host')
    args = parser.parse_args()
    LOCAL = False
    if args.local:
        LOCAL = True
    MessageQueueConnectionTests.LOCAL = LOCAL
    runner = unittest.TextTestRunner(failfast=True)
    runner.run(suitePublishRequest())
