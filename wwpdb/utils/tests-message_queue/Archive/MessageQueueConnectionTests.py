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
#
from wwpdb.utils.message_queue.MessageQueueConnection import MessageQueueConnection

logging.basicConfig(level=logging.WARN, format='\n[%(levelname)s]-%(module)s.%(funcName)s: %(message)s')
logger = logging.getLogger()


class MessageQueueConnectionTests(unittest.TestCase):

    def setUp(self):
        pass

    def testPublishRequestAuthBasic(self):
        """  Test case:  create connection with basic authenication and publish single text message.
        """
        startTime = time.time()
        logger.debug("Starting")
        try:
            #
            mqc = MessageQueueConnection()
            parameters = mqc._getConnectionParameters()
            connection = pika.BlockingConnection(parameters)
            channel = connection.channel()

            channel.exchange_declare(exchange="test_exchange",
                                     type="topic",
                                     passive=False,
                                     durable=True,
                                     auto_delete=False)

            result = channel.queue_declare(queue='test_queue',
                                           durable=True)
            channel.queue_bind(exchange='test_exchange',
                               queue=result.method.queue,
                               routing_key='text_message')
            message = "Test message"
            #
            channel.basic_publish(exchange='test_exchange',
                                  routing_key='text_message',
                                  body=message,
                                  properties=pika.BasicProperties(
                                      delivery_mode=2,  # make message persistent
                                  ))
            #
            connection.close()
        except:
            logger.exception("Publish request failing")
            self.fail()

        endTime = time.time()
        logger.debug("Completed (%f seconds)" % (endTime - startTime))

    def testPublishRequestAuthSSL(self):
        """  Test case:  create SSL connection and publish a test message
        """
        startTime = time.time()
        logger.debug("Starting")
        try:
            mqc = MessageQueueConnection()
            parameters = mqc._getSslConnectionParameters()
            connection = pika.BlockingConnection(parameters)
            channel = connection.channel()
            channel.exchange_declare(exchange="test_exchange",
                                     type="topic",
                                     passive=False,
                                     durable=True,
                                     auto_delete=False)

            result = channel.queue_declare(queue='test_queue',
                                           durable=True)
            channel.queue_bind(exchange='test_exchange',
                               queue=result.method.queue,
                               routing_key='text_message')
            message = "Test message"

            #
            channel.basic_publish(exchange='',
                                  routing_key='test_queue',
                                  body=message,
                                  properties=pika.BasicProperties(
                                      delivery_mode=2,  # make message persistent
                                  ))
            #
            connection.close()
        except:
            logger.exception("Publish request failing")
            self.fail()

        endTime = time.time()
        logger.debug("Completed (%f seconds)" % (endTime - startTime))


def suitePublishRequest():
    suite = unittest.TestSuite()
    suite.addTest(MessageQueueConnectionTests('testPublishRequestAuthBasic'))
    # suite.addTest(MessageQueueConnectionTests('testPublishRequestAuthSSL'))
    #
    return suite

if __name__ == '__main__':
    runner = unittest.TextTestRunner(failfast=True)
    runner.run(suitePublishRequest())
