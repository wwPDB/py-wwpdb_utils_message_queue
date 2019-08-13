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
from __future__ import division, absolute_import, print_function

__docformat__ = "restructuredtext en"
__author__ = "Ezra Peisach"
__email__ = "peisach@rcsb.rutgers.edu"
__license__ = "Creative Commons Attribution 3.0 Unported"
__version__ = "V0.07"


import unittest
import time
import logging
import pika

if __package__ is None or __package__ == '':
    import sys
    from os import path

    sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
    from commonsetup import TESTOUTPUT
else:
    from .commonsetup import TESTOUTPUT

from wwpdb.utils.message_queue.MessagePublisher import MessagePublisher
from wwpdb.utils.message_queue.MessageQueueConnection import MessageQueueConnection
#

logging.basicConfig(level=logging.INFO, format='\n[%(levelname)s]-%(module)s.%(funcName)s: %(message)s')
logger = logging.getLogger()

from wwpdb.utils.testing.Features import Features

@unittest.skipUnless(Features().haveRbmqTestServer(), 'require Rbmq Test Environment')
class MessagePublishConsumeBasicTests(unittest.TestCase):

    def testPublishConsume(self):
        self.publishMessages()
        self.consumeMessages()
        
    def publishMessages(self):
        """  Publish numMessages messages to the test queue -
        """
        self.__numMessages = 50
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


    def consumeMessages(self):
        """  Test case:  publish single text message basic authentication
        """
        self.__messageCount = 0

        startTime = time.time()
        logger.debug("Starting")
        try:
            self.__messageCount = 0
            mqc = MessageQueueConnection()
            parameters = mqc._getConnectionParameters()

            connection = pika.BlockingConnection(parameters)
            channel = connection.channel()

            channel.exchange_declare(exchange="test_exchange",
                                     exchange_type="topic",
                                     durable=True,
                                     auto_delete=False)

            result = channel.queue_declare(queue='test_queue',
                                           durable=True)
            channel.queue_bind(exchange='test_exchange',
                               queue=result.method.queue,
                               routing_key='text_message')

            channel.basic_consume(messageHandler,
                                  queue=result.method.queue,
                                  consumer_tag="test_consumer_tag")

            channel.start_consuming()

        except:
            logger.exception("Basic consumer failing")
            self.fail()

        endTime = time.time()
        logger.debug("Completed (%f seconds)" % (endTime - startTime))

def messageHandler(channel, method, header, body):
    channel.basic_ack(delivery_tag=method.delivery_tag)

    if body == "quit":
        channel.basic_cancel(consumer_tag="test_consumer_tag")
        channel.stop_consuming()
        logger.info("Message body %r -- done " % body)
    else:
        logger.info("Message body %r" % body)
        time.sleep(0.25)
    #
    return

def suitePublishConsumeRequest():
    suite = unittest.TestSuite()
    suite.addTest(MessagePublishConsumeBasicTests('testPublishConsume'))
    #
    return suite

if __name__ == '__main__':
    runner = unittest.TextTestRunner(failfast=True)
    runner.run(suitePublishConsumeRequest())
