#
# File: MessageConsumerBaseTests.py
# Date:  31-Aug-2016  J. Westbrook
#
# Updates:
##
"""
Illustrative tests of the MessageConsumerBase.py class -

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

from wwpdb.utils.message_queue.MessageConsumerBase import MessageConsumerBase
from wwpdb.utils.message_queue.MessageQueueConnection import MessageQueueConnection
#

logging.basicConfig(level=logging.INFO, format='\n[%(levelname)s]-%(module)s.%(funcName)s: %(message)s')
logger = logging.getLogger()


class MessageConsumer(MessageConsumerBase):

    def __init__(self, amqpUrl):
        super(MessageConsumer, self).__init__(amqpUrl)

    def workerMethod(self, msgBody):
        logger.info("Message body %r" % msgBody)
        return True


class MessageConsumerBaseTests(unittest.TestCase):

    def setUp(self):
        self.__messageCount = 0
        #

    def testMessageConsumer(self):
        """  Test case:  run async consumer
        """
        startTime = time.time()
        logger.info("Starting")
        try:
            self.__messageCount = 0
            mqc = MessageQueueConnection()
            url = mqc._getSslConnectionUrl()
            mc = MessageConsumer(amqpUrl=url)
            mc.setQueue(queueName="test_queue", routingKey="text_message")
            mc.setExchange(exchange="test_exchange", exchangeType="topic")
            try:
                mc.run()
            except KeyboardInterrupt:
                mc.stop()
        except:
            logger.exception("MessageConsumer failing")
            self.fail()

        endTime = time.time()
        logger.info("Completed (%f seconds)" % (endTime - startTime))


def suiteMessageConsumer():
    suite = unittest.TestSuite()
    suite.addTest(MessageConsumerBaseTests('testMessageConsumer'))
    #
    return suite

if __name__ == '__main__':
    runner = unittest.TextTestRunner(failfast=True)
    runner.run(suiteMessageConsumer())
