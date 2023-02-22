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
import argparse

if __package__ is None or __package__ == "":
    import sys
    from os import path

    sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
    from commonsetup import TESTOUTPUT  # pylint: disable=import-error,unused-import
else:
    from .commonsetup import TESTOUTPUT  # noqa: F401

from wwpdb.utils.message_queue.MessageConsumerBase import MessageConsumerBase
from wwpdb.utils.message_queue.MessageQueueConnection import MessageQueueConnection
from wwpdb.utils.testing.Features import Features

#

logging.basicConfig(level=logging.INFO, format="\n[%(levelname)s]-%(module)s.%(funcName)s: %(message)s")
logger = logging.getLogger()


# This test needs to run from main - otherwise will block
inmain = True if __name__ == "__main__" else False


@unittest.skipUnless(Features().haveRbmqTestServer(), "require Rbmq Test Environment")
class MessageConsumer(MessageConsumerBase):
    def workerMethod(self, msgBody, deliveryTag=None):
        logger.info("Message body %r", msgBody)
        return True


# @unittest.skipUnless(Features().haveRbmqTestServer(), "require Rbmq Test Environment")
@unittest.skipUnless(inmain, "require running from main()")
class MessageConsumerBaseTests(unittest.TestCase):
    LOCAL = False

    def testMessageConsumer(self):
        """Test case:  run async consumer"""
        startTime = time.time()
        logger.info("Starting")
        try:
            mqc = MessageQueueConnection()
            if self.LOCAL:
                url = 'localhost'
            else:
                url = mqc._getSslConnectionUrl()  # pylint: disable=protected-access
            mc = MessageConsumer(amqpUrl=url, local=self.LOCAL)
            mc.setQueue(queueName="test_queue", routingKey="text_message")
            mc.setExchange(exchange="test_exchange", exchangeType="topic")
            try:
                mc.run()
            except KeyboardInterrupt:
                mc.stop()
        except Exception:
            logger.exception("MessageConsumer failing")
            self.fail()

        endTime = time.time()
        logger.info("Completed (%f seconds)", (endTime - startTime))


def suiteMessageConsumer():
    suite = unittest.TestSuite()
    suite.addTest(MessageConsumerBaseTests("testMessageConsumer"))
    #
    return suite


if __name__ == "__main__":
    parser = argparse.ArgumentParser(formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument('-l', '--local', action='store_true', help='run on local host')
    args = parser.parse_args()
    LOCAL = False
    if args.local:
        LOCAL = True
    MessageConsumerBaseTests.LOCAL = LOCAL
    runner = unittest.TextTestRunner(failfast=True)
    runner.run(suiteMessageConsumer())
