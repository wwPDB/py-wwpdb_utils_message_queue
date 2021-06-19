#
# File: MessagePublisher.py
# Date:  9-Sep-2016  J. Westbrook
#
# Updates:
#  18-Feb-2017  jdw  use default connection parameters
##
"""
Simple wrapper providing message publishing methods.

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


import pika
import time
import logging

#
from wwpdb.utils.message_queue.MessageQueueConnection import MessageQueueConnection

logger = logging.getLogger()


class MessagePublisher(object):
    def __init__(self):
        pass

    def publish(self, message, exchangeName, queueName, routingKey):
        return self.__publishMessage(message=message, exchangeName=exchangeName, queueName=queueName, routingKey=routingKey)

    def __publishMessage(self, message, exchangeName, queueName, routingKey, durableFlag=True, deliveryMode=2):
        """publish the input message -"""
        startTime = time.time()
        logger.debug("Starting to publish message ")
        ok = False
        try:
            mqc = MessageQueueConnection()
            parameters = mqc._getDefaultConnectionParameters()  # pylint: disable=protected-access
            connection = pika.BlockingConnection(parameters)
            channel = connection.channel()
            channel.exchange_declare(exchange=exchangeName, exchange_type="topic", durable=True, auto_delete=False)

            result = channel.queue_declare(queue=queueName, durable=durableFlag)
            channel.queue_bind(exchange=exchangeName, queue=result.method.queue, routing_key=routingKey)

            #
            channel.basic_publish(
                exchange=exchangeName,
                routing_key=routingKey,
                body=message,
                properties=pika.BasicProperties(
                    delivery_mode=deliveryMode,  # set message persistence
                ),
            )
            #
            ok = True
            connection.close()
        except Exception:
            logger.exception("Publish request failing")

        endTime = time.time()
        logger.debug("Completed publish request in (%f seconds) status %r", endTime - startTime, ok)
        return ok
