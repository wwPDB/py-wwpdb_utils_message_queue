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
import re
import sys

#
from wwpdb.utils.message_queue.MessageQueueConnection import MessageQueueConnection

logger = logging.getLogger()


class MessagePublisher(object):
    def __init__(self, local=False):
        self.__local = local
        self.__subscriber_exchange_type = "direct"
        self.__subscriber_routing_key = "subscriber_routing_key"

    def publish(self, message, exchangeName, queueName, routingKey, priority=None):
        # priority is either None or an integer between 1 and 10
        if priority and not re.match(r"^\d+$", str(priority)):
            priority = 1
        return self.__publishMessage(message=message, exchangeName=exchangeName, queueName=queueName, routingKey=routingKey, priority=priority)

    def __publishMessage(self, message, exchangeName, queueName, routingKey, durableFlag=True, deliveryMode=2, priority=None):
        """publish the input message -"""
        startTime = time.time()
        logger.debug("Starting to publish message ")
        ok = False
        try:
            if self.__local:
                connection = pika.BlockingConnection(pika.ConnectionParameters("localhost"))
            else:
                mqc = MessageQueueConnection()
                parameters = mqc._getDefaultConnectionParameters()  # pylint: disable=protected-access
                connection = pika.BlockingConnection(parameters)

            channel = connection.channel()
            channel.exchange_declare(exchange=exchangeName, exchange_type="topic", durable=True, auto_delete=False)

            try:
                if priority:
                    result = channel.queue_declare(queue=queueName, durable=durableFlag, arguments={"x-max-priority": 10})
                else:
                    result = channel.queue_declare(queue=queueName, durable=durableFlag)
            except pika.exceptions.ChannelClosedByBroker as exc:
                connection.close()
                logger.critical("error - priority type of pre-existing queue does not match new queue")
                if sys.version_info[0] == 2:
                    raise pika.exceptions.ChannelClosedByBroker(exc.reply_code, exc.reply_text)  # pylint: disable=raise-missing-from,broad-exception-raised
                else:
                    raise pika.exceptions.ChannelClosedByBroker(exc.reply_code, exc.reply_text) from exc
            except Exception as _exc:  # noqa: F841
                connection.close()
                logger.critical("error - mixing of regular queues and priority queues")
                raise Exception  # pylint: disable=raise-missing-from,broad-exception-raised

            channel.queue_bind(exchange=exchangeName, queue=result.method.queue, routing_key=routingKey)

            #
            if priority:
                channel.basic_publish(
                    exchange=exchangeName,
                    routing_key=routingKey,
                    body=message,
                    properties=pika.BasicProperties(delivery_mode=deliveryMode, priority=priority),  # set message persistence
                )
            else:
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

    # direct exchange pattern having extensive reliance on exchanges, with no queue declare or queue bind from publisher

    def publishDirect(self, message, exchangeName):
        return self.__publishDirect(message=message, exchangeName=exchangeName)

    def __publishDirect(self, message, exchangeName, durableFlag=True, deliveryMode=2):  # pylint: disable=unused-argument
        """publish the input message -"""
        startTime = time.time()
        logger.debug("Starting to publish message ")
        ok = False
        try:
            if self.__local:
                connection = pika.BlockingConnection(pika.ConnectionParameters("localhost"))
            else:
                mqc = MessageQueueConnection()
                parameters = mqc._getDefaultConnectionParameters()  # pylint: disable=protected-access
                connection = pika.BlockingConnection(parameters)

            channel = connection.channel()
            channel.exchange_declare(exchange=exchangeName, exchange_type=self.__subscriber_exchange_type, durable=True, auto_delete=False)

            channel.basic_publish(
                exchange=exchangeName,
                routing_key=self.__subscriber_routing_key,
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
