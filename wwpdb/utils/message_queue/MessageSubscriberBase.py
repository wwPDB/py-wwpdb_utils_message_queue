#
# File: MessageSubscriberBase.py
# Date:  21-Mar-2023   J. Smith
#
# Updates:
##
"""
Async message consumer  -

This software was developed as part of the World Wide Protein Data Bank
Common Deposition and Annotation System Project

"""
from __future__ import division, absolute_import, print_function

__docformat__ = "restructuredtext en"
__author__ = "James Smith"
__email__ = "james_smithrcsb.org"
__license__ = "Creative Commons Attribution 3.0 Unported"
__version__ = "V0.07"

import logging
import threading
import pika

try:
    import exceptions
except ImportError:
    import builtins as exceptions


logger = logging.getLogger()

"""
direct or exchange consumer with temporary queue, one for each consumer, which deletes on closing
hence, the subscriber must start before the publisher begins producing messages, since a queue must exist to store messages
each consumer may bind to multiple exchanges, hence the add_exchange function
the publisher and subscriber must use unique names for the exchanges that are not used by other queues
the routing keys of each exchange published to by the producer must match the routing keys used by the consumer
hence, a default key has been set so that the producer and consumer must only coordinate their exchange names
the publishDirect method has been implemented in the MessagePublisher class for the purpose of publishing to a subscriber
"""


class MessageSubscriberBase(object):
    def __init__(self, amqpUrl, local=False):
        self._url = amqpUrl
        self._closing = False
        self._consumerTag = None
        self.local = local

        self.__exchange_type = "direct"
        self.__routing_key = "subscriber_routing_key"
        self.__exchanges = []

        self._connection = self.connect()
        self._channel = self._connection.channel()
        try:
            result = self._channel.queue_declare(queue="", exclusive=True, durable=True)
        except:  # noqa: E722 pylint: disable=bare-except
            self._connection.close()
            logger.critical("error - mixing of priority queues and non-priority queues")
            return
        self.__queue_name = result.method.queue
        self._channel.basic_qos(prefetch_count=1)

    def add_exchange(self, exchange):
        self.__exchanges.append(exchange)
        self._channel.exchange_declare(exchange=exchange, exchange_type=self.__exchange_type, passive=False, durable=True)
        self._channel.queue_bind(exchange=exchange, queue=self.__queue_name, routing_key=self.__routing_key)

    def run(self):
        if len(self.__exchanges) == 0:
            logger.info("error - no exchanges")
            return

        self._channel.basic_consume(queue=self.__queue_name, on_message_callback=self.onMessage)
        #
        self._channel.start_consuming()

    def workerMethod(self, msgBody, deliveryTag=None):
        raise exceptions.NotImplementedError

    def connect(self):
        logger.info("Connecting to %s", self._url)

        if self.local:
            return pika.BlockingConnection(pika.ConnectionParameters("localhost"))

        return pika.BlockingConnection(
            pika.URLParameters(self._url),
        )

    def onChannelOpen(self, channel):
        logger.info("Channel opened")
        self._channel = channel
        self._channel.basic_qos(prefetch_count=1)
        self._channel.add_on_close_callback(self.onChannelClosed)

    def startConsuming(self):
        logger.info("Issuing consumer related RPC commands")
        self._channel.add_on_cancel_callback(self.onConsumerCancelled)
        self._consumerTag = self._channel.basic_consume(queue=self.__queue_name, on_message_callback=self.onMessage)

    def onMessage(self, unused_channel, basic_deliver, properties, body):
        logger.info("Received message # %s from %s: %s", basic_deliver.delivery_tag, properties.app_id, body)
        try:
            thread = threading.Thread(target=self.workerMethod, args=(body, basic_deliver.delivery_tag))
            thread.start()
            while thread.is_alive():
                self._channel._connection.sleep(1.0)  # pylint: disable=protected-access
        except Exception as e:
            logger.exception("Worker failing with exception")
            logger.exception(e)
        #
        logging.info("Done task")
        self.acknowledgeMessage(basic_deliver.delivery_tag)

    def acknowledgeMessage(self, deliveryTag):
        logger.info("Acknowledging message %s", deliveryTag)
        self._channel.basic_ack(deliveryTag)

    def onConnectionOpenError(self, *args, **kw):  # pylint: disable=unused-argument
        logger.info("Catching connection error - ")
        raise pika.exceptions.AMQPConnectionError

    def onConsumerCancelled(self, method_frame):
        logger.info("Consumer was cancelled remotely, shutting down: %r", method_frame)
        if self._channel:
            self._channel.close()

    def stopConsuming(self):
        if self._channel:
            logger.info("Sending a Basic.Cancel command to RabbitMQ")
            self._channel.basic_cancel(callback=self.onCancelOk, consumer_tag=self._consumerTag)

    def onCancelOk(self, unused_frame):
        logger.info("RabbitMQ acknowledged the cancellation of the consumer")
        self.closeChannel()

    def onChannelClosed(self, channel, reply_code, reply_text):
        logger.warning("Channel %i was closed: (%s) %s", channel, reply_code, reply_text)
        self._connection.close()

    def closeChannel(self):
        logger.info("Closing the channel")
        self._channel.close()

    def stop(self):
        logger.info("Clean stop")
        self._closing = True
        self.stopConsuming()
        logger.info("Cleanly stopped")

    def closeConnection(self):
        logger.info("Closing connection")
        self._connection.close()


#
