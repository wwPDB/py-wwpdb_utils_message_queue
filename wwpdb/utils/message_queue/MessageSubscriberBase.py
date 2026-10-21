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

__docformat__ = "restructuredtext en"
__author__ = "James Smith"
__email__ = "james_smithrcsb.org"
__license__ = "Creative Commons Attribution 3.0 Unported"
__version__ = "V0.07"

import logging
import threading
from typing import Any, List, NoReturn, Optional, SupportsInt

import pika
import pika.exceptions
from pika import spec
from pika.adapters.blocking_connection import BlockingChannel

try:
    import exceptions  # type: ignore[import-not-found]
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


class MessageSubscriberBase:
    def __init__(self, amqpUrl: str, local: bool = False) -> None:  # noqa: FBT001
        self._url = amqpUrl
        self._closing = False
        self._consumerTag: Optional[str] = None
        self.local = local

        self.__exchange_type = "direct"
        self.__routing_key = "subscriber_routing_key"
        self.__exchanges: List[str] = []

        self._connection: pika.BlockingConnection = self.connect()
        self._channel: BlockingChannel = self._connection.channel()
        try:
            result = self._channel.queue_declare(queue="", exclusive=True, durable=True)
        except:  # noqa: E722 pylint: disable=bare-except
            self._connection.close()
            logger.critical("error - mixing of priority queues and non-priority queues")
            return
        self.__queue_name: str = result.method.queue  # type: ignore[assignment]  # set by the broker
        self._channel.basic_qos(prefetch_count=1)

    def add_exchange(self, exchange: str) -> None:
        self.__exchanges.append(exchange)
        self._channel.exchange_declare(
            exchange=exchange, exchange_type=self.__exchange_type, passive=False, durable=True
        )
        self._channel.queue_bind(exchange=exchange, queue=self.__queue_name, routing_key=self.__routing_key)

    def run(self) -> None:
        if len(self.__exchanges) == 0:
            logger.info("error - no exchanges")
            return

        self._channel.basic_consume(queue=self.__queue_name, on_message_callback=self.onMessage)
        self._channel.start_consuming()

    def workerMethod(self, msgBody: bytes, deliveryTag: Optional[int] = None) -> Optional[bool]:
        raise exceptions.NotImplementedError

    def connect(self) -> pika.BlockingConnection:
        logger.info("Connecting to %s", self._url)

        if self.local:
            return pika.BlockingConnection(pika.ConnectionParameters("localhost"))

        return pika.BlockingConnection(
            pika.URLParameters(self._url),
        )

    def onChannelOpen(self, channel: BlockingChannel) -> None:
        logger.info("Channel opened")
        self._channel = channel
        self._channel.basic_qos(prefetch_count=1)
        self.__getAsyncChannel().add_on_close_callback(self.onChannelClosed)

    def __getAsyncChannel(self) -> Any:
        """Channel used by the callback methods, which follow the asynchronous (SelectConnection)
        channel API.  BlockingChannel does not provide that API, so the result is left untyped.
        """
        return self._channel

    def startConsuming(self) -> None:
        logger.info("Issuing consumer related RPC commands")
        self._channel.add_on_cancel_callback(self.onConsumerCancelled)
        self._consumerTag = self._channel.basic_consume(queue=self.__queue_name, on_message_callback=self.onMessage)

    def onMessage(
        self,
        unused_channel: BlockingChannel,  # noqa: ARG002
        basic_deliver: spec.Basic.Deliver,
        properties: spec.BasicProperties,
        body: bytes,
    ) -> None:
        logger.info("Received message # %s from %s: %s", basic_deliver.delivery_tag, properties.app_id, body)
        try:
            thread = threading.Thread(target=self.workerMethod, args=(body, basic_deliver.delivery_tag))
            thread.start()
            while thread.is_alive():
                self._channel.connection.sleep(1.0)
        except Exception as e:
            logger.exception("Worker failing with exception")
            logger.exception(e)
        logger.info("Done task")
        self.acknowledgeMessage(basic_deliver.delivery_tag)

    def acknowledgeMessage(self, deliveryTag: Optional[int]) -> None:
        logger.info("Acknowledging message %s", deliveryTag)
        self._channel.basic_ack(deliveryTag)  # type: ignore[arg-type]  # always set on a delivered message

    @staticmethod
    def onConnectionOpenError(*args: Any, **kw: Any) -> NoReturn:  # noqa: ARG004 pylint: disable=unused-argument
        logger.info("Catching connection error - ")
        raise pika.exceptions.AMQPConnectionError

    def onConsumerCancelled(self, method_frame: spec.Basic.Cancel) -> None:
        logger.info("Consumer was cancelled remotely, shutting down: %r", method_frame)
        if self._channel:
            self._channel.close()

    def stopConsuming(self) -> None:
        if self._channel:
            logger.info("Sending a Basic.Cancel command to RabbitMQ")
            self.__getAsyncChannel().basic_cancel(callback=self.onCancelOk, consumer_tag=self._consumerTag)

    def onCancelOk(self, unused_frame: Any) -> None:  # noqa: ARG002
        logger.info("RabbitMQ acknowledged the cancellation of the consumer")
        self.closeChannel()

    def onChannelClosed(self, channel: SupportsInt, reply_code: int, reply_text: str) -> None:
        logger.warning("Channel %i was closed: (%s) %s", channel, reply_code, reply_text)
        self._connection.close()

    def closeChannel(self) -> None:
        logger.info("Closing the channel")
        self._channel.close()

    def stop(self) -> None:
        logger.info("Clean stop")
        self._closing = True
        self.stopConsuming()
        logger.info("Cleanly stopped")

    def closeConnection(self) -> None:
        logger.info("Closing connection")
        self._connection.close()
