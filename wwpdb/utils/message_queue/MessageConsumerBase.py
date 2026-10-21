#
# File: MessageConsumerBase.py
# Date:  7-Sept-2016  J. Westbrook
#
# Updates:
##
"""
Async message consumer  -

This software was developed as part of the World Wide Protein Data Bank
Common Deposition and Annotation System Project

"""

__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Creative Commons Attribution 3.0 Unported"
__version__ = "V0.07"

import logging
import threading
from typing import Any, NoReturn, Optional, SupportsInt

import pika
import pika.exceptions
from pika import spec
from pika.adapters.blocking_connection import BlockingChannel

# import time

try:
    import exceptions  # type: ignore[import-not-found]
except ImportError:
    import builtins as exceptions


logger = logging.getLogger()


class MessageConsumerBase:
    """Message consumer base class -

    Unexpected connection issues with RabbitMQ such as channel and connection closures
    are handled gracefully.

    """

    def __init__(self, amqpUrl: str, priority: bool = False, local: bool = False) -> None:  # noqa: FBT001
        """Create a new instance of the consumer class, passing in the AMQP URL used to connect to RabbitMQ.

        :param str amqp_url: The AMQP url to connect with

        """
        self._connection: Optional[pika.BlockingConnection] = None
        self._channel: Optional[BlockingChannel] = None
        self._closing = False
        self._consumerTag: Optional[str] = None
        self._url = amqpUrl
        self.__exchange: Optional[str] = None
        self.__exchangeType: Optional[str] = None
        self.__queueName: Optional[str] = None
        self.__routingKey: Optional[str] = None

        self.__priority = priority
        self.__local = local

        #
        # self.__maxReconnectAttemps = 10
        # self.__reconnectInterval = 5
        #

    def setQueue(self, queueName: str, routingKey: str) -> None:
        self.__queueName = queueName
        self.__routingKey = routingKey

    def setExchange(self, exchange: str, exchangeType: str = "topic") -> bool:
        self.__exchange = exchange
        self.__exchangeType = exchangeType
        return True

    def workerMethod(self, msgBody: bytes, deliveryTag: Optional[int] = None) -> Optional[bool]:
        raise exceptions.NotImplementedError

    def __getChannel(self) -> BlockingChannel:
        if self._channel is None:
            emsg = "Channel is not open"
            raise pika.exceptions.ChannelWrongStateError(emsg)
        return self._channel

    def __getAsyncChannel(self) -> Any:
        """Channel used by the callback methods, which follow the asynchronous (SelectConnection)
        channel API.  BlockingChannel does not provide that API, so the result is left untyped.
        """
        return self.__getChannel()

    def __getConnection(self) -> pika.BlockingConnection:
        if self._connection is None:
            emsg = "Connection is not open"
            raise pika.exceptions.ConnectionWrongStateError(emsg)
        return self._connection

    def connect(self) -> pika.BlockingConnection:
        """Create connection to RabbitMQ and return connection handle.

        Call back on_connection_open method is implemented.

        :rtype: pika.SelectConnection

        """
        logger.info("Connecting to %s", self._url)
        # return pika.SelectConnection(pika.URLParameters(self._url),
        #                              on_open_callback=self.onConnectionOpen,
        #                              on_open_error_callback=None,
        #                              stop_ioloop_on_close=False)

        if self.__local:
            return pika.BlockingConnection(pika.ConnectionParameters("localhost"))

        return pika.BlockingConnection(
            pika.URLParameters(self._url),
        )
        #                               on_open_callback=self.onConnectionOpen,
        #                               on_open_error_callback=None,
        #                               stop_ioloop_on_close=False)

    def run(self) -> None:
        """Run the example consumer by connecting to RabbitMQ and then
        starting the IOLoop to block and allow the SelectConnection to operate.

        """
        self._connection = self.connect()
        self._channel = self._connection.channel()
        try:
            if self.__priority:
                self._channel.queue_declare(
                    queue=self.__queueName,  # type: ignore[arg-type]
                    durable=True,
                    arguments={"x-max-priority": 10},  # type: ignore[arg-type]  # pika-stubs mistypes arguments
                )
            else:
                self._channel.queue_declare(queue=self.__queueName, durable=True)  # type: ignore[arg-type]
        except Exception:  # noqa: BLE001
            self._connection.close()
            logger.critical("error - mixing of priority queues and non-priority queues")
            return

        self._channel.basic_qos(prefetch_count=1)
        self._channel.basic_consume(queue=self.__queueName, on_message_callback=self.onMessage)
        #
        # self.addOnChannelCloseCallback()
        # self.setupExchange(self.__exchange, self.__exchangeType)
        self._channel.start_consuming()
        # self.onConnectionOpen()
        # self._connection.ioloop.start()

    def onMessage(
        self,
        unused_channel: BlockingChannel,  # noqa: ARG002
        basic_deliver: spec.Basic.Deliver,
        properties: spec.BasicProperties,
        body: bytes,
    ) -> None:
        """Invoked when a message is delivered from RabbitMQ.

        The channel is passed.  The basic_deliver object that
        is passed in carries the exchange, routing key, delivery tag and
        a redelivered flag for the message. The properties passed in is an
        instance of BasicProperties with the message properties and the body
        is the message that was sent.

        :param pika.channel.Channel unused_channel: The channel object
        :param pika.Spec.Basic.Deliver: basic_deliver method
        :param pika.Spec.BasicProperties: properties
        :param str|unicode body: The message body

        """
        logger.info("Received message # %s from %s: %s", basic_deliver.delivery_tag, properties.app_id, body)
        try:
            thread = threading.Thread(target=self.workerMethod, args=(body, basic_deliver.delivery_tag))
            thread.start()
            while thread.is_alive():
                # Loop while the thread is processing
                # time.sleep(1.0)
                # self._channel.process_data_events()
                self.__getChannel().connection.sleep(1.0)
            # print("Back from thread")
            # self.workerMethod(msgBody=body, deliveryTag=basic_deliver.delivery_tag)
            # time.sleep(10)
        except Exception as e:
            logger.exception("Worker failing with exception")
            logger.exception(e)
        logger.info("Done task")
        # unused_channel.basic_ack(delivery_tag = basic_deliver.delivery_tag)
        self.acknowledgeMessage(basic_deliver.delivery_tag)

    def acknowledgeMessage(self, deliveryTag: Optional[int]) -> None:
        """Acknowledge the message delivery from RabbitMQ by sending a Basic.Ack method with the delivery tag.

        :param int delivery_tag: The delivery tag from the Basic.Deliver frame

        """
        logger.info("Acknowledging message %s", deliveryTag)
        self.__getChannel().basic_ack(deliveryTag)  # type: ignore[arg-type]  # always set on a delivered message

    def onChannelOpen(self, channel: BlockingChannel) -> None:
        """This method is invoked by pika when the channel has been opened.
        The channel object is passed in so we can make use of it.

        QOS and Exchange are declared here -

        :param pika.channel.Channel channel: The channel object

        """
        logger.info("Channel opened")
        self._channel = channel
        self._channel.basic_qos(prefetch_count=1)
        self.addOnChannelCloseCallback()
        self.setupExchange(self.__exchange, self.__exchangeType)

    def setupExchange(self, exchangeName: Optional[str], exchangeType: Optional[str]) -> None:
        """Setup the exchange on RabbitMQ by invoking the Exchange.Declare RPC
        command. When it is complete, the on_exchange_declareok method will
        be invoked by pika.

        :param str|unicode exchange_name: The name of the exchange to declare

        """
        logger.info("Declaring exchange %s", exchangeName)
        self.__getAsyncChannel().exchange_declare(
            callback=self.onExchangeDeclareOk,
            exchange=exchangeName,
            exchange_type=exchangeType,
            passive=False,
            durable=True,
        )

    def onExchangeDeclareOk(self, unused_frame: Any) -> None:  # noqa: ARG002
        """Invoked on successful Exchange.Declare command.

        :param pika.Frame.Method unused_frame: Exchange.DeclareOk response frame

        """
        logger.info("Exchange %s declared success", self.__exchange)
        self.setupQueue(self.__queueName)

    def setupQueue(self, queueName: Optional[str]) -> None:
        """Declare queue on RabbitMQ by invoking the Queue.Declare command.
          On success invoke onQueueDeclareOk() method.

        :param str|unicode queue_name: The name of the queue to declare.

        """
        logger.info("Declaring queue %s", queueName)
        try:
            if self.__priority:
                self.__getAsyncChannel().queue_declare(
                    callback=self.onQueueDeclareOk, queue=queueName, durable=True, arguments={"x-max-priority": 10}
                )
            else:
                self.__getAsyncChannel().queue_declare(callback=self.onQueueDeclareOk, queue=queueName, durable=True)
        except Exception as _exc:  # noqa: BLE001,F841
            self.__getConnection().close()
            logger.critical("error - mixing of priority queues and non-priority queues")

    def onQueueDeclareOk(self, method_frame: Any) -> None:  # noqa: ARG002 pylint: disable=unused-argument
        """Method invoked on success of Queue.Declare call made when setupQueue has completed.

        This method binds the queue and exchange with the routing key.
        On success, the onBindOk method will be invoked.

        :param pika.frame.Method method_frame: The Queue.DeclareOk frame

        """
        logger.info("Binding %s to %s with %s", self.__exchange, self.__queueName, self.__routingKey)
        self.__getAsyncChannel().queue_bind(
            callback=self.onBindOk, queue=self.__queueName, exchange=self.__exchange, routing_key=self.__routingKey
        )

    def onBindOk(self, unused_frame: Any) -> None:  # noqa: ARG002
        """Invoked by pika when the Queue.Bind method has completed. At this
        point we will start consuming messages by calling start_consuming
        which will invoke the needed RPC commands to start the process.

        :param pika.frame.Method unused_frame: The Queue.BindOk response frame

        """
        logger.info("Queue %s bound success", self.__queueName)
        self.startConsuming()

    def startConsuming(self) -> None:
        """This method sets up the consumer by first calling
        add_on_cancel_callback so that the object is notified if RabbitMQ
        cancels the consumer.
        Basic.Consume is invoked  which returns the consumer tag that is used to uniquely identify the
        consumer with RabbitMQ.

        We keep the value to use it when we want to cancel consuming.

        The onMessage method is passed in as a callback pika will invoke when a message is fully received.

        """
        logger.info("Issuing consumer related RPC commands")
        self.addOnCancelCallback()
        self._consumerTag = self.__getChannel().basic_consume(
            queue=self.__queueName, on_message_callback=self.onMessage
        )

    def addOnCancelCallback(self) -> None:
        """Add a callback that will be invoked if RabbitMQ cancels the consumer
        for some reason.

        If RabbitMQ does cancel the consumer, onConsumerCancelled will be invoked.

        """
        logger.info("Adding consumer cancellation callback")
        self.__getChannel().add_on_cancel_callback(self.onConsumerCancelled)

    @staticmethod
    def onConnectionOpenError(*args: Any, **kw: Any) -> NoReturn:  # noqa: ARG004 pylint: disable=unused-argument
        """Callback on connection error  - not used  -"""
        logger.info("Catching connection error - ")
        raise pika.exceptions.AMQPConnectionError

    def addOnChannelCloseCallback(self) -> None:
        """This method tells pika to call the on_channel_closed method if
        RabbitMQ unexpectedly closes the channel.

        """
        logger.info("Adding channel close callback")
        self.__getAsyncChannel().add_on_close_callback(self.onChannelClosed)

    def onChannelClosed(self, channel: SupportsInt, reply_code: int, reply_text: str) -> None:
        """Invoked on unexpected closed the channel.

        :param pika.channel.Channel: The closed channel
        :param int reply_code: The numeric reason the channel was closed
        :param str reply_text: The text reason the channel was closed

        """
        logger.warning("Channel %i was closed: (%s) %s", channel, reply_code, reply_text)
        self.__getConnection().close()

    def onConsumerCancelled(self, method_frame: spec.Basic.Cancel) -> None:
        """Invoked on a Basic.Cancel for a consumer receiving messages.

        :param pika.frame.Method method_frame: The Basic.Cancel frame

        """
        logger.info("Consumer was cancelled remotely, shutting down: %r", method_frame)
        if self._channel:
            self._channel.close()

    def stopConsuming(self) -> None:
        """Tell RabbitMQ that you would like to stop consuming by sending the
        Basic.Cancel RPC command.

        """
        if self._channel:
            logger.info("Sending a Basic.Cancel command to RabbitMQ")
            self.__getAsyncChannel().basic_cancel(callback=self.onCancelOk, consumer_tag=self._consumerTag)

    def onCancelOk(self, unused_frame: Any) -> None:  # noqa: ARG002
        """This method is invoked by pika when RabbitMQ acknowledges the
        cancellation of a consumer. At this point we will close the channel.
        This will invoke the on_channel_closed method once the channel has been
        closed, which will in-turn close the connection.

        :param pika.frame.Method unused_frame: The Basic.CancelOk frame

        """
        logger.info("RabbitMQ acknowledged the cancellation of the consumer")
        self.closeChannel()

    def closeChannel(self) -> None:
        """Call to close the channel with RabbitMQ cleanly by issuing the
        Channel.Close RPC command.

        """
        logger.info("Closing the channel")
        self.__getChannel().close()

    def stop(self) -> None:
        """Cleanly shutdown the connection to RabbitMQ by stopping the consumer
           with RabbitMQ.

        When RabbitMQ confirms the cancellation, onCancelOk
        will be invoked by pika, which will then closing the channel and
        connection. The IOLoop is started again because this method is invoked
        when the exception is raised (e.g., CTRL-C is pressed raising a KeyboardInterrupt
        exception. This exception stops the IOLoop which needs to be running for pika to
        communicate with RabbitMQ. All of the commands issued prior to starting
        the IOLoop will be buffered but not processed.

            mc = MessageConsumer('amqp://guest:guest@localhost:5672/%2F')
            try:
                mc.run()
            except KeyboardInterrupt:
                mc.stop()

        """
        logger.info("Clean stop")
        self._closing = True
        self.stopConsuming()
        logger.info("Cleanly stopped")
        # self._connection.ioloop.start()

    def closeConnection(self) -> None:
        """This method closes the connection to RabbitMQ."""
        logger.info("Closing connection")
        self.__getConnection().close()

    #
    # Blocking connection does not have callbacks
    # def onConnectionOpen(self, unusedConnection):  # pylint: disable=unused-argument
    #     """Callback method on successful connection to RabbitMQ server.

    #     :type unused_connection: pika.SelectConnection

    #     """
    #     logger.info("Connection opened")
    #     self.addOnConnectionCloseCallback()
    #     self.openChannel()

    # Blocking connection does not have callbacks
    # def addOnConnectionCloseCallback(self):
    #     """This method adds an on close callback that will be invoked by pika
    #     when RabbitMQ closes the connection to the publisher unexpectedly.

    #     """
    #     logger.info("Adding connection close callback")
    #     self._connection.add_on_close_callback(self.onConnectionClosed)

    # Blocking connctions do not support this method
    # def onConnectionClosed(self, connection, reply_code, reply_text):  # pylint: disable=unused-argument
    #     """This method is invoked by pika when the connection to RabbitMQ is
    #     closed unexpectedly. Since it is unexpected, we will reconnect to
    #     RabbitMQ if it disconnects.

    #     :param pika.connection.Connection connection: The closed connection obj
    #     :param int reply_code: The server provided reply_code if given
    #     :param str reply_text: The server provided reply_text if given

    #     """
    #     self._channel = None
    #     if self._closing:
    #         self._connection.ioloop.stop()
    #     else:
    #         logger.warning("Connection closed, reopening in 5 seconds: (%s) %s", reply_code, reply_text)
    #         self._connection.add_timeout(5, self.reconnect)

    # Callbacks only on asyncio
    # def reconnect(self):
    #     """Callback invoked by the IOLoop timer if the connection is closed.

    #     See the onConnectionClosed method.

    #     Extended reconnection attempts are performed to handle RabbitMQ server restarts.

    #     """
    #     # This is the old connection IOLoop instance, stop its ioloop
    #     self._connection.ioloop.stop()

    #     if not self._closing:

    #         # Create a new connection
    #         self.__maxReconnectAttemps = 10
    #         self.__reconnectInterval = 5
    #         iTry = 1
    #         while True:
    #             try:
    #                 if iTry > self.__maxReconnectAttemps:
    #                     logger.info("Quitting after %d reconnect attempts", iTry)
    #                     break
    #                 else:
    #                     logger.info("Reconnect attempt %d", iTry)
    #                 self._connection = self.connect()
    #                 break
    #             except pika.exceptions.AMQPConnectionError:
    #                 iTry += 1
    #                 time.sleep(self.__reconnectInterval * iTry)

    #         # There is now a new connection, needs a new ioloop to run
    #         self._connection.ioloop.start()

    # Callbacks only implemented for async connections and we use Blocking.
    # def openChannel(self):
    #     """Open a new channel with RabbitMQ by issuing the Channel.Open RPC
    #     command.

    #     On success the onChannelOpen callback will be invoked.

    #     """
    #     logger.info("Creating a new channel")
    #     self._connection.channel(on_open_callback=self.onChannelOpen)
