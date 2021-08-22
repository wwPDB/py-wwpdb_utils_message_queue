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
from __future__ import division, absolute_import, print_function

__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Creative Commons Attribution 3.0 Unported"
__version__ = "V0.07"

import logging
import threading
import pika

# import time

try:
    import exceptions
except ImportError:
    import builtins as exceptions


logger = logging.getLogger()


class MessageConsumerBase(object):
    """Message consumer base class -

    Unexpected connection issues with RabbitMQ such as channel and connection closures
    are handled gracefully.

    """

    def __init__(self, amqpUrl):
        """Create a new instance of the consumer class, passing in the AMQP URL used to connect to RabbitMQ.

        :param str amqp_url: The AMQP url to connect with

        """
        self._connection = None
        self._channel = None
        self._closing = False
        self._consumerTag = None
        self._url = amqpUrl
        #
        self.__exchange = None
        self.__exchangeType = None
        self.__queueName = None
        self.__routingKey = None
        #
        # self.__maxReconnectAttemps = 10
        # self.__reconnectInterval = 5
        #

    def setQueue(self, queueName, routingKey):
        self.__queueName = queueName
        self.__routingKey = routingKey
        #

    def setExchange(self, exchange, exchangeType="topic"):
        self.__exchange = exchange
        self.__exchangeType = exchangeType
        return True

    def workerMethod(self, msgBody, deliveryTag=None):
        raise exceptions.NotImplementedError

    def connect(self):
        """Create connection to RabbitMQ and return connection handle.

        Call back on_connection_open method is implemented.

        :rtype: pika.SelectConnection

        """
        logger.info("Connecting to %s", self._url)
        # return pika.SelectConnection(pika.URLParameters(self._url),
        #                              on_open_callback=self.onConnectionOpen,
        #                              on_open_error_callback=None,
        #                              stop_ioloop_on_close=False)

        return pika.BlockingConnection(
            pika.URLParameters(self._url),
        )
        #                               on_open_callback=self.onConnectionOpen,
        #                               on_open_error_callback=None,
        #                               stop_ioloop_on_close=False)

    def onConnectionOpenError(self, *args, **kw):  # pylint: disable=unused-argument
        """Callback on connection error  - not used  -"""
        logger.info("Catching connection error - ")
        raise pika.exceptions.AMQPConnectionError

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

    def onChannelOpen(self, channel):
        """This method is invoked by pika when the channel has been opened.
        The channel object is passed in so we can make use of it.

        QOS and Exchange are declared here -

        :param pika.channel.Channel channel: The channel object

        """
        logger.info("Channel opened")
        self._channel = channel
        #
        self._channel.basic_qos(prefetch_count=1)
        #
        self.addOnChannelCloseCallback()
        self.setupExchange(self.__exchange, self.__exchangeType)

    def addOnChannelCloseCallback(self):
        """This method tells pika to call the on_channel_closed method if
        RabbitMQ unexpectedly closes the channel.

        """
        logger.info("Adding channel close callback")
        self._channel.add_on_close_callback(self.onChannelClosed)

    def onChannelClosed(self, channel, reply_code, reply_text):
        """Invoked on unexpected closed the channel.

        :param pika.channel.Channel: The closed channel
        :param int reply_code: The numeric reason the channel was closed
        :param str reply_text: The text reason the channel was closed

        """
        logger.warning("Channel %i was closed: (%s) %s", channel, reply_code, reply_text)
        self._connection.close()

    def setupExchange(self, exchangeName, exchangeType):
        """Setup the exchange on RabbitMQ by invoking the Exchange.Declare RPC
        command. When it is complete, the on_exchange_declareok method will
        be invoked by pika.

        :param str|unicode exchange_name: The name of the exchange to declare

        """
        logger.info("Declaring exchange %s", exchangeName)
        self._channel.exchange_declare(callback=self.onExchangeDeclareOk, exchange=exchangeName, exchange_type=exchangeType, passive=False, durable=True)

    def onExchangeDeclareOk(self, unused_frame):
        """Invoked on successful Exchange.Declare command.

        :param pika.Frame.Method unused_frame: Exchange.DeclareOk response frame

        """
        logger.info("Exchange %s declared success", self.__exchange)
        self.setupQueue(self.__queueName)

    def setupQueue(self, queueName):
        """Declare queue on RabbitMQ by invoking the Queue.Declare command.
          On success invoke onQueueDeclareOk() method.

        :param str|unicode queue_name: The name of the queue to declare.

        """
        logger.info("Declaring queue %s", queueName)
        self._channel.queue_declare(callback=self.onQueueDeclareOk, queue=queueName, durable=True)

    def onQueueDeclareOk(self, method_frame):  # pylint: disable=unused-argument
        """Method invoked on success of Queue.Declare call made when setupQueue has completed.

        This method binds the queue and exchange with the routing key.
        On success, the onBindOk method will be invoked.

        :param pika.frame.Method method_frame: The Queue.DeclareOk frame

        """
        logger.info("Binding %s to %s with %s", self.__exchange, self.__queueName, self.__routingKey)
        self._channel.queue_bind(callback=self.onBindOk, queue=self.__queueName, exchange=self.__exchange, routing_key=self.__routingKey)

    def onBindOk(self, unused_frame):
        """Invoked by pika when the Queue.Bind method has completed. At this
        point we will start consuming messages by calling start_consuming
        which will invoke the needed RPC commands to start the process.

        :param pika.frame.Method unused_frame: The Queue.BindOk response frame

        """
        logger.info("Queue %s bound success", self.__queueName)
        self.startConsuming()

    def startConsuming(self):
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
        self._consumerTag = self._channel.basic_consume(queue=self.__queueName, on_message_callback=self.onMessage)

    def addOnCancelCallback(self):
        """Add a callback that will be invoked if RabbitMQ cancels the consumer
        for some reason.

        If RabbitMQ does cancel the consumer, onConsumerCancelled will be invoked.

        """
        logger.info("Adding consumer cancellation callback")
        self._channel.add_on_cancel_callback(self.onConsumerCancelled)

    def onConsumerCancelled(self, method_frame):
        """Invoked on a Basic.Cancel for a consumer receiving messages.

        :param pika.frame.Method method_frame: The Basic.Cancel frame

        """
        logger.info("Consumer was cancelled remotely, shutting down: %r", method_frame)
        if self._channel:
            self._channel.close()

    def onMessage(self, unused_channel, basic_deliver, properties, body):
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
                self._channel._connection.sleep(1.0)  # pylint: disable=protected-access
            # print("Back from thread")
            # self.workerMethod(msgBody=body, deliveryTag=basic_deliver.delivery_tag)
            # time.sleep(10)
        except Exception as e:
            logger.exception("Worker failing with exception")
            logger.exception(e)
        #
        logging.info("Done task")
        # unused_channel.basic_ack(delivery_tag = basic_deliver.delivery_tag)
        self.acknowledgeMessage(basic_deliver.delivery_tag)

    def acknowledgeMessage(self, deliveryTag):
        """Acknowledge the message delivery from RabbitMQ by sending a Basic.Ack method with the delivery tag.

        :param int delivery_tag: The delivery tag from the Basic.Deliver frame

        """
        logger.info("Acknowledging message %s", deliveryTag)
        self._channel.basic_ack(deliveryTag)

    def stopConsuming(self):
        """Tell RabbitMQ that you would like to stop consuming by sending the
        Basic.Cancel RPC command.

        """
        if self._channel:
            logger.info("Sending a Basic.Cancel command to RabbitMQ")
            self._channel.basic_cancel(callback=self.onCancelOk, consumer_tag=self._consumerTag)

    def onCancelOk(self, unused_frame):
        """This method is invoked by pika when RabbitMQ acknowledges the
        cancellation of a consumer. At this point we will close the channel.
        This will invoke the on_channel_closed method once the channel has been
        closed, which will in-turn close the connection.

        :param pika.frame.Method unused_frame: The Basic.CancelOk frame

        """
        logger.info("RabbitMQ acknowledged the cancellation of the consumer")
        self.closeChannel()

    def closeChannel(self):
        """Call to close the channel with RabbitMQ cleanly by issuing the
        Channel.Close RPC command.

        """
        logger.info("Closing the channel")
        self._channel.close()

    def run(self):
        """Run the example consumer by connecting to RabbitMQ and then
        starting the IOLoop to block and allow the SelectConnection to operate.

        """
        self._connection = self.connect()
        self._channel = self._connection.channel()
        #
        self._channel.queue_declare(queue=self.__queueName, durable=True)
        self._channel.basic_qos(prefetch_count=1)
        self._channel.basic_consume(queue=self.__queueName, on_message_callback=self.onMessage)
        #
        # self.addOnChannelCloseCallback()
        # self.setupExchange(self.__exchange, self.__exchangeType)
        self._channel.start_consuming()
        # self.onConnectionOpen()
        # self._connection.ioloop.start()

    def stop(self):
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

    def closeConnection(self):
        """This method closes the connection to RabbitMQ."""
        logger.info("Closing connection")
        self._connection.close()


#
