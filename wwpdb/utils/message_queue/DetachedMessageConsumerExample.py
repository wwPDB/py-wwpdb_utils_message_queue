#
# File:  DetachedMessageConsumerExample.py
# Date:  7-Sep-2016
#
#  Contolling wrapper for consumer client service -  This provides a template for
#  implementing specific services.
#
#  Updates:
#
#  8-Sep-2016  jdw overhaul
#  9-Sep-2016  jdw now as example class =
#
##

import sys
import os
import platform
import time
import logging
from optparse import OptionParser  # pylint: disable=deprecated-module

from wwpdb.utils.detach.DetachedProcessBase import DetachedProcessBase
from wwpdb.utils.message_queue.MessageConsumerBase import MessageConsumerBase

#
from wwpdb.utils.message_queue.MessageQueueConnection import MessageQueueConnection
from wwpdb.utils.config.ConfigInfo import ConfigInfo, getSiteId

logger = logging.getLogger()
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s")


class MessageConsumer(MessageConsumerBase):
    """This class would be replaced to perform application specific services..."""

    # def __init__(self, amqpUrl):
    #     super(MessageConsumer, self).__init__(amqpUrl)

    def workerMethod(self, msgBody, deliveryTag=None):
        logger.info("Message body %r", msgBody)
        return True


class MessageConsumerWorker(object):
    def __init__(self):
        self.__setup()

    def __setup(self):
        mqc = MessageQueueConnection()
        url = mqc._getSslConnectionUrl()  # pylint: disable=protected-access
        self.__mc = MessageConsumer(amqpUrl=url)
        self.__mc.setQueue(queueName="test_queue", routingKey="text_message")
        self.__mc.setExchange(exchange="test_exchange", exchangeType="topic")
        #

    def run(self):
        """Run async consumer"""
        startTime = time.time()
        logger.info("Starting ")
        try:
            try:
                logger.info("Run consumer worker starts")
                self.__mc.run()
            except KeyboardInterrupt:
                self.__mc.stop()
        except Exception:
            logger.exception("MessageConsumer failing")

        endTime = time.time()
        logger.info("Completed (%f seconds)", (endTime - startTime))

    def suspend(self):
        logger.info("Suspending consumer worker... ")
        self.__mc.stop()


class MyDetachedProcess(DetachedProcessBase):
    """This class implements the run() method of the DetachedProcessBase() utility class.

    Illustrates the use of python logging and various I/O channels in detached process.
    """

    def __init__(self, pidFile="/tmp/DetachedProcessBase.pid", stdin=os.devnull, stdout=os.devnull, stderr=os.devnull, wrkDir="/", gid=None, uid=None):
        super(MyDetachedProcess, self).__init__(pidFile=pidFile, stdin=stdin, stdout=stdout, stderr=stderr, wrkDir=wrkDir, gid=gid, uid=uid)
        self.__mcw = MessageConsumerWorker()

    def run(self):
        logger.info("STARTING detached run method")
        self.__mcw.run()

    def suspend(self):
        logger.info("SUSPENDING detached process")
        try:
            self.__mcw.suspend()
        except Exception as _e:  # noqa: F841
            pass


def main():
    # adding a conservative permission mask for this
    # os.umask(0o022)
    #
    siteId = getSiteId(defaultSiteId=None)
    cI = ConfigInfo(siteId)

    #    topPath = cI.get('SITE_WEB_APPS_TOP_PATH')
    topSessionPath = cI.get("SITE_WEB_APPS_TOP_SESSIONS_PATH")

    #
    myFullHostName = platform.uname()[1]
    myHostName = str(myFullHostName.split(".")[0]).lower()
    #
    wsLogDirPath = os.path.join(topSessionPath, "ws-logs")

    #  Setup logging  --
    now = time.strftime("%Y-%m-%d", time.localtime())

    usage = "usage: %prog [options]"
    parser = OptionParser(usage)
    parser.add_option("--start", default=False, action="store_true", dest="startOp", help="Start consumer client process")
    parser.add_option("--stop", default=False, action="store_true", dest="stopOp", help="Stop consumer client process")
    parser.add_option("--restart", default=False, action="store_true", dest="restartOp", help="Restart consumer client process")
    parser.add_option("--status", default=False, action="store_true", dest="statusOp", help="Report consumer client process status")

    # parser.add_option("-v", "--verbose", default=False, action="store_true", dest="verbose", help="Enable verbose output")
    parser.add_option("--debug", default=1, type="int", dest="debugLevel", help="Debug level (default=1) [0-3]")
    parser.add_option("--instance", default=1, type="int", dest="instanceNo", help="Instance number [1-n]")
    #
    (options, _args) = parser.parse_args()
    #
    pidFilePath = os.path.join(wsLogDirPath, myHostName + "_" + str(options.instanceNo) + ".pid")
    stdoutFilePath = os.path.join(wsLogDirPath, myHostName + "_" + str(options.instanceNo) + "_stdout.log")
    stderrFilePath = os.path.join(wsLogDirPath, myHostName + "_" + str(options.instanceNo) + "_stderr.log")
    wfLogFilePath = os.path.join(wsLogDirPath, myHostName + "_" + str(options.instanceNo) + "_" + now + ".log")
    #
    logger = logging.getLogger(name="root")  # pylint: disable=redefined-outer-name
    logging.captureWarnings(True)
    formatter = logging.Formatter("%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s")
    handler = logging.FileHandler(wfLogFilePath)
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    #
    lt = time.strftime("%Y %m %d %H:%M:%S", time.localtime())
    #
    if options.debugLevel > 2:
        logger.setLevel(logging.DEBUG)
    elif options.debugLevel > 0:
        logger.setLevel(logging.INFO)
    else:
        logger.setLevel(logging.ERROR)
    #
    #
    myDP = MyDetachedProcess(pidFile=pidFilePath, stdout=stdoutFilePath, stderr=stderrFilePath, wrkDir=wsLogDirPath)

    if options.startOp:
        sys.stdout.write("+DetachedMessageConsumer() starting consumer service at %s\n" % lt)
        logger.info("DetachedMessageConsumer() starting consumer service at %s", lt)
        myDP.start()
    elif options.stopOp:
        sys.stdout.write("+DetachedMessageConsumer() stopping consumer service at %s\n" % lt)
        logger.info("DetachedMessageConsumer() stopping consumer service at %s", lt)
        myDP.stop()
    elif options.restartOp:
        sys.stdout.write("+DetachedMessageConsumer() restarting consumer service at %s\n" % lt)
        logger.info("DetachedMessageConsumer() restarting consumer service at %s", lt)
        myDP.restart()
    elif options.statusOp:
        sys.stdout.write("+DetachedMessageConsumer() reporting status for consumer service at %s\n" % lt)
        sys.stdout.write(myDP.status())
    else:
        pass


if __name__ == "__main__":
    main()
