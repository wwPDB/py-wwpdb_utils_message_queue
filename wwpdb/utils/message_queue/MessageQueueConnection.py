#
# File: MessageQueueConnection.py
# Date:  9-Sep-2016  J. Westbrook
#
# Updates:
#    17-Feb-2017 jdw add method to obtain parameters encoded as URL on standard port -
#    18-Feb-2017 jdw add _getDefaultConnectionUrl() and _getDefaultConnectionParameters()
##
"""
Provide support essential connection methods shared by all messaging clients.

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
import logging

#
try:
    from urllib.parse import urlencode
except ImportError:
    from urllib import urlencode

from wwpdb.utils.config.ConfigInfo import ConfigInfo, getSiteId

logger = logging.getLogger()


class MessageQueueConnection(object):
    def __init__(self):
        self._siteId = getSiteId(defaultSiteId=None)
        self._cI = ConfigInfo(self._siteId)

    def _getDefaultConnectionUrl(self):
        """Provide the connection URL appropriate for the configured protocol.."""
        rbmqClientProtocol = self._cI.get("SITE_RBMQ_CLIENT_PROTOCOL", default="")
        if "SSL" in rbmqClientProtocol:
            return self._getSslConnectionUrl()
        else:
            return self._getConnectionUrl()

    def _getDefaultConnectionParameters(self):
        """Provide the connection parameters appropriate for the configured protocol.."""
        rbmqClientProtocol = self._cI.get("SITE_RBMQ_CLIENT_PROTOCOL", default="")
        if "SSL" in rbmqClientProtocol:
            return self._getSslConnectionParameters()
        else:
            return self._getConnectionParameters()

    def _getSslConnectionParameters(self):
        pObj, _url = self.__getSslConnectionParameters()
        return pObj

    def _getSslConnectionUrl(self):
        _pObj, url = self.__getSslConnectionParameters()
        return url

    def __getSslConnectionParameters(self):
        """Return connection parameter object for SSL client connection -"""
        parameters = None
        rbmqUrl = None
        try:
            rbmqServerHost = self._cI.get("SITE_RBMQ_SERVER_HOST")
            rbmqServerPort = self._cI.get("SITE_RBMQ_SSL_SERVER_PORT")
            rbmqUser = self._cI.get("SITE_RBMQ_USER_NAME")
            rbmqPassword = self._cI.get("SITE_RBMQ_PASSWORD")
            rbmqVirtualHost = self._cI.get("SITE_RBMQ_VIRTUAL_HOST")
            clientSslCaCertFile = self._cI.get("SITE_RBMQ_SSL_CA_CERT_FILE")
            clientSslKeyFile = self._cI.get("SITE_RBMQ_SSL_KEY_FILE")
            clientSslCertFile = self._cI.get("SITE_RBMQ_SSL_CERT_FILE")
            ssl_opts = urlencode({"ssl_options": {"ca_certs": clientSslCaCertFile, "keyfile": clientSslKeyFile, "certfile": clientSslCertFile}})
            rbmqUrl = "amqps://%s:%s@%s:%d/%s?%s" % (rbmqUser, rbmqPassword, rbmqServerHost, int(rbmqServerPort), rbmqVirtualHost, ssl_opts)
            logger.debug("rbmq URL: %s ", rbmqUrl)
            parameters = pika.URLParameters(rbmqUrl)
        except Exception as _e:  # noqa: F841
            logger.exception("Failing")

        return parameters, rbmqUrl
        #

    def _getConnectionParameters(self):
        """Return connection parameters for the standard TCP client connection --"""
        pObj, _url = self.__getConnectionParameters()
        return pObj

    def _getConnectionUrl(self):
        """Return connection parameters as a URL for the standard TCP client connection"""
        _pObj, url = self.__getConnectionParameters()
        return url

    def __getConnectionParameters(self):
        """Return connection parameter object for client connection using basic authentication."""
        parameters = None
        try:
            rbmqServerHost = self._cI.get("SITE_RBMQ_SERVER_HOST")
            rbmqServerPort = self._cI.get("SITE_RBMQ_SERVER_PORT")
            rbmqUser = self._cI.get("SITE_RBMQ_USER_NAME")
            rbmqPassword = self._cI.get("SITE_RBMQ_PASSWORD")
            rbmqVirtualHost = self._cI.get("SITE_RBMQ_VIRTUAL_HOST")

            credentials = pika.PlainCredentials(rbmqUser, rbmqPassword)
            parameters = pika.ConnectionParameters(host=rbmqServerHost, port=int(rbmqServerPort), virtual_host=rbmqVirtualHost, credentials=credentials)
            rbmqUrl = "amqp://%s:%s@%s:%d/%s" % (rbmqUser, rbmqPassword, rbmqServerHost, int(rbmqServerPort), rbmqVirtualHost)
            logger.debug("rbmq URL: %s ", rbmqUrl)
        except Exception:
            logger.exception("Failing")

        return parameters, rbmqUrl
