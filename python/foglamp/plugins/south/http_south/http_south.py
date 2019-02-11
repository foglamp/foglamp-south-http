# -*- coding: utf-8 -*-

# FOGLAMP_BEGIN
# See: http://foglamp.readthedocs.io/
# FOGLAMP_END

"""HTTP Listener handler for sensor readings"""
import asyncio
import copy
import os
import ssl
import logging
from threading import Thread
from aiohttp import web

from foglamp.common import logger
from foglamp.common.web import middleware
from foglamp.plugins.common import utils
import async_ingest


__author__ = "Amarendra K Sinha"
__copyright__ = "Copyright (c) 2017 Dianomic Systems"
__license__ = "Apache 2.0"
__version__ = "${VERSION}"

_LOGGER = logger.setup(__name__, level=logging.INFO)
c_callback = None
c_ingest_ref = None
loop = None
t = None
_FOGLAMP_DATA = os.getenv("FOGLAMP_DATA", default=None)
_FOGLAMP_ROOT = os.getenv("FOGLAMP_ROOT", default='/usr/local/foglamp')

_CONFIG_CATEGORY_NAME = 'HTTP_SOUTH'
_CONFIG_CATEGORY_DESCRIPTION = 'South Plugin HTTP Listener'
_DEFAULT_CONFIG = {
    'plugin': {
        'description': 'HTTP Listener South Plugin',
        'type': 'string',
        'default': 'http_south',
        'readonly': 'true'
    },
    'host': {
        'description': 'Address to accept data on',
        'type': 'string',
        'default': '0.0.0.0',
        'order': '1',
        'displayName': 'Host'
    },
    'port': {
        'description': 'Port to listen on',
        'type': 'integer',
        'default': '6683',
        'order': '2',
        'displayName': 'Port'
    },
    'uri': {
        'description': 'URI to accept data on',
        'type': 'string',
        'default': 'sensor-reading',
        'order': '3',
        'displayName': 'URI'
    },
    'assetNamePrefix': {
        'description': 'Asset name prefix',
        'type': 'string',
        'default': 'http-',
        'order': '4',
        'displayName': 'Asset Name Prefix'
    },
    'enableHttp': {
        'description': 'Enable HTTP (Set false to use HTTPS)',
        'type': 'boolean',
        'default': 'true',
        'order': '5',
        'displayName': 'Enable Http'
    },
    'httpsPort': {
        'description': 'Port to accept HTTPS connections on',
        'type': 'integer',
        'default': '6684',
        'order': '6',
        'displayName': 'Https Port'
    },
    'certificateName': {
        'description': 'Certificate file name',
        'type': 'string',
        'default': 'foglamp',
        'order': '7',
        'displayName': 'Certificate Name'
    }
}


def plugin_info():
    return {
        'name': 'HTTP South Listener',
        'version': '1.5.0',
        'mode': 'async',
        'type': 'south',
        'interface': '1.0',
        'config': _DEFAULT_CONFIG
    }


def plugin_init(config):
    """Registers HTTP Listener handler to accept sensor readings

    Args:
        config: JSON configuration document for the South plugin configuration category
    Returns:
        handle: JSON object to be used in future calls to the plugin
    Raises:
    """
    handle = copy.deepcopy(config)
    return handle


def plugin_start(data):
    global loop, t
    _LOGGER.info("plugin_start called")

    loop = asyncio.new_event_loop()
    try:
        host = data['host']['value']
        port = data['port']['value']
        uri = data['uri']['value']

        http_south_ingest = HttpSouthIngest(config=data)
        app = web.Application(middlewares=[middleware.error_middleware], loop=loop)
        app.router.add_route('POST', '/{}'.format(uri), http_south_ingest.render_post)
        handler = app.make_handler(loop=loop)

        # SSL context
        ssl_ctx = None

        is_https = True if data['enableHttp']['value'] == 'false' else False
        if is_https:
            port = data['httpsPort']['value']
            cert_name = data['certificateName']['value']
            ssl_ctx = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
            cert, key = get_certificate(cert_name)
            _LOGGER.info('Loading TLS certificate %s and key %s', cert, key)
            ssl_ctx.load_cert_chain(cert, key)

        server_coro = loop.create_server(handler, host, port, ssl=ssl_ctx)
        future = asyncio.ensure_future(server_coro, loop=loop)

        data['app'] = app
        data['handler'] = handler
        data['server'] = None

        def f_callback(f):
            # _LOGGER.info(repr(f.result()))
            """ <Server sockets=
            [<socket.socket fd=17, family=AddressFamily.AF_INET, type=2049,proto=6, laddr=('0.0.0.0', 6683)>]>"""
            data['server'] = f.result()
        future.add_done_callback(f_callback)

        def run():
            global loop
            loop.run_forever()

        t = Thread(target=run)
        t.start()
    except Exception as e:
        _LOGGER.exception(str(e))


def plugin_reconfigure(handle, new_config):
    """ Reconfigures the plugin

    it should be called when the configuration of the plugin is changed during the operation of the South service;
    The new configuration category should be passed.

    Args:
        handle: handle returned by the plugin initialisation call
        new_config: JSON object representing the new configuration category for the category
    Returns:
        new_handle: new handle to be used in the future calls
    Raises:
    """
    global loop
    _LOGGER.info("Old config for HTTP south plugin {} \n new config {}".format(handle, new_config))

    # plugin_shutdown
    plugin_shutdown(handle)

    # plugin_init
    new_handle = plugin_init(new_config)

    # plugin_start
    plugin_start(new_handle)

    return new_handle


def _plugin_stop(handle):
    _LOGGER.info('Stopping South HTTP plugin.')
    global loop
    try:
        app = handle['app']
        handler = handle['handler']
        server = handle['server']
        if server:
            server.close()
            asyncio.ensure_future(server.wait_closed(), loop=loop)
            asyncio.ensure_future(app.shutdown(), loop=loop)
            asyncio.ensure_future(handler.shutdown(60.0), loop=loop)
            asyncio.ensure_future(app.cleanup(), loop=loop)
        loop.stop()
    except Exception as e:
        _LOGGER.exception(str(e))
        raise


def plugin_shutdown(handle):
    """ Shutdowns the plugin doing required cleanup, to be called prior to the South service being shut down.

    Args:
        handle: handle returned by the plugin initialisation call
    Returns:
    Raises:
    """
    _plugin_stop(handle)
    _LOGGER.info('South HTTP plugin shut down.')


def plugin_register_ingest(handle, callback, ingest_ref):
    """Required plugin interface component to communicate to South C server

    Args:
        handle: handle returned by the plugin initialisation call
        callback: C opaque object required to passed back to C->ingest method
        ingest_ref: C opaque object required to passed back to C->ingest method
    """
    global c_callback, c_ingest_ref
    c_callback = callback
    c_ingest_ref = ingest_ref


def get_certificate(cert_name):
    if _FOGLAMP_DATA:
        certs_dir = os.path.expanduser(_FOGLAMP_DATA + '/etc/certs')
    else:
        certs_dir = os.path.expanduser(_FOGLAMP_ROOT + '/data/etc/certs')

    cert = certs_dir + '/{}.cert'.format(cert_name)
    key = certs_dir + '/{}.key'.format(cert_name)

    if not os.path.isfile(cert) or not os.path.isfile(key):
        _LOGGER.warning("%s certificate files are missing. Hence using default certificate.", cert_name)
        cert = certs_dir + '/foglamp.cert'
        key = certs_dir + '/foglamp.key'
        if not os.path.isfile(cert) or not os.path.isfile(key):
            _LOGGER.error("Certificates are missing")
            raise RuntimeError

    return cert, key


class HttpSouthIngest(object):
    """Handles incoming sensor readings from HTTP Listener"""

    def __init__(self, config):
        self.config_data = config

    async def render_post(self, request):
        """Store sensor readings from http_south to FogLAMP

        Args:
            request:
                The payload block decodes to JSON similar to the following:

                .. code-block:: python

                    [ {
                        "timestamp": "2017-01-02T01:02:03.23232Z-05:00",
                        "asset": "pump1",
                        "key": "80a43623-ebe5-40d6-8d80-3f892da9b3b4",
                        "readings": {"humidity": 0.0, "temperature": -40.0}
                      },
                      ...
                    ]
        Example:
            curl -X POST http://localhost:6683/sensor-reading -d '[{"timestamp": "2017-01-02T01:02:03.23232Z-05:00",
                "asset": "pump1", "key": "80a43623-ebe5-40d6-8d80-3f892da9b3b4", "readings": {"humidity": 0.0, "temperature": -40.0}}]'
        """
        message = {'result': 'success'}
        try:
            try:
                payload_block = await request.json()
            except Exception:
                raise ValueError('Payload block must be a valid json')

            if type(payload_block) is not list:
                raise ValueError('Payload block must be a valid list')

            for payload in payload_block:
                asset = "{}{}".format(self.config_data['assetNamePrefix']['value'], payload['asset'])
                timestamp = payload['timestamp']
                key = payload['key']

                # HOTFIX: To ingest readings sent from foglamp sending process
                if not timestamp.rfind("+") == -1:
                    timestamp = timestamp + ":00"

                # readings or sensor_values are optional
                try:
                    readings = payload['readings']
                except KeyError:
                    readings = payload['sensor_values']  # sensor_values is deprecated

                # if optional then
                # TODO: confirm, do we want to check this?
                if not isinstance(readings, dict):
                    raise ValueError('readings must be a dictionary')

                data = {
                    'asset': asset,
                    'timestamp': timestamp,
                    'key': key,
                    'readings': readings
                }
                async_ingest.ingest_callback(c_callback, c_ingest_ref, data)
        except (KeyError, ValueError, TypeError) as e:
            _LOGGER.exception("%d: %s", web.HTTPBadRequest.status_code, str(e))
            raise web.HTTPBadRequest(reason=e)
        except Exception as ex:
            _LOGGER.exception("%d: %s", web.HTTPInternalServerError.status_code, str(ex))
            raise web.HTTPInternalServerError(reason=str(ex))

        return web.json_response(message)
