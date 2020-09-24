import socket
import logging
import asyncio
from urllib.parse import urlparse

from .handlers import _dict as handlers

from .._constants import SOCKET_FILE

log = logging.getLogger(__name__)

async def save_dataset(**kwargs):

    kwargs['save_cache']='True'

    log.info('Starting Conecction')    

    sock = socket.socket(socket.AF_UNIX,socket.SOCK_STREAM)
    sock.connect(SOCKET_FILE)
    readpt = sock.makefile('rb')

    request = 'cmd?serve_dataset&{}'.\
               format('&'.join(\
                      ['{}={}'.format(i,j)
                       for i,j in kwargs.items()]))

    log.info('Sending Request: {}'.format(request))    

    sock.send(request.encode('utf-8'))

    sock.close()

    return


async def handle_client(reader,writer):
    request = None
    print('socket conectado')

    keep_running = True

    try:
        while keep_running:
            request = urlparse(await reader.read(255))

            if reader.at_eof():
                print('Connection Closed')
                break
            else:
                handler = handlers.get(request.path.decode('utf-8'),
                                       handlers['dummy'])

            keep_running = await handler(request.query.decode('utf-8'),
                                         writer)

            await writer.drain()

    except ConnectionResetError:
        log.info('connection Reset by peer')

    except BrokenPipeError:
        log.info('Connection Closed Upstream')

    finally:    
        writer.close()

