#!/home/javier/envs/tmppsql/bin/python

# Core imports
import os
import asyncio
import warnings
import logging

# External imports
from .tasks import save_dataset,handle_client

logging.basicConfig()
logging.getLogger().setLevel(logging.INFO)

log = logging.getLogger(__name__)

warnings.simplefilter(action='ignore', category=FutureWarning)

ENV_DEFAULTS= { 'DATOS':'/datos', 'SI_BASE':'sismoident','SI_USER':'postgres',
                'SI_HOST':'localhost','SI_PORT':'5432','SI_PASS':'docker',
                'PGPASSFILE':'/.pgpass','SI_SOCKDIR':'/sockets/'}

SOCKET_DIR = os.environ.get('SI_SOCKDIR',ENV_DEFAULTS.get('SI_SOCKDIR'))

CMD_SOCKET = os.path.join(SOCKET_DIR,'sidb_cmd')

def parse_args():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--cache', action='append', default=[])
    return parser.parse_args()

def main():
    loop = asyncio.get_event_loop()
    loop.create_task(asyncio.start_unix_server(handle_client, CMD_SOCKET))

    args = parse_args()
    if args.cache:
        log.info('Will Cache {}'.format(args.cache))

    for name in args.cache:
        loop.create_task(save_dataset(name=name))

    loop.run_forever()
