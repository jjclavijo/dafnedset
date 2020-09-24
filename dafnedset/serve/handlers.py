import os
import asyncio
from ast import literal_eval
from importlib import import_module
from io import BytesIO
from urllib.parse import parse_qs

import logging

log = logging.getLogger(__name__)

import numpy as np
import pyarrow as pa
import pandas as pd

from .base import DataServer
from ..base import CachedLoader,CachedSaver

from .._constants import DATOS_PATH,BATCH_SIZE

dataset_cache = {}

async def handle_cmd(cmd,writer,*args):

    print('Handling Command {}'.format(cmd))

    cmd,*opts = cmd.split('&')

    if cmd == 'serve_dataset':

        opts = parse_qs('&'.join(opts))
        #keep only first element,
        opts = {i:j[0] for i,j in opts.items()}

        # A Partir de Aqui debería ser parte de base
        batch_size = literal_eval(opts.get('batch',str(BATCH_SIZE))),
        repeat = literal_eval(opts.get('repeat','False'))
        data_name = opts.get('name',False)

        if not data_name:
            raise ValueError('Should Specify Dataset Name')
        else:
            if literal_eval(opts.get('cached','False')):
                dset_files = os.listdir(DATOS_PATH)
                dset_files = [ os.path.splitext(i)[0] for i in dset_files\
                                 if os.path.splitext(i)[1] == '.dset']
                cache = {**{i:i for i in dset_files},**dataset_cache}
            else:
                cache = {}

            if data_name in cache:
                if isinstance(cache[data_name],str):
                    log.info('Using File Cached Version')
                    data = CachedLoader.read_parquet(
                            os.path.join(DATOS_PATH,'{}.dset'.format(data_name))
                            )
                    dataset_cache[data_name] = data
                else:
                    log.info('Using Memory Cached Version')
                    data = cache[data_name]
            else:
                # TODO This doesn't has sense anymore since all imports on
                # presets are delayed operations, dont consume resources until
                # iterated.
                try:
                    dset = import_module('.{}'.format(data_name),'pydafne.data.presets')
                    data = dset.data
                except ModuleNotFoundError:
                    psets = import_module('.presets','pydafne.data')
                    data = psets.__dict__[data_name]

                if literal_eval(opts.get('cached','False')):
                    dataset_cache[data_name] = data

            if literal_eval(opts.get('save_cache','False')):
                saver = CachedSaver(data)
                saver.write_parquet(
                        os.path.join(DATOS_PATH,'{}.dset'.format(data_name))
                        )

            if literal_eval(opts.get('serve','True')):
                server = DataServer.from_batch_generator(data)

                await server.serve_data(writer)
        # Hasta Aqui debería ser parte de base

    if cmd == 'quit':
        return False

    return True


async def handle_dummy(*args):
    return True

async def handle_quit(*args):
    return False

_dict = { 'cmd':handle_cmd,
          'quit':handle_quit,
          'dummy':handle_dummy,
          }
