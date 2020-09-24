import os

ENV_DEFAULTS= { 'DATOS':'/datos', 'SI_BASE':'sismoident','SI_USER':'postgres',
                'SI_HOST':'localhost','SI_PORT':'5432','SI_PASS':'docker',
                'PGPASSFILE':'/.pgpass','SI_SOCKDIR':'/sockets/'}


DB_PARAMS = (
             os.environ.get('SI_HOST',ENV_DEFAULTS.get('SI_HOST')),
             os.environ.get('SI_PORT',ENV_DEFAULTS.get('SI_PORT')),
             os.environ.get('SI_USER',ENV_DEFAULTS.get('SI_USER')),
             os.environ.get('SI_PASS',ENV_DEFAULTS.get('SI_PASS')),
             os.environ.get('SI_BASE',ENV_DEFAULTS.get('SI_BASE'))
            )

CONN_STRING = 'host={} port={} user={} password={} dbname={}'.\
              format(*DB_PARAMS)

DATOS_PATH = os.environ.get('DATOS',ENV_DEFAULTS.get('DATOS'))

DEFAULT_DATA_PATH = 'casos.parquet'

QUERY_SIZE = 2000
MAX_BATCHSIZE=20000
BATCH_SIZE=500

#For Save_dataset
SOCKET_DIR = os.environ.get('SI_SOCKDIR',ENV_DEFAULTS.get('SI_SOCKDIR'))
SOCKET_FILE = os.path.join(SOCKET_DIR,'sidb_cmd')

