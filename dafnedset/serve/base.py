#Core Imports
import logging
from functools import partial
from io import BytesIO

#External imports
import numpy as np
import pyarrow as pa
import socket

log = logging.getLogger(__name__)

BATCH_SIZE=500

class DataServer(object):
    def __init__(self):
        """
        Avoid direct construction
        """
        return None

    @classmethod
    def from_batch_generator(cls, generator, **kwargs):
        """
        Create instance from a generator returning batches.
        """
        server = cls(**kwargs)
        server.data = generator
        return server

    @classmethod
    def from_pandas(cls, dataset, **kwargs):
        """
        Create instance from pandas DataFrame.
        """
        server = cls(**kwargs)
        server.data = PreProcess.prepare_pandas(dataset,**kwargs)
        return server

    async def serve_data(self,write_stream):
      # Create the socket
    
      bytes_buffer = BytesIO()

      writer = None

      try:
          for batch in self.data:
            # Initialize the pyarrow writer on first batch
            if writer is None:
              writer = pa.RecordBatchStreamWriter(bytes_buffer, batch.schema)

            # Write the batch to the client stream
            writer.write_batch(batch)

            bytes_buffer.seek(0)
            write_stream.write(bytes_buffer.read())
            bytes_buffer.seek(0)
            bytes_buffer.truncate()
            
            log.info('Written Batch')

            await write_stream.drain()

      # Probablemente no sea este el error.
      except (socket.error, BrokenPipeError) as e:
        log.info("{}, Dataset Connection Closed".format(e)) 

      # Cleanup client connection
      finally:
        if writer is not None:
          writer.close()

        bytes_buffer.close()
        write_stream.close()
        await write_stream.wait_closed()

        log.info('Connection Closed')
