
#Core Imports
from functools import partial
from math import ceil
import os.path
import logging

#External Imports
import pyarrow as pa
import pyarrow.parquet as pq
import numpy as np

#Local imports
from .helpers import * # a Class for iterable funcions
from .extensions import acumulable,PromisedInt,Container
from ._db_connect import default_conn
from ._constants import QUERY_SIZE,MAX_BATCHSIZE


log = logging.getLogger(__name__)


class BatchBuffer():
    def __init__(self,batch_size,*args,force=False,**kwargs):
        """
        batch_size: int or PromisedInt. cannot yield until set.
        force: yield all when asked for next if data length is 
               below batch_size
        """
        kwargs['force']=force

        self.callargs = ([batch_size,*args],kwargs)

        self.batch_size = batch_size
        self.force = force

        self.accum = []

    def __iter__(self):
        args,kwargs = self.callargs
        kwargs['force']=True

        nuevo = self.__class__(*args,**kwargs)

        #Copy data before iter.
        for i in self.accum:
            nuevo.push(i)

        return nuevo

    def __next__(self):
        return self.pull()

    def pull(self):
        if len(self.accum) > 1:
            self.accum = self.collapse()

        batch, accum = self.unfold()

        if batch is None:
            if self.force and self.accum:
                return self.accum.pop()
            raise StopIteration
        else:
            self.accum = accum
            return batch

    def push(self,batch):
        self.accum.append(batch)

    def collapse(self):
        if pa_check_cols(self.accum):
            log.debug('normalizando columnas')
            columns = pa_common_cols(self.accum)
            self.accum = pa_keep_cols(columns,self.accum)
            log.warning('columns: {}'.format(self.accum[0].schema.names))
        rebatched = pa.Table.\
                    from_batches(self.accum).\
                    combine_chunks().\
                    to_batches()
        return rebatched

    def unfold(self):
        #MODIF
        try:
            if len(self.accum) != 0 and \
               len(self.accum[0]) >= self.batch_size:
                this,*other = self.accum
                
                this_a,*this_b = (this.slice(0,self.batch_size),
                                 this.slice(self.batch_size))

                if sum([len(i) for i in this_b]) == 0:
                    this_b = []

                return this_a, [*this_b, *other]
        except ValueError:
            # ValueError will be raised if self.batch_size is 
            # not Set yet
            pass

        return None, self.accum

    @acumulable
    def __add__(self,other):
        feeders = []
        for feeder in (self, other):
            if isinstance(feeder,MultiFeededBuffer):
                feeders.extend(feeder.feeder)
            else:
                feeders.append(feeder)
        return MultiFeededBuffer(feeders)


class FeededBuffer(BatchBuffer):
    def __init__(self,feeder,batch_size,*args,force=False,
                 repeat=False,max_length=None,**kwargs):
        """
        feeder: any iterable.
        batch_size: int or PromisedInt
        max_length: int or PromisedInt limit before reset feeder.
        """
        super(FeededBuffer,self).__init__(batch_size,force=force)

        kwargs['force'] = force
        kwargs['repeat'] = repeat
        kwargs['max_length'] = max_length

        self.callargs = ([feeder,batch_size,*args],kwargs)

        self.repeat = repeat
        self.feeder = feeder
        self.count = 0
        self.max_length = max_length
        
        if hasattr(feeder,'length'):
            self.length = feeder.length
        else:
            self.length = PromisedInt()

    def __iter__(self):
        #Returns a copy of itself FORCED for exhaustion if needed.
        # Note that simoultaneos force and repeat takes no efect.
        # Setting callargs no need to override __iter__ for descendant.
        args,kwargs = self.callargs
        kwargs['force'] = True

        return self.__class__(*args,**kwargs)

    def pull(self):
        #MODIF
        try:
            if self.max_length is not None \
                    and self.count >= self.max_length:
                raise StopIteration
        except ValueError:
            pass


        if len(self.accum) > 1:
            self.accum = self.collapse()

        batch, accum = self.unfold()

        if batch is None:
            try:
                self.feed_next()
            except StopIteration:
                if self.force and self.accum:
                #MODIF
                    batch = self.accum.pop()
                    self.count += len(batch)
                    
                    if isinstance(self.length,PromisedInt):
                        self.length.set(self.count)
                    
                    return batch
                    
                raise StopIteration

            return self.pull()
            # Unhandled StopIteration from pull
            # means eshausted feeder

        else:
            self.accum = accum
            self.count += len(batch)

            #MODIF
            try:
                if self.max_length is not None \
                        and self.count > self.max_length:
                    rem = self.max_length - self.count
                    batch,*_ = batch.slice(0,rem)
                    #self.accum = []
            except ValueError:
                pass

            return batch

    def feed_next(self):
        if not hasattr(self,'food'): # Si tengo hambre
            self.food = iter(self.feeder) # Me alimento.
        try:
            self.push(next(self.food))
        except StopIteration:
            if self.repeat:
                log.warning('Repeating')
                #self.food.close()
                self.food = iter(self.feeder)
                self.feed_next()
                return
            raise StopIteration

    def feed_all(self):
        repeat_state = self.repeat
        try:
            self.repeat = False
            while True:
                self.feed_next()
        except StopIteration:
            pass
        finally:
            self.repeat = repeat_state

class MultiFeededBuffer(FeededBuffer):

    def __init__(self,feeder,*args,force=False,repeat=False,max_length=None,**kwargs):

        self.repeat = repeat
        self.feeder = feeder

        if not isinstance(self.feeder,list):
            raise TypeError('feeder argument expects a list')

        try:
            batch_size = sum([i.batch_size for i in feeder])
        except AttributeError:
            batch_size = MAX_BATCHSIZE #???

        # Init simple Buffer
        super(FeededBuffer,self).__init__(batch_size,force=force)
        
        # Save Callargs for __iter__
        kwargs['force']=force
        kwargs['repeat']=repeat
        kwargs['max_length']=max_length
        self.callargs = ([feeder,*args],kwargs)

        self.count = 0
        self.max_length = max_length

        has_len = [hasattr(f,'length') for f in feeder]

        if all(has_len):
            self.length = sum([f.length for f in feeder])
        else:
            self.length = PromisedInt()

    def feed_next(self):
        if not hasattr(self,'food'): # Si tengo Hambre:
            self.food = [iter(i) for i in self.feeder] # Me alimento.
        try:
            for portion in self.food:
                self.push(next(portion))
        except StopIteration:
            if self.repeat:
                log.warning('Repeating')
                #self.food.close()
                self.food = [iter(i) for i in self.feeder]
                self.feed_next()
                return
            raise StopIteration

class SyncdBuffer(FeededBuffer):
    """
    A Feeded Buffer whose feeder returns a list of batches instead of 
    single batches. 
    Each SyncdBuffer consumes only one position on the list, but can 
    Sync with other sibling SyncdBuffers consuming the other positions.
    """
    def __init__(self,feeder,index,*args,force=True,max_length=None,**kwargs):
        self.feeder = feeder 

        batch_size = kwargs.get('batch_size',feeder.length)

        super(FeededBuffer,self).__init__(batch_size,force=force)

        ## Save Callargs for __iter__ not usefull, syncdbuffers are discarded
        #kwargs['force']=force
        #kwargs['max_length']=max_length
        #self.callargs = ([feeder,index,*args],kwargs)

        # Never repeat
        self.repeat = False
        self.exhausted = False
        self.index = index
        self.count = 0
        self.max_length = max_length
        self.siblings = {index:self}

        #MODIF
        if hasattr(feeder,'length'):
            try:
                self.length = feeder.length[self.index]
            except TypeError:
                self.length = feeder.length
        else:
            self.length = PromisedInt()

    def add_sibling(self,sibling,index):

        if index in self.siblings:
            # Si ya existe el hermano
            if self.siblings[index] is not sibling:
                # Pero no coincide
                raise ValueError('sibling has another sibling with index {}'.format(index))
                # ERROR
        else:
            # Si no existe lo agrego.
            self.siblings[index] = sibling

        for ix,sib in self.siblings.items():
            # Para cada hermano (incluyendome)
            if ix in sibling.siblings:
                # Si el hermano que estoy agregando lo tiene
                if sibling.siblings[ix] is not sib:
                    # Pero no coincide.
                    raise ValueError('sibling has another sibling with index {}'.format(self.index))
                    # Error.
            else:
                # Si no lo tiene se lo agrego.
                sibling.add_sibling(self,self.index)

    def __iter__(self):
        """
        SyncdBuffers are syncd, thus can be iterated only once.
        if you want a repeatible instance, use a Cacher.
        """
        return self

    def feed_next(self):
        if self.exhausted:
            raise StopIteration
            # Never, NEVER should iteration continue
            # after first feeder iteration

        if not hasattr(self,'food'): # Si tengo Hambre:
            food = iter(self.feeder) # Me alimento.
            for ix,sibling in self.siblings.items():
                sibling.food = food
        try:
            portion = next(self.food)
            for ix,sibling in self.siblings.items():
                sibling.push(portion[ix])
                #TODO: index should be optional

        except StopIteration:
#            if self.exhausted:
#                raise ValueError('SyncdBuffers are syncd, thus can be iterated \
#                only once. if you want a repeatible instance, use a Cacher.')
            log.warning('Splited section exhausted')
            self.set_exhausted()
            raise
#            if self.repeat:
#                log.warning('Repeating')
#                #self.food.close()
#                food = iter(self.feeder) # Me alimento.
#                for ix,sibling in self.siblings.items():
#                    sibling.food = food
#                self.feed_next()
#                return
#            raise StopIteration
    def set_exhausted(self):
        for k,s in self.siblings.items():
            s.exhausted=True


class Repeater(object):
    def __init__(self,feeder,*args,**kwargs):
        self.feeder = feeder
        self.callargs = ([feeder,*args],kwargs)

    def __next__(self):
        if not hasattr(self,'food'): # Si tengo hambre
            self.food = iter(self.feeder) # Me alimento.
        try:
            return next(self.food)
            
        except StopIteration:
            self.reset = True
            raise StopIteration

    def __iter__(self):
        """
        generating a new iterator will reset iteration.
        it makes for loops predictable.
        """
        args,kwargs = self.callargs
        return self.__class__(*args,**kwargs)

    def __call__(self):
        return self

class Cacher():
    def __init__(self,feeder,**kwargs):
        
        self.feeder = feeder
        self.buffer = []
        self.exhausted = False
        self.iterating = False
        self.length = PromisedInt()
        self.count = 0

    def __next__(self):
        if not self.iterating:
            self.iterating = True
            self.food = iter(self.feeder)

        if not self.exhausted:
            try:
                batch = next(self.food)
                self.buffer.append(batch)

                self.count += len(batch) #modif

                return batch

            except StopIteration:
                self.exhausted = True
                self.feeder = self.buffer
                self.length.set(self.count) #modif
                raise StopIteration
        else:
            return next(self.food)

    def __iter__(self):
        """
        generating a new iterator will reset iteration.
        it makes for loops predictable.
        """
        if not self.exhausted:
            if not self.iterating and \
                    hasattr(self,'food'):
                log.warning("""When building data chains with cachers, shure that
                               cachers have either a single children branch or
                               are fully iterated on the first time.""")

            if self.buffer:
                log.warning('not fully iterated, resetting cache')
                self.buffer = []

            self.iterating = False

            return self

        else:
            return Repeater(self.feeder)

    def __call__(self):
        return self
        
def yield_batched_query(q, query, query_size=QUERY_SIZE,
                        conn=None, label=None,**kwargs):
    if conn is None:
        conn = default_conn()
        to_close=True
    else:
        to_close=False

    with conn.cursor() as curs:
        curs.execute(q)
        q1 = curs.fetchall()
        maxii = q1[0][0]
        log.info('query going to {}'.format(maxii))
        top = 1

        while top < maxii:
            top = top + query_size
            curs.execute(query,vars=[top - query_size,top])

            many = curs.fetchall()
            how_many = len(many)
            if how_many > 0:
                rb = pa.RecordBatch.from_arrays([*zip(*many)],
                     names=['est','iid','ep','t','norte','este','altura'])
                #TODO: Names as constant
                yield rb

    if to_close:
        conn.close()

class CachedLoader(Cacher):

    @classmethod
    def read_db(cls,q,query,**kwargs):
        gen = partial(yield_batched_query,
                                q,query,**kwargs)
        return cls(FuncGen(gen),**kwargs)

    @classmethod
    def read_parquet(cls,parquet_path,**kwargs):
        try:
            open(parquet_path,'r').close()
        except OSError:
            raise

        def gen():
            parq = pq.ParquetFile(parquet_path)
            for i in range(parq.num_row_groups):
                yield parq.read_row_group(i).to_batches()[0]
            
        return cls(FuncGen(gen),**kwargs)

class CachedSaver(Cacher):

    def write_parquet(self,parquet_path):
        gen = self.feeder
        init = None

        for batch in gen:
            if init is None:
                init = True
                parq = pq.ParquetWriter(parquet_path,batch.schema)

            parq.write_table(pa.Table.\
                             from_batches([batch])
                             )
        parq.close()

class Loader(Repeater):

    @classmethod
    def read_db(cls,q,query,**kwargs):
        gen = partial(yield_batched_query,
                                q,query,**kwargs)
        return cls(FuncGen(gen),**kwargs)

    @classmethod
    def read_parquet(cls,parquet_path,**kwargs):
        try:
            open(parquet_path,'r').close()
        except OSError:
            raise

        def gen():
            parq = pq.ParquetFile(parquet_path)
            for i in range(parq.num_row_groups):
                yield parq.read_row_group(i).to_batches()[0]
            
        return cls(FuncGen(gen),**kwargs)

class Saver(Repeater):

    def write_parquet(self,parquet_path):
        gen = self.feeder
        init = None

        for batch in gen:
            if init is None:
                init = True
                parq = pq.ParquetWriter(parquet_path,batch.schema)

            parq.write_table(pa.Table.\
                             from_batches([batch])
                             )
        parq.close()
