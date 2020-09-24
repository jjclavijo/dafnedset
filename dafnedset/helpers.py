import pyarrow as pa

class FuncGen(object):
    def __init__(self,func):
        self.func = func
    def __iter__(self):
        return self.func()

def pa_common_cols(batches):
    if len(batches) > 1:
        return pa_common_cols(batches[:1]) & pa_common_cols(batches[1:])
    else:
        return set(batches[0].schema.names)

def pa_keep_cols(cols,batches):
    if batches:
        b = batches[0]
        return [ pa.RecordBatch.from_pandas(b.to_pandas().loc[:,cols]),
                 *pa_keep_cols(cols,batches[1:])]
    else:
        return []

def pa_check_cols(batches):
    if len(batches) > 1:
        if set(batches[0].schema.names) - set(batches[1].schema.names) or\
           set(batches[1].schema.names) - set(batches[0].schema.names):
            return True
        else:
            return pa_check_cols(batches[1:])
    else:
        return False

