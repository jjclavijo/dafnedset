from ..transformations import Splitter,BatchProcessing
from ..base import SyncdBuffer
from ..extensions import Container
from .direct import preprocess

def scale(*args,**kwargs):
    kwargs['mix'] = [BatchProcessing.scaler()]
    return preprocess(*args,**kwargs)

def label(feeder,label,*args,**kwargs):
    args = [feeder,*args]

    label = label
    label_col_name = kwargs.get('label_col_name','etiqueta')

    labeler = BatchProcessing.labeler(label,label_col_name)
    kwargs['mix'] = [labeler]

    return preprocess(*args,**kwargs)

# Splitter is a Cacher because splits are random on the first run and repeated
# on the next.

def split(feeder,splits,batch_size=None):
    """
    Splits a Feeder and returns sibling SyncdFeeders for individual treatment
    of each split.
    """
    feeder = Splitter(feeder,splits)
    if batch_size is None:
        bsizes = feeder.out_bs
    elif isinstance(batch_size, Container):
        bsizes = batch_size
    else:
        bsizes = Container.pack([batch_size] * len(splits))

    result = []
    for i,bs in enumerate(bsizes):
        sibling = SyncdBuffer(feeder,i,batch_size=bs)
        for s in result:
            sibling.add_sibling(s,s.index)
        result.append(sibling)
    return Container.pack(result)

