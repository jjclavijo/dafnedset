from functools import partial

from .pos10 import data as pos10
from .neg10 import data as neg10
from .pos08 import data as pos08

from .. import transformations as pp
from ..tools import pack

pos10x = pos10.feed(repeat=True,max_length=pos10.length*4,
                    batch_size=pos10.length) #TODO implement pos10*4 sintactic
                                             #     sugar, maybe pos10/4 too.
neg10x = neg10.feed(repeat=True,max_length=neg10.length*4,
                    batch_size=neg10.length)

ch_mixer = pp.BatchProcessing.ch_mixer(times=1,p_true_pos=0.9,p_true_pos_ch=0.9)
rotator = pp.BatchProcessing.random_rot_dir()

to_mix = pack([pos10x,neg10x,pos10x + neg10x])

mixed = to_mix.preprocess(mix=[ch_mixer,rotator])

data = mixed.sum() + pos08.feed(repeat=False,max_length=pos10.length,
                               batch_size=pos10.length//4) +\
                    neg10.feed(repeat=False,max_length=neg10.length,
                               batch_size=neg10.length//4)

