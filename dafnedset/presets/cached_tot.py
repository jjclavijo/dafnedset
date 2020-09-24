import pydafne.data.batchers as ba
import pydafne.data.query_batch as qb
import pydafne.data.preprocessing as pp
from functools import partial

pos10 = qb.DefaultQuerys.positive()
pos10 = pp.Labeler(pos10,label=[1.,0.])
pos08 = pp.Labeler(pos10,label=[0.8,0.2])

pos10 = ba.FeededBuffer(pos10,batch_size=100000,force=True)
pos08 = ba.FeededBuffer(pos08,batch_size=100000,force=True)

pos_length = sum([len(i) for i in pos10]) #save length and feed buffer

neg10 = qb.DefaultQuerys.negative()
neg10 = pp.Labeler(neg10, label=[0.,1.])
neg10 = ba.FeededBuffer(neg10,batch_size=100000,force=True)

neg_length = sum([len(i) for i in neg10]) #save length and feed buffer

pos10x = ba.FeededBuffer(pos10,repeat=True,max_length=pos_length*4,batch_size=pos_length)

neg10x = ba.FeededBuffer(neg10,repeat=True,max_length=neg_length*4,batch_size=neg_length)

ch_mixer = pp.BatchProcessing.ch_mixer(times=1,p_true_pos=0.9,p_true_pos_ch=0.9)

pos_neg_mix = pp.PreProcessor(pos10x + neg10x,mix=[ch_mixer])

pos_mix = pp.PreProcessor(pos10x,mix=[ch_mixer])

neg_mix = pp.PreProcessor(neg10x,mix=[ch_mixer])

total = pos_mix + neg_mix + pos_neg_mix + \
        ba.FeededBuffer(pos08,repeat=False,max_length=pos_length,
                        batch_size=pos_length/4) + \
        ba.FeededBuffer(neg10,repeat=False,max_length=neg_length,
                        batch_size=neg_length/4)

cached_tot = qb.Cacher(total)


pos10x8 = ba.FeededBuffer(pos10,repeat=True,max_length=pos_length*8,batch_size=pos_length)

pos_mix8 = pp.PreProcessor(pos10x8,mix=[ch_mixer])

total_pos = pos10 + pos_mix8

cached_tot_p = qb.Cacher(total_pos)

cached_tot_p_n = qb.Cacher(pp.Labeler(total_pos,label=[0.,1.]))
