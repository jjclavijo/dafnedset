from ..monkey import patch_contained,patch_add_tools

from .neg10 import parts as parts_neg
from .pos08 import parts as parts_pos

_, test_pos, val_pos, show_pos = parts_pos
_, test_neg, val_neg, show_neg = parts_neg

_, test_tot,val_tot,show_tot = parts_pos+parts_neg
_, test_tot_n,val_tot_n,show_tot_n = (parts_pos+parts_neg).label(label=[0.,1.])
_, test_pos_n, val_pos_n, show_pos_n = (parts_pos).label(label=[0.,1.])

