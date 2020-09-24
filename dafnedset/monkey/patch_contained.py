"""
This is for enabling "Contained" versions of clasess.

Any

Feed -> params -> Food

process can be represented as

Feed.Contained(*n) -> params.Contained(*n) -> Food.Contained(*n)

"""


from ..base import BatchBuffer,FeededBuffer,MultiFeededBuffer,SyncdBuffer,\
                   Repeater,Cacher,CachedSaver,CachedLoader,Loader,Saver
from ..datasets import DefaultQuerys
from ..extensions.delayed import PromisedInt,PromisedResult
from ..extensions.patch import contained as patch_contained

classes_to_patch = BatchBuffer,FeededBuffer,MultiFeededBuffer,SyncdBuffer,\
                   Repeater,Cacher,CachedSaver,CachedLoader,Loader,Saver,\
                   DefaultQuerys,\
                   PromisedInt,PromisedResult

for cls in classes_to_patch:
    patch_contained(cls)
