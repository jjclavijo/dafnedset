"""
Monkey pathch all Base clases used in dataset transformation and augmentation.
adding methods for easy representation of transformations as
data.transform().etc...
"""

from ..base import BatchBuffer,FeededBuffer,MultiFeededBuffer,SyncdBuffer,\
                   Repeater,Cacher,CachedLoader,Loader

from ..datasets import DefaultQuerys

from ..tools.patch import toolify as add_tools

classes_to_patch = BatchBuffer,FeededBuffer,MultiFeededBuffer,SyncdBuffer,\
                   Repeater,Cacher,CachedLoader,Loader,\
                   DefaultQuerys,\

for cls in classes_to_patch:
    add_tools(cls)
    #TODO: verify containerization patching is already applied
    add_tools(cls.Contained)
