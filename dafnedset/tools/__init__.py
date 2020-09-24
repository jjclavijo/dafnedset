"""
This "tools" submodule gathers all transformer functions which can
be applyed to any data-stream-ish.

preprocess,feed,cache are wrappers on feeder Clasess (see direct.py)
when called as functions, new instances of the base clases are created,
and the data is feeded inside those.

scale and label are based on the PreProcessor class.

split, operates on a single stream, creating a Contained group of streams
(see Container Class in ../extensions...)

All functions that are patchable, are added as methods to each Feeder class on
initialization of the main package (by monkey patching). Then we can use dot
notation to apply transformations, like data.split(..).scale(...) etc.

"""

from .direct import pack,preprocess,feed,cache
from .more import scale,label,split

patchable = [preprocess,feed,cache,scale,label,split]
