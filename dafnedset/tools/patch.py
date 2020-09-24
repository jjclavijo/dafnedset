from . import patchable

def toolify(cls):
    for i in patchable:
        setattr(cls,i.__name__,i)
