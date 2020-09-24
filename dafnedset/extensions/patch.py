"""
Monkey patch classes 

patch_contained to create contained version of classes
Simply wrapping a list and exposing @acumulable methods,
those methods will apply one-to-one mappings of the 
underlaying methods.
"""

import logging
log = logging.getLogger(__name__)

from .contained import Container

def contained(cls,*args):

    name = 'Contained{}'.format(cls.__name__)
    log.debug('_Containable:decorating {}'.format(name))
    base = (Container,)
    attr = {'contained_class':cls,'decorate_extra':args}
    contained_version = type(name,base,attr)

    def repeat(self,times):
        return Container.pack([self]*times)

    setattr(cls,'repeat',repeat)
    setattr(cls,'Contained',contained_version)

    return cls

#_monky_patch_contained(PromisedInt)
#_monky_patch_contained(PromisedResult)

