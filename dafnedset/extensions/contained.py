"""
Implement extensions for
and Contained.... Classes.

On the base clases the only needed modifications are
the use of @acumulable decorators for methods that will be available on
Contained... Classes.

TODO: Extend documentation
"""

import logging

log = logging.getLogger(__name__)

def acumulable(fun):
    log.debug('Make {} acumulable'.format(fun.__name__))
    fun.is_acumulable=True
    return fun

def is_acumulable(fun):
    try:
        return fun.is_acumulable
    except AttributeError:
        return False

def multifun(wfun):
    def wrapper(self,*args):
        dargs = []
        for arg in args:
            try:
                if len(arg) != len(self):
                    raise ValueError('Arguments unaligned: {} != {}'.\
                                     format( len(arg),len(self) ))
                else:
                    dargs.append(arg)
            except (AttributeError,TypeError):
                dargs.append( [arg]*len(self) )

        if dargs:
            out = [wfun(s,*args) for s,args in zip(self,zip(*dargs))]
        else:
            out = [wfun(s) for s in self]

        try:
            return Container.pack(out)
        except AttributeError:
            return out

    wrapper.__name__ = wfun.__name__
    new_name = wrapper.__qualname__
    new_name = '.'.join(new_name.split('.')[:-2])
    new_name = '.'.join([new_name,'multiplied',wfun.__name__])
    wrapper.__qualname__ = new_name

    log.debug('creating {}'.format(new_name))
    log.debug('creating {}'.format(wrapper.__repr__()))

    return wrapper

class Container(object):
    contained_class = object
    def __init__(self,arglist,**kwargs):
        self.list = arglist
        try:
            self.length = Container.pack([i.length for i in arglist])
        except AttributeError:
            pass
    def __len__(self):
        return self.list.__len__()
    def __str__(self):
        return self.list.__str__()
    def __repr__(self):
        return self.list.__repr__()
    def __getitem__(self,index):
        return self.list.__getitem__(index)
    def sum(self):
        try:
            cum = self[0]
            for other in self[1:]:
                cum += other
            return cum
        except TypeError as e:
            log.warning(e)
            return NotImplemented

    @multifun
    def __add__(self,other):
        return self + other

    @multifun
    def __radd__(self,other):
        return self + other

    @classmethod
    def pack(cls,objects):
        classes_list = [o.__class__ for o in objects]
        baseclass = object
        classes = set(classes_list.pop().mro())

        # Drop not common clases
        for c in classes_list:
            classes = classes.intersection(c.mro())

        # Get closer common parent
        for c in classes:
            if issubclass(c,baseclass):
                baseclass = c

        try:
            return baseclass.Contained(objects)
        except AttributeError:
            return Container(objects)


    def __init_subclass__(cls,**kwargs):
        if 'contained_class' in kwargs:
            log.debug('{} replaces {}'.\
                        format(kwargs.get('contained_class'),cls.contained_class))
            cls.contained_class = kwargs.get('contained_class')

        log.debug('Container:decorating {}'.format(cls.__name__))
        refer_funcs = {}
        for c in cls.contained_class.mro():
            for k,v in c.__dict__.items():
                if not is_acumulable(v) and \
                   not k in cls.decorate_extra:
                    continue
                if k in refer_funcs:
                    continue
                log.debug('{}:{}'.format(k,v))
                refer_funcs[k] = v

        for name,wfun in refer_funcs.items():

            log.debug('wrapping multiple function {}'.format(wfun))

            multi = multifun(wfun)

            setattr(cls,name,multi)

