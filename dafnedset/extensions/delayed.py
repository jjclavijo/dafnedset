"""
DelayedInt parameters

Using those for parameters instead of plain ints implies
Catching ValueErrors for unset Delayed parameters.

This is part of a brute-force reimplementation of lazyness, from the times
when i didn't knew Haskell.
"""

from .contained import acumulable,multifun

import logging
log = logging.getLogger(__name__)

def delayable(fun):
    def wrapper(num,*args,**kwargs):
        if num.value is None:
            #raise ValueError('PromisedInt not set yet')
            return PromisedResult(fun,num,*args)
        else:
            return fun(num.value,*args,**kwargs)

    wrapper.__name__ = fun.__name__
    new_name = wrapper.__qualname__
    new_name = '.'.join(new_name.split('.')[:-2])
    new_name = '.'.join([new_name,'delayed_wrapper',fun.__name__])
    wrapper.__qualname__ = new_name

    log.debug('creating {}'.format(new_name))

    return wrapper

class DelayedInt():
    _target_methods = {'__add__':int.__add__,
                       '__radd__':int.__radd__,
                       '__sub__':int.__sub__,
                       '__rsub__':int.__rsub__,
                       '__mul__':int.__mul__,
                       '__rmul__':int.__rmul__,
                       '__floordiv__':int.__floordiv__,
                       '__truediv__':int.__truediv__,
                       '__rfloordiv__':int.__rfloordiv__,
                       '__rtruediv__':int.__rtruediv__,
                       '__divmod__':int.__divmod__,
                       '__rdivmod__':int.__rdivmod__,
                       '__mod__':int.__mod__,
                       '__rmod__':int.__rmod__,
                       '__ge__':int.__ge__,
                       '__lt__':int.__lt__,
                       '__gt__':int.__gt__,
                       '__le__':int.__le__,
                       '__ne__':int.__ne__,
                       '__eq__':int.__eq__}

    def __init_subclass__(cls,**kwargs):
        log.debug('decorating {}'.format(cls.__name__))
        for m,f in cls._target_methods.items():
            if callable(f):
                # Decorate functions to be:
                # @acumulable
                # @delayable
                # def method....
                method = acumulable(delayable(f))
                #try:
                #    getattr(m,cls)
                #except TypeError:
                #    setattr(cls,m,method)
                setattr(cls,m,method)

class PromisedInt(DelayedInt):
    def __init__(self):
        self.value = None

    @acumulable
    def set(self, value):
        try:
            self.value = int(value)
            if abs(value) % 1 > 0:
                log.warning('PromisedInt casting float to int')
        except ValueError:
            raise

    @acumulable
    def eval(self):
        if self.value is None:
            #raise ValueError('PromisedInt not set yet')
            return self
        else:
            return self.value

    @acumulable
    def __int__(self):
        value = self.eval()
        if isinstance(value,DelayedInt):
            raise ValueError('Promise not fullfilled')
        return value

class PromisedResult(PromisedInt):
    def __init__(self,fun,*args):
        self.value = None
        self.promise = (fun,args)

    @acumulable
    def eval(self):
        fun,args = self.promise
        args = list(args)
        if hasattr(args[0],'eval'):
            args[0] = args[0].eval()
        if hasattr(args[1],'eval'):
            args[1] = args[1].eval()
        try:
            value = fun(*args)
            if not isinstance(value,DelayedInt) and \
               not value is NotImplemented:
                return value
            else:
                return self
        except TypeError as e:
            log.debug('{}'.format(e))
            return self

    @acumulable
    def __int__(self):
        value = self.eval()
        if isinstance(value,DelayedInt):
            raise ValueError('Promise not fullfilled')
        return int(value)

    @acumulable
    def __bool__(self):
        value = self.eval()
        if isinstance(value,DelayedInt):
            raise ValueError('Promise not fullfilled')
        return bool(value)


