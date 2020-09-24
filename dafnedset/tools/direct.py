from ..transformations import PreProcessor
from ..base import FeededBuffer,Cacher
from ..extensions import Container

pack = Container.pack

def wrap_class(cls):
    """
    Decorator for creating functions as tools based on Classes.

    the resulting functions are functions Stream -> params -> Stream
    When the resulting Class instance is created with provided params, and
    feeded with the caller Stream data.
    """
    def decorator(fun):
        name = fun.__name__

        def wraper(*args, **kwargs):
            flags = [isinstance(i,Container) for i in args]
            kwflags = {k:isinstance(i,Container) for k,i in kwargs.items()}

            if any(flags) or any(kwflags.values()):

                flag_len = [ len(i) if f else 0 for i,f in zip(args,flags) ]
                kw_len = { k:len(kwargs[k]) if kwflags[k] else 0 \
                           for k in kwargs }

                lengths = set(flag_len).union(kw_len.values())

                if len( lengths ) > 2:
                    raise ValueError('Contained values lengths not aligned')

                lengths.discard(0)

                l,*_ = lengths

                new_args = [ # for range in len(Containers)
                            [arg[i] if f else arg for arg,f in zip(args,flags)]
                             for i in range(l)]

                new_kwargs = [ # for range in len(Containers)
                            {k:kwargs[k][i] if kwflags[k] else kwargs[k]\
                             for k in kwargs}
                             for i in range(l)]

                new_feeds = [cls(*a,**k) for a,k in zip(new_args,new_kwargs)]

                return Container.pack(new_feeds)
            else:
                #Exceptions Here
                return cls(*args,**kwargs)

        #This section is almost shurely cleaner as a decorator
        wraper.__name__ = name
        wraper.__qualname__ = 'wrapers.{}'.format(name)
        wraper.__doc__ = \
        """Wraps {} Class:
        {}
        """.format(cls.__name__,cls.__init__.__doc__)
        return wraper
    return decorator

@wrap_class(PreProcessor)
def preprocess(): pass

@wrap_class(FeededBuffer)
def feed(): pass

@wrap_class(Cacher)
def cache(): pass
