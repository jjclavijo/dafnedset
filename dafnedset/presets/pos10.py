from ..datasets import DefaultQuerys

raw = DefaultQuerys.positive().label(label=[1.,0.]).scale()

parts = raw.split([0.7,0.15,0.145,0.005]).cache() # Train, Test, Val, show

source = parts[0]

data = source.feed(batch_size=source.length,max_length=source.length,force=True)
