from .pos10 import parts

parts = parts.label(label=[0.8,0.2])
source = parts[0]

data = source.feed(batch_size=source.length,max_length=source.length,force=True)
