"""Dataset Creation for seismic GNSS dialy timeseries dataset, 

Data is queryed form a pre-filled database.

Raw data is filtered trough pre-configured data_augmentation and labelling
operations.

The transformation configuration is managed by the presets submodule.

Any object of the presets submodeule which has a .data property which iterates 
trough pyArrow batches can be served by the app.

We usually use Batch Feeders from base.py, transformed by the operations on
transformations.py. See presets for examples.

"""


__version__ = "0.0.1"
