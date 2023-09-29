"""Takes as input a batch of comments and returns classifications.

Batching managed by whatever is calling this service. Can possibly
even be a batch of size 1.
"""
# NOTE: need to import the necessary models outside of
# the main function, otherwise the lambda function
# will import the models every time it is called
pass
