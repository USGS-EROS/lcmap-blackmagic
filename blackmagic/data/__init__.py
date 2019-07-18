from interface import Interface

"""Blackmagic Storage provides a pluggable interface that can be implemented for
multiple back-edn providers.  Each operation is assumed to be working at that
partition level rather than an individual data point.

For example:
get_chip should retrieve a partition of chip-level data, not a raster chip.
get_pixel does not retrieve a single pixel, but a partition of data that pertains to pixel-level information.
delete_segment deletes the entire partition of segments identified by the x & y.
"""

class Storage(Interface):

    def setup(self):
        pass

    def start(self):
        pass

    def stop(self):
        pass

    def get(self, prefix, key):
        pass

    def put(self, prefix, key, value):
        pass

    def delete(self, prefix, key):
        pass
