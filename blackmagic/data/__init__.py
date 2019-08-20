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

    def __init__(self, cfg):
        pass
    
    def setup(self):
        pass

    def start(self):
        pass

    def stop(self):
        pass

    def select_tile(self, tx, ty):
        pass
    
    def select_chip(self, cx, cy):
        pass

    def select_pixels(self, cx, cy):
        pass

    def select_segments(self, cx, cy):
        pass

    def select_predictions(self, cx, cy):
        pass

    def insert_tile(self, tx, ty, model):
        pass
    
    def insert_chip(self, detections):
        pass

    def insert_pixels(self, detections):
        pass

    def insert_segments(self, detections):
        pass

    def insert_predictions(self, predictions):
        pass

    def delete_tile(self, tx, ty):
        pass
    
    def delete_chip(self, cx, cy):
        pass

    def delete_pixels(self, cx, cy):
        pass

    def delete_segments(self, cx, cy):
        pass

    def delete_predictions(self, cx, cy):
        pass
