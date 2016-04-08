__author__ = 'mertergun'
from math import radians, cos, sin, asin, sqrt

def haversine(lon1, lat1, lon2, lat2):
    """
    Calculate the great circle distance between two points
    on the earth (specified in decimal degrees)
    """
    # convert decimal degrees to radians
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])

    # haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a))
    r = 6371 # Radius of earth in kilometers. Use 3956 for miles
    return c * r
#notfinished
class Sorted_List:
    def __init__(self, maxsize, compare_func):
        self.lst = []
        self.size = maxsize
        self.func = compare_func # f(x,y) returns true if x > y else false
    
    def add(self,el):
        if len(self.lst) < self.size:
            #binary sort-insert
            i_begin = 0
            i_end = len(self.lst)
            while i_begin < i_end:
                i = (i_begin + i_end)/2
                if self.func(self.lst[i],el):
                    i_begin = i + 1
                else:
                    i_end = i - 1
            self.lst.insert(i_begin, el)
            del(self.lst[-1])
            
            
        