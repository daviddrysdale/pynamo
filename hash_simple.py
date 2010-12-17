#!/usr/bin/env python
"""Consistent hash code"""
import hashlib
import binascii
import bisect

class ConsistentHashTable:
    def __init__(self, nodelist):
        """Initialize a consistent hash table for the given list of nodes"""
        baselist = [(hashlib.md5(str(node)).digest(), node) for node in nodelist]
        self.nodelist = sorted(baselist, key=lambda x:x[0])
    def find_nodes(self, key, count=1):
        hv = hashlib.md5(str(key)).digest()
        # bisect.bisect(self.nodelist @@@@@
    def __str__(self):
        return ",".join(["(%s, %s)" % (binascii.hexlify(nodeinfo[0]), nodeinfo[1]) for nodeinfo in self.nodelist])
            

if __name__ == "__main__":
    c1 = ConsistentHashTable(('A', 'B', 'C'))
    print c1
    
