#!/usr/bin/env python
"""Consistent hash code"""
import hashlib
import binascii
import bisect

class ConsistentHashTable:
    def __init__(self, nodelist):
        """Initialize a consistent hash table for the given list of nodes"""
        baselist = [(hashlib.md5(str(node)).digest(), node) for node in nodelist]
        # Build two lists: one of (hashvalue, node) pairs, sorted by hashvalue
        # One of just the hashvalues, to allow use of bisect.
        self.nodelist = sorted(baselist, key=lambda x:x[0])
        self.hashlist = [hashnode[0] for hashnode in self.nodelist]

    def find_nodes(self, key, count=1, avoid=None):
        """Return a list of count nodes from the hash table that are consecutively after the hash of the given key"""
        hv = hashlib.md5(str(key)).digest()
        if avoid is None: avoid = set() # Use an empty set
        initial_index = bisect.bisect(self.hashlist, hv)
        next_index = initial_index
        results = []
        while len(results) < count:
            if next_index == len(self.nodelist): next_index = 0
            if self.nodelist[next_index][1] not in avoid:
                results.append(self.nodelist[next_index][1])
            next_index = next_index + 1
            if next_index == initial_index:
                # Gone all the way around -- terminate loop regardless
                break
        return results

    def __str__(self):
        return ",".join(["(%s, %s)" % (binascii.hexlify(nodeinfo[0]), nodeinfo[1]) for nodeinfo in self.nodelist])
            
# -----------IGNOREBEYOND: test code ---------------
import random
if __name__ == "__main__":
    c1 = ConsistentHashTable(('A', 'B', 'C'))
    print c1.find_nodes('splurg', 2)
    print c1.find_nodes('splurg', 2, avoid=set(('A', )))
    print c1.find_nodes('splurg', 2, avoid=set(('A', 'B')))
    print c1.find_nodes('splurg', 2, avoid=set(('A', 'B',  'C')))
    num_nodes = random.randint(10,100)
    nodeset = set()
    while len(nodeset) < num_nodes:
        node = (chr(ord('A') + random.randint(0,25)) +
                chr(ord('A') + random.randint(0,25)) +
                chr(ord('A') + random.randint(0,25)))
        nodeset.add(node)
    c2 = ConsistentHashTable(nodeset)
    print c2.find_nodes('splurg', 15)
