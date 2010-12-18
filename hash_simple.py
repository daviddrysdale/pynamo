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
import sys
import random
import unittest

def random_3letters():
    return (chr(ord('A') + random.randint(0,25)) +
            chr(ord('A') + random.randint(0,25)) +
            chr(ord('A') + random.randint(0,25)))

class HashSimpleTestCase(unittest.TestCase):
    """Test simple consistent hashing class"""

    def setUp(self):
        self.c1 = ConsistentHashTable(('A', 'B', 'C'))
        num_nodes = random.randint(20,50)
        self.nodeset = set()
        while len(self.nodeset) < num_nodes:
            node = random_3letters()
            self.nodeset.add(node)
        self.c2 = ConsistentHashTable(self.nodeset)
        
    def testSmallExact(self):
        self.assertEqual(self.c1.find_nodes('splurg', 2), ['A', 'B'])
        self.assertEqual(self.c1.find_nodes('splurg', 2, avoid=('A',)), ['B', 'C'])
        self.assertEqual(self.c1.find_nodes('splurg', 2, avoid=('A','B')), ['C'])
        self.assertEqual(self.c1.find_nodes('splurg', 2, avoid=('A','B','C')), [])

    def testLarge(self):
        x = self.c2.find_nodes('splurg', 15)
        self.assertEqual(len(x), 15)

    def testDistribution(self):
        """Generate a lot of hash values and see how even the distribution is"""
        nodecount = dict([(node, 0) for node in self.nodeset])
        for _ in range(1000):
            node = self.c2.find_nodes(random_3letters(), 1)[0]
            nodecount[node] = nodecount[node] + 1
        average_count = 1000/len(self.nodeset)
        average_percent = 100*average_count / 1000
        print "Expect average node to get %d of the 1000 hash values, %0.1f%% of total" % (average_count, average_percent)
        overfull_count = 0
        for node, count in nodecount.items():
            percent_allocated = 100*count/1000
            if percent_allocated > 1.5 * average_percent:
                overfull_count = overfull_count + 1
                print "  %s %0.0f%%" % (node, 100*count/1000)
        print ("%d nodes (of %d, so %0.0f%%) had more than 50%% over the expected average" % 
               (overfull_count, len(self.nodeset), 100*overfull_count/len(self.nodeset)))
            
    def testFailover(self):
        """For a given unavailable node, see what other nodes get new traffic"""
        test_node = None
        transfer_count = dict([(node, 0) for node in self.nodeset])
        total_transfers = 0
        for _ in range(1000):
            key = random_3letters()
            node_pair = self.c2.find_nodes(key, 2)
            if test_node is None: test_node = node_pair[0]
            if node_pair[0] == test_node:
                next_node = node_pair[1]
                transfer_count[next_node] = transfer_count[next_node] + 1
                total_transfers = total_transfers + 1

        for node in self.nodeset:
            if transfer_count[node] > 0:
                print ("Node %s gets %d of %d (%0.0f%%) transfers" % 
                       (node, transfer_count[node], total_transfers , 100*transfer_count[node]/ total_transfers))


if __name__ == "__main__":
    for ii in range(1, len(sys.argv)-1):
        arg = sys.argv[ii]
        if arg == "-s" or arg == "--seed":
            random.seed(sys.argv[ii+1])
            del sys.argv[ii:ii+2]
            break
    unittest.main()
