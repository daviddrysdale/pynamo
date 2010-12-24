#!/usr/bin/env python
"""Consistent hash code"""
import hashlib
import binascii
import bisect

class ConsistentHashTable:
    def __init__(self, nodelist, repeat):
        """Initialize a consistent hash table for the given list of nodes"""
        # Insert each node into the hash circle multiple times
        baselist = []
        for node in nodelist:
            for ii in range(repeat):
                nodestring = str(node) + (":%d" % ii) 
                baselist.append((hashlib.md5(nodestring).digest(), node))
        # Build two lists: one of (hashvalue, node) pairs, sorted by hashvalue
        # One of just the hashvalues, to allow use of bisect.
        self.nodelist = sorted(baselist, key=lambda x:x[0])
        self.hashlist = [hashnode[0] for hashnode in self.nodelist]

    def find_nodes(self, key, count=1, avoid=None):
        """Return a list of count nodes from the hash table that are consecutively after the hash of the given key"""
        hv = hashlib.md5(str(key)).digest()
        if avoid is None: # Use an empty set
            avoid = set() 
        initial_index = bisect.bisect(self.hashlist, hv)
        next_index = initial_index
        results = []
        while len(results) < count:
            if next_index == len(self.nodelist): # Wrap round to the start
                next_index = 0
            node = self.nodelist[next_index][1]
            if node not in avoid and node not in results:
                results.append(node)
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
from testutils import random_3letters, Stats

class HashMultipleTestCase(unittest.TestCase):
    """Test consistent hash utility that uses multiple virtual nodes"""

    def setUp(self):
        self.c1 = ConsistentHashTable(('A', 'B', 'C'), 2)
        num_nodes = 50
        self.nodeset = set()
        while len(self.nodeset) < num_nodes:
            node = random_3letters()
            self.nodeset.add(node)
        self.c2 = ConsistentHashTable(self.nodeset, 10)
        
    def testSmallExact(self):
        self.assertEqual(str(self.c1), 
                         "(0ec9e6875e4c6e6702e1b81813a0b70d, B),"
                         "(1aa81a7562b705fb6779655b8e407ee3, A),"
                         "(1d1eeea52e95de7227efa6e226563cd2, C),"
                         "(2af91581036572478db2b2c90479c73f, B),"
                         "(57e1e221c0a1aa811bc8d4d8dd6deaa7, A),"
                         "(8b872364fb86c3da3f942c6346f01195, C)")
        self.assertEqual(self.c1.find_nodes('splurg', 2), ['A', 'C'])
        self.assertEqual(self.c1.find_nodes('splurg', 2, avoid=('A',)), ['C', 'B'])
        self.assertEqual(self.c1.find_nodes('splurg', 2, avoid=('A','B')), ['C'])
        self.assertEqual(self.c1.find_nodes('splurg', 2, avoid=('A','B','C')), [])

    def testLarge(self):
        x = self.c2.find_nodes('splurg', 15)
        self.assertEqual(len(x), 15)

    def testDistribution(self):
        """Generate a lot of hash values and see how even the distribution is"""
        nodecount = dict([(node, 0) for node in self.nodeset])
        numhashes = 10000
        for _ in range(numhashes):
            node = self.c2.find_nodes(random_3letters(), 1)[0]
            nodecount[node] = nodecount[node] + 1
        average_count = numhashes/len(self.nodeset)
        average_percent = 100*average_count / numhashes
        stats = Stats()
        for node, count in nodecount.items():
            stats.add(count)
        print ("%d random hash keys assigned to %d nodes "
               "are distributed across the nodes "
               "with a standard deviation of %0.2f." %
               (numhashes, len(self.nodeset), stats.stddev()))

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
    for ii in range(1, len(sys.argv)-1): # pragma: no cover
        arg = sys.argv[ii]
        if arg == "-s" or arg == "--seed":
            random.seed(sys.argv[ii+1])
            del sys.argv[ii:ii+2]
            break
    unittest.main()
