#!/usr/bin/env python
"""Consistent hash code"""
import hashlib
import binascii
import bisect


class ConsistentHashTable(object):
    def __init__(self, nodelist, repeat):
        """Initialize a consistent hash table for the given list of nodes"""
        # Insert each node into the hash circle multiple times
        baselist = []
        for node in nodelist:
            for ii in xrange(repeat):
                nodestring = "%s:%d" % (node, ii)
                baselist.append((hashlib.md5(nodestring).digest(), node))
        # Build two lists: one of (hashvalue, node) pairs, sorted by
        # hashvalue, one of just the hashvalues, to allow use of bisect.
        self.nodelist = sorted(baselist, key=lambda x: x[0])
        self.hashlist = [hashnode[0] for hashnode in self.nodelist]

    def find_nodes(self, key, count=1, avoid=None):
        """Return a list of count nodes from the hash table that are
        consecutively after the hash of the given key, together with
        those nodes from the avoid collection that have been avoided.

        Returned list size is <= count, and any nodes in the avoid collection
        are not included."""
        if avoid is None:  # Use an empty set
            avoid = set()
        # Hash the key to find where it belongs on the ring
        hv = hashlib.md5(str(key)).digest()
        # Find the node after this hash value around the ring, as an index
        # into self.hashlist/self.nodelist
        initial_index = bisect.bisect(self.hashlist, hv)
        next_index = initial_index
        results = []
        avoided = []
        while len(results) < count:
            if next_index == len(self.nodelist):  # Wrap round to the start
                next_index = 0
            node = self.nodelist[next_index][1]
            if node in avoid:
                if node not in avoided:
                    avoided.append(node)
            elif node not in results:
                results.append(node)
            next_index = next_index + 1
            if next_index == initial_index:
                # Gone all the way around -- terminate loop regardless
                break
        return results, avoided

    def __str__(self):
        return ",".join(["(%s, %s)" %
                         (binascii.hexlify(nodeinfo[0]), nodeinfo[1])
                         for nodeinfo in self.nodelist])

# -----------IGNOREBEYOND: test code ---------------
import sys
import random
import unittest
from testutils import random_3letters, Stats

NODE_REPEAT = 10


class HashMultipleTestCase(unittest.TestCase):
    """Test consistent hash utility that uses multiple virtual nodes"""

    def setUp(self):
        self.c1 = ConsistentHashTable(('A', 'B', 'C'), 2)
        num_nodes = 50
        self.nodeset = set()
        while len(self.nodeset) < num_nodes:
            node = random_3letters()
            self.nodeset.add(node)
        self.c2 = ConsistentHashTable(self.nodeset, NODE_REPEAT)

    def testSmallExact(self):
        self.assertEqual(str(self.c1),
                         "(0ec9e6875e4c6e6702e1b81813a0b70d, B),"
                         "(1aa81a7562b705fb6779655b8e407ee3, A),"
                         "(1d1eeea52e95de7227efa6e226563cd2, C),"
                         "(2af91581036572478db2b2c90479c73f, B),"
                         "(57e1e221c0a1aa811bc8d4d8dd6deaa7, A),"
                         "(8b872364fb86c3da3f942c6346f01195, C)")
        result, avoided = self.c1.find_nodes('splurg', 2)
        self.assertEqual(result, ['A', 'C'])
        self.assertEqual(avoided, [])

        result, avoided = self.c1.find_nodes('splurg', 2, avoid=('A',))
        self.assertEqual(result, ['C', 'B'])
        self.assertEqual(avoided, ['A'])

        result, avoided = self.c1.find_nodes('splurg', 2, avoid=('A', 'B'))
        self.assertEqual(result, ['C'])
        self.assertEqual(set(avoided), set(['A', 'B']))

        result, avoided = self.c1.find_nodes('splurg', 2, avoid=('A', 'B', 'C'))
        self.assertEqual(result, [])
        self.assertEqual(set(avoided), set(['A', 'B', 'C']))

    def testLarge(self):
        x = self.c2.find_nodes('splurg', 15)[0]
        self.assertEqual(len(x), 15)

    def testDistribution(self):
        """Generate a lot of hash values and see how even the distribution is"""
        nodecount = dict([(node, 0) for node in self.nodeset])
        numkeys = 10000
        for _ in range(numkeys):
            node = self.c2.find_nodes(random_3letters(), 1)[0][0]
            nodecount[node] = nodecount[node] + 1
        stats = Stats()
        for node, count in nodecount.items():
            stats.add(count)
        print ("%d random hash keys assigned to %d nodes "
               "each repeated %d times "
               "are distributed across the nodes "
               "with a standard deviation of %0.2f (compared to a mean of %d)." %
               (numkeys, len(self.nodeset), NODE_REPEAT, stats.stddev(), numkeys / len(self.nodeset)))

    def testFailover(self):
        """For a given unavailable node, see what other nodes get new traffic"""
        transfer = {}
        for from_node in self.nodeset:
            transfer[from_node] = {}
            for to_node in self.nodeset:
                transfer[from_node][to_node] = 0
        numkeys = 10000
        for _ in range(numkeys):
            key = random_3letters()
            node_pair = self.c2.find_nodes(key, 2)[0]
            transfer[node_pair[0]][node_pair[1]] = transfer[node_pair[0]][node_pair[1]] + 1
        stats = Stats()
        for from_node in self.nodeset:
            num_dest_nodes = 0
            for to_node in self.nodeset:
                if transfer[from_node][to_node] > 0:
                    num_dest_nodes = num_dest_nodes + 1
            stats.add(num_dest_nodes)
        print ("On failure of a single node, %.1f other nodes (on average) "
               "handle the transferred traffic from that node." %
               stats.mean())


if __name__ == "__main__":
    ii = 1
    while ii < len(sys.argv):  # pragma: no cover
        arg = sys.argv[ii]
        if arg == "-s" or arg == "--seed":
            random.seed(sys.argv[ii + 1])
            del sys.argv[ii:ii + 2]
        elif arg == "-r" or arg == "--repeat":
            NODE_REPEAT = int(sys.argv[ii + 1])
            del sys.argv[ii:ii + 2]
        else:
            ii += 1
    unittest.main()
