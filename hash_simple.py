#!/usr/bin/env python
"""Consistent hash code"""
import hashlib
import binascii
import bisect


class SimpleConsistentHashTable(object):
    def __init__(self, nodelist):
        """Initialize a consistent hash table for the given list of nodes"""
        baselist = [(hashlib.md5(str(node)).digest(), node) for node in nodelist]
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
        avoided = set()
        while len(results) < count:
            if next_index == len(self.nodelist):  # Wrap round to the start
                next_index = 0
            node = self.nodelist[next_index][1]
            if node in avoid:
                avoided.add(node)
            else:
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


class HashSimpleTestCase(unittest.TestCase):
    """Test simple consistent hashing class"""

    def setUp(self):
        self.c1 = SimpleConsistentHashTable(('A', 'B', 'C'))
        num_nodes = 50
        self.nodeset = set()
        while len(self.nodeset) < num_nodes:
            node = random_3letters()
            self.nodeset.add(node)
        self.c2 = SimpleConsistentHashTable(self.nodeset)

    def testSmallExact(self):
        self.assertEqual(str(self.c1),
                         "(0d61f8370cad1d412f80b84d143e1257, C),"
                         "(7fc56270e7a70fa81a5935b72eacbe29, A),"
                         "(9d5ed678fe57bcca610140957afab571, B)")
        self.assertEqual(self.c1.find_nodes('splurg', 2),
                         (['A', 'B'], set()))
        self.assertEqual(self.c1.find_nodes('splurg', 2, avoid=('A',)),
                         (['B', 'C'], set('A')))
        self.assertEqual(self.c1.find_nodes('splurg', 2, avoid=('A', 'B')),
                         (['C'], set(('A', 'B'))))
        self.assertEqual(self.c1.find_nodes('splurg', 2, avoid=('A', 'B', 'C')),
                         ([], set(('A', 'B', 'C'))))

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
               "are distributed across the nodes "
               "with a standard deviation of %0.2f (compared to a mean of %d)." %
               (numkeys, len(self.nodeset), stats.stddev(), numkeys / len(self.nodeset)))

    def testFailover(self):
        """For a given unavailable node, see what other nodes get new traffic"""
        test_node = None
        transfer_count = dict([(node, 0) for node in self.nodeset])
        total_transfers = 0
        for _ in range(1000):
            key = random_3letters()
            node_pair = self.c2.find_nodes(key, 2)[0]
            if test_node is None:
                test_node = node_pair[0]
            if node_pair[0] == test_node:
                next_node = node_pair[1]
                transfer_count[next_node] = transfer_count[next_node] + 1
                total_transfers = total_transfers + 1

        for node in self.nodeset:
            if transfer_count[node] > 0:
                print ("Node %s gets %d of %d (%0.0f%%) transfers" %
                       (node, transfer_count[node], total_transfers,
                        100 * transfer_count[node] / total_transfers))


if __name__ == "__main__":
    ii = 1
    while ii < len(sys.argv):  # pragma: no cover
        arg = sys.argv[ii]
        if arg == "-s" or arg == "--seed":
            random.seed(sys.argv[ii + 1])
            del sys.argv[ii:ii + 2]
        else:
            ii += 1
    unittest.main()
