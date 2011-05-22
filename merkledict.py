#!/usr/bin/env python
"""Merkle Tree wrapper for Python dictionary"""

from merkle import MerkleTree

class MerkleDict(MerkleTree, dict):
    """Dictionary that maintains a parallel Merkle tree of its contents"""
    def __init__(self, initdata=None):
        dict.__init__(self)
        if hasattr(initdata, "iteritems"):
            for k, v in initdata.iteritems():
                self[k] = v
        elif initdata is not None:  # assume iterable
            for ii, pair in enumerate(initdata):
                if len(pair) != 2:
                    raise ValueError("dictionary update sequence element #%d has length %d; 2 is required" % 
                                     (ii, len(pair)))
                self[pair[0]] = pair[1]

    def __setitem__(self, key, value):
        return dict.__setitem__(self, key, value)

    def __delitem__(self, key):
        return dict.__delitem__(self, key)

# -----------IGNOREBEYOND: test code ---------------
import unittest

class MerkleContainerTest(unittest.TestCase):
    def test001(self):
        d1 = MerkleDict()
        d1['a'] = 1
        d1['b'] = 2
        self.assertEqual(d1['a'], 1)
        self.assertEqual(d1['b'], 2)
        self.assertEqual(len(d1), 2)
        self.assertTrue('a' in d1)
        self.assertTrue('b' in d1)
        self.assertFalse('c' in d1)
        del d1['a']
        self.assertRaises(KeyError, d1.__getitem__, *('a',))
        self.assertEqual(d1['b'], 2)
        self.assertEqual(len(d1), 1)
        self.assertFalse('a' in d1)
        self.assertTrue('b' in d1)
        self.assertFalse('c' in d1)

        d2 = MerkleDict({'a': 1, 'b': 2})
        self.assertEqual(d2['a'], 1)
        self.assertEqual(d2['b'], 2)
        self.assertEqual(len(d2), 2)
        self.assertNotEquals(d1, d2)
        del d2['a']
        self.assertEqual(d1, d2)
        
        d2.clear()
        self.assertFalse('a' in d2)
        self.assertFalse('b' in d2)
        self.assertFalse('c' in d2)


if __name__ == "__main__":
    unittest.main()
