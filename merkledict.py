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

    # Methods that set values
    def __setitem__(self, key, value):
        return dict.__setitem__(self, key, value)

    def update(self, other=(), **kwds):
        if hasattr(other, "keys"):
            for key in other.keys():
                self[key] = other[key]
        else:
            for key, value in other:
                self[key] = value
        for key, value in kwds.items():
            self[key] = value

    # Methods that remove values
    def __delitem__(self, key):
        return dict.__delitem__(self, key)

    def clear(self):
        return dict.clear(self)

    __marker = object()

    def pop(self, key, default=__marker):
        if default is self.__marker:
            return dict.pop(self, key)
        else:
            return dict.pop(self, key, default)

    def popitem(self):
        return dict.popitem(self)


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

    def test002(self):
        d1 = MerkleDict({'a': 1, 'b': 2, 'c': 3})
        d2 = MerkleDict((('a', 1), ('b', 2), ('c', 3)))
        self.assertEqual(d1, d2)
        self.assertEqual(d1.pop('a'), 1)
        self.assertEqual(d1.pop('b'), 2)
        self.assertEqual(d1.pop('x', 'yy'), 'yy')
        self.assertEqual(d1.popitem(), ('c', 3))

        d2.update({'x': 8, 'y': 9})
        self.assertEqual(set(('a','b','c','x','y')),
                         set(d2.keys()))
        d2.update((('u', 10), ('v', 11), ('w', 12)))
        self.assertEqual(set(('a','b','c','u','v','w','x','y')),
                         set(d2.keys()))

        
if __name__ == "__main__":
    unittest.main()
