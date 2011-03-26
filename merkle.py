#!/usr/bin/env python
"""Minimal Merkle Tree implementation in Python"""
import hashlib

def extract_subrange(min_key, max_key, keystore):
    """Find all of the keys in the keystore that hash within the given range"""
    subdict = {}
    for key, value in keystore.items():
        hashval = hashlib.md5(str(key))
        # convert 128-bit MD5 value to long
        hashval_l = long(hashval.hexdigest(), 16)
        if hashval_l >= min_key and hashval_l < max_key:
            subdict[key] = value
    return subdict

def divide_range(min_key, max_key, divisions):
    """Divide key range into equal size chunks"""
    total_keyrange = (max_key - min_key)
    # Round up the per-division range to ensure the whole range is covered.
    per_leaf_range = (total_keyrange + divisions - 1) / divisions
    return [(min_key + ii*per_leaf_range, 
             min_key + (ii+1)*per_leaf_range) 
            for ii in xrange(divisions)]

class MerkleLeaf:
    """Leaf node in Merkle tree, encompassing all keys in range [min, max)"""
    def __init__(self, min_key, max_key, keystore):
        self.min_key = min_key
        self.max_key = max_key
        subdict = extract_subrange(min_key, max_key, keystore)
        self.value = hashlib.md5(str(subdict))
    def __str__(self):
        return "[%s,%s)=>%s" % (self.min_key, self.max_key, self.value.hexdigest()[:6])

class MerkleNode:
    """Interior node in Merkle tree"""
    def __init__(self, left, right):
        self.left = left
        self.right = right
        # Hash value is hash of two children's hash values concatenated
        self.value = hashlib.md5(left.value.digest() + right.value.digest())
    def __str__(self):
        return self.value.hexdigest()[:6]

class MerkleTree:
    def __init__(self, depth, min_key, max_key, keystore):
        """Build a Merkle tree of given depth covering keys in range [min, max)"""
        # There are 2^depth leaves in the tree.
        num_leaves = 2 ** depth
        divisions = divide_range(min_key, max_key, num_leaves)

        # Bottom layer of the tree is 2^depth leaf nodes
        self.nodes = []
        self.nodes.append([MerkleLeaf(divisions[ii][0], divisions[ii][1], keystore)
                           for ii in xrange(num_leaves)])
        # Each layer >= 1 consists of interior nodes, and is half the size
        # of the layer below.
        level = 1
        while level <= depth:
            self.nodes.append([MerkleNode(self.nodes[level-1][2*ii], 
                                          self.nodes[level-1][2*ii+1]) 
                               for ii in xrange(len(self.nodes[level-1])/2)])
            level = level + 1

    def root(self):
        return self.nodes[-1][0]

    def __str__(self):
        result = ""
        for level, list in enumerate(self.nodes):
            result = result + "[%d] " % level
            for node in list:
                result = result + str(node) + ' '
            result = result + '\n'
        return result
        


# -----------IGNOREBEYOND: test code ---------------
import sys
import copy
import random
import unittest

from testutils import random_3letters

class MerkleTestCase(unittest.TestCase):
    """Test Merkle tree implementation"""

    def setUp(self):
        self.keystore = dict((random_3letters(), random.randint(0,99)) for ii in xrange(50))
        self.keya = long(hashlib.md5('A').hexdigest(), 16)
        self.keyb = long(hashlib.md5('B').hexdigest(), 16)
        if self.keya < self.keyb:
            self.min_key = self.keya
            self.max_key = self.keyb
        else:
            self.min_key = self.keyb
            self.max_key = self.keya

    def testCreation(self):
        # MD5 values are 128-bit; convert to long
        x = MerkleTree(3, self.min_key, self.max_key, self.keystore) 
        xs = str(x)

    def testCompare(self):
        keystore2 = copy.copy(self.keystore)
        keystore2['A'] = 'xyzzy'
        x0 = MerkleTree(3, self.min_key, self.max_key, self.keystore) 
        x1 = MerkleTree(3, self.min_key, self.max_key, self.keystore) 
        x2 = MerkleTree(3, self.min_key, self.max_key, keystore2) 
        x0t = x0.root()
        x1t = x1.root()
        x2t = x2.root()
        self.assertEqual(x0t.value.hexdigest(), x1t.value.hexdigest())
        self.assertNotEqual(x1t.value.hexdigest(), x2t.value.hexdigest())
        x1L = x1t.left
        x1R = x1t.right
        x2L = x2t.left
        x2R = x2t.right
        # Exactly one of the LL or RR pairs differ
        if x1L.value.hexdigest() == x2L.value.hexdigest():
            self.assertNotEqual(x1R.value.hexdigest(), x2R.value.hexdigest())
        else:
            self.assertEqual(x1R.value.hexdigest(), x2R.value.hexdigest())
            self.assertNotEqual(x1L.value.hexdigest(), x2L.value.hexdigest())

    def testDivideRange(self):
        divs = divide_range(0, 1022, 12)
        self.assertEqual(len(divs), 12)
        self.assertEqual(divs[1], (86,172))
    
if __name__ == "__main__":
    ii = 1
    while ii < len(sys.argv): # pragma: no cover
        arg = sys.argv[ii]
        if arg == "-s" or arg == "--seed":
            random.seed(sys.argv[ii+1])
            del sys.argv[ii:ii+2]
        else:
            ii = ii + 1
    unittest.main()
