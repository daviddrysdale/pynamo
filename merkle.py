
import hashlib

class MerkleLeaf:
    """Leaf node in Merkle tree, encompassing all keys in range [min, max)"""
    def __init__(self, min_key, max_key):
        self.min_key = min_key
        self.max_key = max_key
        self.value = hashlib.md5(str(self.min_key)) # @@@
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
    def __init__(self, depth, min_key, max_key):
        """Build a Merkle tree of given depth covering keys in range [min, max)"""
        # There are 2^depth leaves in the tree.
        num_leaves = 2 ** depth
        # Each leaf covers a keyrange of size ~ (max-min)/2^depth.  Round
        # up the per-leaf range to ensure the whole range is covered.
        total_keyrange = (max_key - min_key)
        per_leaf_range = (total_keyrange + num_leaves - 1)/ num_leaves
        self.nodes = []
        self.nodes.append([MerkleLeaf(min_key + ii*per_leaf_range,
                                      min_key + (ii+1)*per_leaf_range) 
                           for ii in xrange(num_leaves)])
        level = 1
        while level <= depth:
            self.nodes.append([MerkleNode(self.nodes[level-1][2*ii], 
                                          self.nodes[level-1][2*ii+1]) 
                               for ii in xrange(len(self.nodes[level-1])/2)])
            level = level + 1

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
import random
import unittest

class MerkleTestCase(unittest.TestCase):
    """Test Merkle tree implementation"""

    def setUp(self):
        pass
    def testCreation(self):
        x = MerkleTree(3, 0, 1022)
        print x
    
    
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

