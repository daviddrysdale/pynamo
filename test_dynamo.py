import sys
import random
import unittest

from framework import Framework, reset
from history import History
from dynamo import DynamoNode, DynamoClientNode

class SimpleTestCase(unittest.TestCase):
    """Test simple Dynamo function"""
    def setUp(self):
        reset()

    def tearDown(self):
        pass

    def test_simple_put(self):
        for _ in range(10):
            DynamoNode()
        a = DynamoClientNode('a')
        a.put('K1', None, 1)
        Framework.schedule()
        print History.ladder()
    
    def test_simple_get(self):
        for _ in range(10):
            DynamoNode()
        a = DynamoClientNode('a')
        a.put('K1', None, 1)
        Framework.schedule()
        from_line = len(History.history)
        a.get('K1')
        Framework.schedule()
        print History.ladder(start_line=from_line)

if __name__ == "__main__":
    for ii in range(1, len(sys.argv)-1): # pragma: no cover
        arg = sys.argv[ii]
        if arg == "-s" or arg == "--seed":
            random.seed(sys.argv[ii+1])
            del sys.argv[ii:ii+2]
            break
    unittest.main()

