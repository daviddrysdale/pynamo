import sys
import random
import unittest

from framework import Framework, reset
from history import History
import dynamo1

class SimpleTestCase(unittest.TestCase):
    """Test simple Dynamo function"""
    def setUp(self):
        reset()

    def tearDown(self):
        pass

    def test_simple_put(self):
        for _ in range(6):
            dynamo1.DynamoNode()
        a = dynamo1.DynamoClientNode('a')
        a.put('K1', None, 1)
        Framework.schedule()
        print History.ladder()
    
    def test_simple_get(self):
        for _ in range(6):
            dynamo1.DynamoNode()
        a = dynamo1.DynamoClientNode('a')
        a.put('K1', None, 1)
        Framework.schedule()
        from_line = len(History.history)
        a.get('K1')
        Framework.schedule()
        print History.ladder(start_line=from_line)

    def test_double_put(self):
        for _ in range(6):
            dynamo1.DynamoNode()
        a = dynamo1.DynamoClientNode('a')
        b = dynamo1.DynamoClientNode('b')
        a.put('K1', None, 1)
        Framework.schedule(1)
        b.put('K2', None, 17)
        Framework.schedule()
        print History.ladder(spacing=14)

if __name__ == "__main__":
    for ii in range(1, len(sys.argv)-1): # pragma: no cover
        arg = sys.argv[ii]
        if arg == "-s" or arg == "--seed":
            random.seed(sys.argv[ii+1])
            del sys.argv[ii:ii+2]
            break
    unittest.main()

