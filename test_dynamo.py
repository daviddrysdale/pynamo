import sys
import random
import unittest

from framework import Framework, reset
from history import History

import dynamo1
import dynamo

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

    def test_put1_fail_initial_node(self):
        self.test_put_fail_initial_node(dynamo1)
    def test_put2_fail_initial_node(self):
        self.test_put_fail_initial_node(dynamo)
    def test_put_fail_initial_node(self, cls):
        for _ in range(6): cls.DynamoNode()
        a = cls.DynamoClientNode('a')
        destnode = random.choice(cls.DynamoNode.nodelist)
        a.put('K1', None, 1, destnode=destnode)
        # Fail at the forwarding node before it gets a chance to forward
        destnode.fail()
        Framework.schedule()
        print History.ladder()

    def test_put1_fail_initial_node2(self):
        self.test_put_fail_initial_node2(dynamo1)
    def test_put2_fail_initial_node2(self):
        self.test_put_fail_initial_node2(dynamo)
    def test_put_fail_initial_node2(self, cls):
        for _ in range(6): dynamo1.DynamoNode()
        a = dynamo1.DynamoClientNode('a')
        destnode = random.choice(dynamo1.DynamoNode.nodelist)
        a.put('K1', None, 1, destnode=destnode)
        # Fail at the forwarding node after it gets a chance to forward
        Framework.schedule(1)
        destnode.fail()
        Framework.schedule()
        print History.ladder()

    def test_put1_fail_node2(self):
        self.test_put_fail_node2(dynamo1)
    def test_put2_fail_node2(self):
        self.test_put_fail_node2(dynamo)
    def test_put_fail_node2(self, cls):
        for _ in range(6): cls.DynamoNode()
        a = cls.DynamoClientNode('a')
        a.put('K1', None, 1)
        # Fail the second node in the preference list
        pref_list = cls.DynamoNode.chash.find_nodes('K1', 3)
        Framework.schedule(1)
        pref_list[1].fail()
        Framework.schedule()
        a.get('K1')
        Framework.schedule()
        print History.ladder()

    def test_put1_fail_nodes23(self):
        self.test_put_fail_nodes23(dynamo1)
    def test_put2_fail_nodes23(self):
        self.test_put_fail_nodes23(dynamo)
    def test_put_fail_nodes23(self, cls):
        for _ in range(6): cls.DynamoNode()
        a = cls.DynamoClientNode('a')
        # Fail the second and third node in the preference list
        pref_list = cls.DynamoNode.chash.find_nodes('K1', 3)
        a.put('K1', None, 1, destnode=pref_list[0])
        Framework.schedule(1)
        pref_list[1].fail()
        pref_list[2].fail()
        Framework.schedule()
        print History.ladder()
    
    
if __name__ == "__main__":
    for ii in range(1, len(sys.argv)-1): # pragma: no cover
        arg = sys.argv[ii]
        if arg == "-s" or arg == "--seed":
            random.seed(sys.argv[ii+1])
            del sys.argv[ii:ii+2]
            break
    unittest.main()

