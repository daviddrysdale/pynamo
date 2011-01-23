import sys
import random
import unittest

from framework import Framework, reset
from history import History

import dynamo1
import dynamo2
import dynamo as dynamo99

class SimpleTestCase(unittest.TestCase):
    """Test simple Dynamo function"""
    def setUp(self):
        reset()

    def tearDown(self):
        reset()

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
        self.put_fail_initial_node(dynamo1)
    def test_put2_fail_initial_node(self):
        self.put_fail_initial_node(dynamo2)
    def put_fail_initial_node(self, cls):
        for _ in range(6): cls.DynamoNode()
        a = cls.DynamoClientNode('a')
        destnode = random.choice(cls.DynamoNode.nodelist)
        a.put('K1', None, 1, destnode=destnode)
        # Fail at the forwarding node before it gets a chance to forward
        destnode.fail()
        Framework.schedule()
        print History.ladder()

    def test_put1_fail_initial_node2(self):
        self.put_fail_initial_node2(dynamo1)
    def test_put2_fail_initial_node2(self):
        self.put_fail_initial_node2(dynamo2)
    def put_fail_initial_node2(self, cls):
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
        self.put_fail_node2(dynamo1)
    def test_put2_fail_node2(self):
        self.put_fail_node2(dynamo2)
    def put_fail_node2(self, cls):
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
        self.put_fail_nodes23(dynamo1)
        print History.ladder(spacing=14)
    def test_put2_fail_nodes23(self):
        (_, pref_list) = self.put_fail_nodes23(dynamo2)
        # Force nodes that are of interest in put2_fail_nodes23_[234] to be included in the history
        print History.ladder(force_include=pref_list, spacing=14)
    def put_fail_nodes23(self, cls):
        for _ in range(6): cls.DynamoNode()
        a = cls.DynamoClientNode('a')
        # Fail the second and third node in the preference list
        pref_list = cls.DynamoNode.chash.find_nodes('K1', 5)
        a.put('K1', None, 1, destnode=pref_list[0])
        Framework.schedule(1)
        pref_list[1].fail()
        pref_list[2].fail()
        Framework.schedule(timers_to_process=2)
        return a, pref_list
    
    def test_put2_fail_nodes23_2(self):
        """Show second request for same key skipping failed nodes"""
        (a, pref_list) = self.put_fail_nodes23(dynamo2)
        destnode = pref_list[0]
        from_line = len(History.history)
        a.put('K1', None, 2, destnode=destnode)
        Framework.schedule()
        print History.ladder(force_include=pref_list, start_line=from_line, spacing=14)

    def test_put2_fail_nodes23_3(self):
        """Show PingReq failing"""
        (a, pref_list) = self.put_fail_nodes23(dynamo99)
        destnode = pref_list[0]
        a.put('K1', None, 2, destnode=destnode)
        Framework.schedule(timers_to_process=0)
        from_line = len(History.history)
        Framework.schedule(timers_to_process=3) 
        print History.ladder(force_include=pref_list, start_line=from_line, spacing=14)
        
    def test_put2_fail_nodes23_4(self):
        """Show PingReq recovering, and a subsequent Put returning to the original preference list"""
        (a, pref_list) = self.put_fail_nodes23(dynamo99)
        destnode = pref_list[0]
        from_line = len(History.history)
        a.put('K1', None, 2, destnode=destnode)
        Framework.schedule(timers_to_process=10)
        from_line = len(History.history)
        pref_list[1].recover()
        pref_list[2].recover()
        Framework.schedule(timers_to_process=15)
        a.put('K1', None, 3, destnode=pref_list[0])
        Framework.schedule(timers_to_process=5)
        print History.ladder(force_include=pref_list, start_line=from_line, spacing=14)
        # print History.ladder() # @@@@ staggered start to ... lines
    
if __name__ == "__main__":
    for ii in range(1, len(sys.argv)-1): # pragma: no cover
        arg = sys.argv[ii]
        if arg == "-s" or arg == "--seed":
            random.seed(sys.argv[ii+1])
            del sys.argv[ii:ii+2]
            break
    unittest.main()

