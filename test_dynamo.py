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

    def test_simple_put_fail1(self):
        for _ in range(6):
            dynamo1.DynamoNode()
        a = dynamo1.DynamoClientNode('a')
        destnode = random.choice(dynamo1.DynamoNode.nodelist)
        a.put('K1', None, 1, destnode=destnode)
        # Fail at the forwarding node before it gets a chance to forward
        destnode.fail()
        Framework.schedule()
        print History.ladder()

    def test_simple_put_fail2(self):
        for _ in range(6):
            dynamo1.DynamoNode()
        a = dynamo1.DynamoClientNode('a')
        destnode = random.choice(dynamo1.DynamoNode.nodelist)
        a.put('K1', None, 1, destnode=destnode)
        # Fail at the forwarding node after it gets a chance to forward
        Framework.schedule(1)
        destnode.fail()
        Framework.schedule()
        print History.ladder()

    def test_simple_put_fail3(self):
        for _ in range(6):
            dynamo1.DynamoNode()
        a = dynamo1.DynamoClientNode('a')
        a.put('K1', None, 1)
        # Fail the second node in the preference list
        pref_list = dynamo1.DynamoNode.chash.find_nodes('K1', 3)
        Framework.schedule(1)
        pref_list[1].fail()
        Framework.schedule()
        a.get('K1')
        Framework.schedule()
        print History.ladder()

    def test_simple_put_fail4(self):
        for _ in range(6):
            dynamo1.DynamoNode()
        a = dynamo1.DynamoClientNode('a')
        a.put('K1', None, 1)
        # Fail the second and third node in the preference list
        pref_list = dynamo1.DynamoNode.chash.find_nodes('K1', 3)
        Framework.schedule(1)
        pref_list[1].fail()
        pref_list[2].fail()
        Framework.schedule()
        print History.ladder()
    
    def test_client_retry(self):
        for _ in range(6):
            dynamo.DynamoNode()
        a = dynamo.DynamoClientNode('a')
        destnode = random.choice(dynamo.DynamoNode.nodelist)
        a.put('K1', None, 1, destnode=destnode)
        # Fail at the forwarding node before it gets a chance to forward
        destnode.fail()
        Framework.schedule()
        print "\n".join(["%s: %s" % (e[0],str(e[1])) for e in History.history])
        print History.ladder()

if __name__ == "__main__":
    for ii in range(1, len(sys.argv)-1): # pragma: no cover
        arg = sys.argv[ii]
        if arg == "-s" or arg == "--seed":
            random.seed(sys.argv[ii+1])
            del sys.argv[ii:ii+2]
            break
    unittest.main()

