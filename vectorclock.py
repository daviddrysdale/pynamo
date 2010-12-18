#!/usr/bin/env python
"""Vector clock class"""
class VectorClock:
    def __init__(self, node, counter):
        self.clock = {node: counter}

    def add(self, node, counter):
        if node in self.clock and counter <= self.clock[node]:
            raise Exception("Node %s has gone backwards from %d to %d" % 
                            (node, self.clock[node], counter))
        self.clock[node] = counter

    def __str__(self):
        result = "{"
        need_comma = False
        for node in sorted(self.clock.keys()):
            if need_comma: result = result + ","
            result = result + "%s:%d" % (node, self.clock[node])
            need_comma = True
        return result + "}"

    # Comparison operations. Vector clocks are partially ordered:
    #   reflexive: a<=a (because a==a)
    #   anti-symmetric: a<=b and b<=a => a==b
    #   transitive: a<=b and b<=c => a<=c
    # Vector clocks are not totally ordered -- it can be that neither a<=b nor b<=a holds
    def __eq__(self, other):
        return self.clock == other.clock
    def __ne__(self, other):
        return not (self==other)
    def __lt__(self, other):
        for node in self.clock:
            if node not in other.clock: return False
            if self.clock[node] > other.clock[node]: return False
        return True
    def __le__(self, other):
        lt = self < other
        if lt is NotImplemented: return NotImplemented
        if lt: return True
        return (self==other)
    def __gt__(self, other):
        return other<self
    def __ge__(self, other):
        gt = self > other
        if gt is NotImplemented: return NotImplemented
        if gt: return True
        return (self==other)

# -----------IGNOREBEYOND: test code ---------------
import unittest

class VectorClockTestCase(unittest.TestCase):
    """Test vector clock class"""

    def setUp(self):
        self.c1 = VectorClock('A', 1)
        self.c2 = VectorClock('B', 2)
        
    def testSmall(self):
        self.assertEquals(str(self.c1), "{A:1}")
        self.c1.add('A',2)
        self.assertEquals(str(self.c1), "{A:2}")
        self.c1.add('A',200)
        self.assertEquals(str(self.c1), "{A:200}")
        self.c1.add('B',1)
        self.assertEquals(str(self.c1), "{A:200,B:1}")

    def testInternalError(self):
        self.assertRaises(Exception, self.c2.add, 'B', 1)

    def testEquality(self):
        self.assertEquals(self.c1==self.c2, False)
        self.assertEquals(self.c1!=self.c2, True)
        self.c1.add('B',2)
        self.c2.add('A',1)
        self.assertEquals(self.c1==self.c2, True)
        self.assertEquals(self.c1!=self.c2, False)

    def testOrder(self):
        self.assertEquals(self.c1 < self.c2, False)
        self.assertEquals(self.c2 < self.c1, False)
        self.assertEquals(self.c1 <= self.c2, False)
        self.assertEquals(self.c2 <= self.c1, False)
        self.c1.add('B',2)
        self.assertEquals(self.c1 < self.c2, False)
        self.assertEquals(self.c2 < self.c1, True)
        self.assertEquals(self.c1 <= self.c2, False)
        self.assertEquals(self.c2 <= self.c1, True)
        self.assertEquals(self.c1 > self.c2, True)
        self.assertEquals(self.c2 > self.c1, False)
        self.assertEquals(self.c1 >= self.c2, True)
        self.assertEquals(self.c2 >= self.c1, False)

if __name__ == "__main__":
    unittest.main()

        
