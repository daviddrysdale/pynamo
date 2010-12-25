#!/usr/bin/env python
"""Vector clock class"""
class VectorClock:
    def __init__(self):
        self.clock = {} # node => counter
    
    def update(self, node, counter):
        """Add a new node:counter value to a VectorClock."""
        if node in self.clock and counter <= self.clock[node]:
            raise Exception("Node %s has gone backwards from %d to %d" % 
                            (node, self.clock[node], counter))
        self.clock[node] = counter
        return self

    def __str__(self):
        return "{%s}" % ",".join(["%s:%d" % (node, self.clock[node]) 
                                  for node in sorted(self.clock.keys())])
# PART 2
    # Comparison operations. Vector clocks are partially ordered, but not totally ordered.
    def __eq__(self, other):
        return self.clock == other.clock
    def __lt__(self, other):
        for node in self.clock:
            if node not in other.clock: return False
            if self.clock[node] > other.clock[node]: return False
        return True
    def __ne__(self, other):
        return not (self==other)
    def __le__(self, other):
        return (self==other) or (self < other)
    def __gt__(self, other):
        return (other<self)
    def __ge__(self, other):
        return (self==other) or (self > other)
# PART 3
    @classmethod
    def coalesce(cls, vcs):
        """Coalesce a container of VectorClock objects."""
        result = []
        for vc in vcs:
            # See if this vector-clock is subsumed by anything already present
            subsumed = False
            for existing in result:
                if vc < existing:
                    subsumed = True
                    break
            if not subsumed:
                result.append(vc)
        return result

    @classmethod
    def combine(cls, vcs):
        result = VectorClock()
        for vc in vcs:
            for node, counter in vc.clock.items():
                if node in result.clock:
                    if result.clock[node] < counter:
                        result.clock[node] = counter
                else:
                    result.clock[node] = counter
        return result

# -----------IGNOREBEYOND: test code ---------------
import unittest
import copy

class VectorClockTestCase(unittest.TestCase):
    """Test vector clock class"""

    def setUp(self):
        self.c1 = VectorClock()
        self.c1.update('A', 1)
        self.c2 = VectorClock()
        self.c2.update('B', 2)
        
    def testSmall(self):
        self.assertEquals(str(self.c1), "{A:1}")
        self.c1.update('A',2)
        self.assertEquals(str(self.c1), "{A:2}")
        self.c1.update('A',200)
        self.assertEquals(str(self.c1), "{A:200}")
        self.c1.update('B',1)
        self.assertEquals(str(self.c1), "{A:200,B:1}")

    def testInternalError(self):
        self.assertRaises(Exception, self.c2.update, 'B', 1)

    def testEquality(self):
        self.assertEquals(self.c1==self.c2, False)
        self.assertEquals(self.c1!=self.c2, True)
        self.c1.update('B',2)
        self.c2.update('A',1)
        self.assertEquals(self.c1==self.c2, True)
        self.assertEquals(self.c1!=self.c2, False)

    def testOrder(self):
        self.assertEquals(self.c1 < self.c2, False)
        self.assertEquals(self.c2 < self.c1, False)
        self.assertEquals(self.c1 <= self.c2, False)
        self.assertEquals(self.c2 <= self.c1, False)
        self.c1.update('B',2)
        self.assertEquals(self.c1 < self.c2, False)
        self.assertEquals(self.c2 < self.c1, True)
        self.assertEquals(self.c1 <= self.c2, False)
        self.assertEquals(self.c2 <= self.c1, True)
        self.assertEquals(self.c1 > self.c2, True)
        self.assertEquals(self.c2 > self.c1, False)
        self.assertEquals(self.c1 >= self.c2, True)
        self.assertEquals(self.c2 >= self.c1, False)

    def testCoalesce(self):
        self.c1.update('B', 2)
        self.assertEquals(VectorClock.coalesce((self.c1, self.c1, self.c1)), [self.c1])
        c3 = copy.deepcopy(self.c1)
        c4 = copy.deepcopy(self.c1)
        # Diverge the two clocks
        c3.update('X',200)
        self.c1.update('Y',100)
        self.assertEquals(VectorClock.coalesce((self.c1, c3, c4)), [self.c1, c3])

    def testCombine(self):
        self.c1.update('B', 1)
        c3 = copy.deepcopy(self.c1)
        c4 = copy.deepcopy(self.c1)
        # Diverge two of the clocks
        c3.update('X',200)
        self.c1.update('Y',100)
        cx = VectorClock.combine((self.c1, self.c2, c3, c4))
        self.assertEquals(str(cx), "{A:1,B:2,X:200,Y:100}")
        cy = VectorClock.combine(VectorClock.coalesce((self.c1, self.c2, c3, c4)))
        self.assertEquals(str(cy), "{A:1,B:2,X:200,Y:100}")


if __name__ == "__main__":
    unittest.main()
