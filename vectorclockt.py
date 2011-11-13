#!/usr/bin/env python
"""Vector clock class with truncation support"""
import sys
import time
from vectorclock import VectorClock


class VectorClockTimestamp(VectorClock):
    NODE_LIMIT = 10

    def __init__(self):
        super(VectorClockTimestamp, self).__init__()
        self.clock_time = {}  # node => timestamp

    def _maybe_truncate(self):
        if len(self.clock_time) < VectorClockTimestamp.NODE_LIMIT:
            return
        # Find the oldest entry
        oldest_node = None
        oldest_time = sys.maxint
        for node, when in self.clock_time.items():
            if when < oldest_time:
                oldest_node = node
                oldest_time = when
        del self.clock_time[oldest_node]
        del self.clock[oldest_node]

    def update(self, node, counter):
        VectorClock.update(self, node, counter)
        self.clock_time[node] = time.time()
        self._maybe_truncate()
        return self

# -----------IGNOREBEYOND: test code ---------------
import unittest
import copy


class VectorClockTimestampTestCase(unittest.TestCase):
    """Test vector clock class"""

    def setUp(self):
        VectorClockTimestamp.NODE_LIMIT = 3
        self.c1 = VectorClockTimestamp()
        self.c1.update('A', 1)
        self.c2 = VectorClockTimestamp()
        self.c2.update('B', 2)

    def testSmall(self):
        self.assertEquals(str(self.c1), "{A:1}")
        self.c1.update('A', 2)
        self.assertEquals(str(self.c1), "{A:2}")
        self.c1.update('A', 200)
        self.assertEquals(str(self.c1), "{A:200}")
        self.c1.update('B', 1)
        self.assertEquals(str(self.c1), "{A:200, B:1}")
        self.c1.update('C', 4)
        self.assertEquals(str(self.c1), "{B:1, C:4}")

    def testInternalError(self):
        self.assertRaises(Exception, self.c2.update, 'B', 1)

    def testEquality(self):
        self.assertEquals(self.c1 == self.c2, False)
        self.assertEquals(self.c1 != self.c2, True)
        self.c1.update('B', 2)
        self.c2.update('A', 1)
        self.assertEquals(self.c1 == self.c2, True)
        self.assertEquals(self.c1 != self.c2, False)

    def testOrder(self):
        self.assertEquals(self.c1 < self.c2, False)
        self.assertEquals(self.c2 < self.c1, False)
        self.assertEquals(self.c1 <= self.c2, False)
        self.assertEquals(self.c2 <= self.c1, False)
        self.c1.update('B', 2)
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
        self.assertEquals(VectorClockTimestamp.coalesce((self.c1, self.c1, self.c1)), [self.c1])
        c3 = copy.deepcopy(self.c1)
        c4 = copy.deepcopy(self.c1)
        # Diverge the two clocks
        c3.update('X', 200)
        c4.update('Y', 100)
        # Now sufficient updates that first entry is lost.
        self.assertEquals(VectorClockTimestamp.coalesce(((self.c1, c3, c4))), [self.c1, c3, c4])
        self.assertEquals(VectorClockTimestamp.coalesce((c3, self.c1, c3, c4)), [c3, self.c1, c4])

    def testConverge(self):
        self.c1.update('B', 1)
        c3 = copy.deepcopy(self.c1)
        c4 = copy.deepcopy(self.c1)
        # Diverge two of the clocks
        c3.update('X', 200)
        self.c1.update('Y', 100)
        cx = VectorClockTimestamp.converge((self.c1, self.c2, c3, c4))
        self.assertEquals(str(cx), "{A:1, B:2, X:200, Y:100}")
        cy = VectorClockTimestamp.converge(VectorClock.coalesce((self.c1, self.c2, c3, c4)))
        self.assertEquals(str(cy), "{A:1, B:2, X:200, Y:100}")


if __name__ == "__main__":
    unittest.main()
