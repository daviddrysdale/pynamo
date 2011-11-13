"""Utility functions for testing"""
import math
import random


def random_3letters():
    return (chr(ord('A') + random.randint(0, 25)) +
            chr(ord('A') + random.randint(0, 25)) +
            chr(ord('A') + random.randint(0, 25)))


class Stats(object):
    def __init__(self):
        self.total = 0.0
        self.values = []
        self._mean = None
        self._variance = None

    def add(self, value):
        self.values.append(float(value))
        self.total = self.total + float(value)
        self._mean = None
        self._variance = None

    def mean(self):
        if self._mean is None:
            self._mean = self.total / float(len(self.values))
        return self._mean

    def variance(self):
        if self._variance is None:
            mean = self.mean()
            self._variance = 0.0
            for value in self.values:
                diffval = (value - mean)
                self._variance = self._variance + (diffval * diffval)
            self._variance = self._variance / float(len(self.values) - 1)
        return self._variance

    def stddev(self):
        return math.sqrt(self.variance())
