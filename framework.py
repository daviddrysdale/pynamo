#!/usr/bin/env python
import sys
import logging
from collections import deque

from node import Node
from history import History
from message import Message
from timer import Timer
import logconfig

_logger = logging.getLogger('dynamo')

class Framework:
    cuts = [] # List of incommunicado sets of nodes
    queue = deque([]) # queue of pending messages
    
    @classmethod
    def reset():
        cuts = []
        queue = deque()

    @classmethod
    def cut_wires(cls, from_nodes, to_nodes):
        cls.cuts.append((from_nodes, to_nodes))
    
    @classmethod
    def reachable(cls, from_node, to_node):
        for (from_nodes, to_nodes) in cls.cuts:
            if from_node in from_nodes and to_node in to_nodes:
                return False
        return True
    
    @classmethod
    def send_message(cls, msg):
        """Send a message"""
        _logger.info("Enqueue %s->%s: %s", msg.from_node, msg.to_node, msg)
        cls.queue.append(msg)
        History.add("send", msg)
    
    @classmethod
    def schedule(cls, num_to_process=32768):
        """Schedule given number of pending messages"""
        while cls.queue and num_to_process > 0:
            msg = cls.queue.popleft()
            if msg.to_node.failed:
                _logger.info("Drop %s->%s: %s as destination down", msg.from_node, msg.to_node, msg)
                History.add("drop", msg)
            elif not Framework.reachable(msg.from_node, msg.to_node):
                _logger.info("Drop %s->%s: %s as route down", msg.from_node, msg.to_node, msg)
                History.add("cut", msg)
            else:
                _logger.info("Dequeue %s->%s: %s", msg.from_node, msg.to_node, msg)
                History.add("deliver", msg)
                msg.to_node.rcvmsg(msg)
            num_to_process = num_to_process - 1

def reset():
    """Reset all message and other history"""
    Framework.reset()
    Timer.reset()
    History.reset()
def reset_all():
    """Reset all message and other history, and remove all nodes"""
    reset()
    Node.reset()

