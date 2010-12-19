#!/usr/bin/env python
"""Implementation of Dynamo"""
import sys
import logging

import logconfig
from node import Node
from framework import Framework
from hash_multiple import ConsistentHashTable
from message import Message
_logger = logging.getLogger('dynamo')

class DynamoMessage(Message):
    def __init__(self, from_node, to_node, key, value, metadata):
        Message.__init__(self, from_node, to_node)
        self.key = key
        self.value = value
        self.metadata = metadata
    def __str__(self):
        return "%s %s=%s" % (Message.__str__(self), self.key, self.value)

class DynamoResponse(DynamoMessage):
    def __init__(self, req):
        DynamoMessage.__init__(self, req.to_node, req.from_node, req.key, req.value, req.metadata)

class PutFwd(DynamoMessage):
    def __str__(self):
        return "%s PutFwd(%s=%s)" % (Message.__str__(self), self.key, self.value)

class PutReq(DynamoMessage):
    def __str__(self):
        return "%s PutReq(%s=%s)" % (Message.__str__(self), self.key, self.value)

class PutRsp(DynamoResponse):
    def __str__(self):
        return "%s PutRsp(%s=%s)" % (Message.__str__(self), self.key, self.value)
        
class DynamoNode(Node):
    T = 10 # Number of "tokens"/"virtual nodes"/"repeats" in consistent hash table
    N = 3 # Number of nodes to replicate at
    W = 2 # Number of nodes that need to reply to a write operation
    R = 2 # Number of nodes that need to reply to a read operation
    chash = ConsistentHashTable((), T)

    def __init__(self):
        Node.__init__(self)
        self.store = {} # key => (value, metadata)
        # Rebuild the consistent hash table
        DynamoNode.chash = ConsistentHashTable(Node.name.keys(), DynamoNode.T)

    def put(self, key, value, metadata):
        nodelist = DynamoNode.chash.find_nodes(key, DynamoNode.N)
        # Determine if we are in the list
        if self not in nodelist:
            # Forward to the coordinator for this key
            _logger.info("put(%s=%s) maps to %s", key, value, nodelist)
            putmsg = PutFwd(self, nodelist[0], key, value, metadata)
            Framework.send_message(putmsg)
        else:
            # Store locally
            self.store[key] = (value, metadata)
            for node in nodelist:
                if node != self:
                    putmsg = PutReq(self, node, key, value, metadata)
                    Framework.send_message(putmsg)
            

    def rcvmsg(self, msg):
        if isinstance(msg, PutFwd):
            self.put(msg.key, msg.value, msg.metadata)
        elif isinstance(msg, PutReq):
            self.store[msg.key] = (msg.value, msg.metadata)
            putrsp = PutRsp(msg)
            Framework.send_message(putrsp)
        elif isinstance(msg, PutRsp):
            pass
        else: 
            raise TypeError("Unexpected message type %s", msg.__class__)
            
            

a = DynamoNode()
for _ in range(49):
    DynamoNode()
a.put('A', 1, None)
Framework.schedule()
from history import History
print History.ladder()
