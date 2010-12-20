#!/usr/bin/env python
"""Implementation of Dynamo"""
import sys
import random
import logging

import logconfig
from node import Node
from framework import Framework
from hash_multiple import ConsistentHashTable
from dynamomessages import *
_logger = logging.getLogger('dynamo')

class DynamoNode(Node):
    T = 10 # Number of "tokens"/"virtual nodes"/"repeats" in consistent hash table
    N = 3 # Number of nodes to replicate at
    W = 2 # Number of nodes that need to reply to a write operation
    R = 2 # Number of nodes that need to reply to a read operation
    nodelist = []
    chash = ConsistentHashTable(nodelist, T)

    def __init__(self):
        Node.__init__(self)
        self.store = {} # key => (value, metadata)
        self.pending_put = {} # (key, sequence) => set of nodes that have stored
        self.pending_get = {} # key => set of (node, value, metadata) tuples
        # Rebuild the consistent hash table 
        DynamoNode.nodelist.append(self)
        DynamoNode.chash = ConsistentHashTable(DynamoNode.nodelist, DynamoNode.T)

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
            seqno = self.generate_sequence_number()
            _logger.info("%s, %d: store %s=%s", self, seqno, key, value)
            metadata = seqno # @@@ replace with vector clock
            self.store[key] = (value, metadata)
            self.pending_put[(key, seqno)] = set([self])
            reqcount = 1
            for node in nodelist:
                if node != self:
                    # Send message to get other node in preference list to store
                    putmsg = PutReq(self, node, key, value, metadata)
                    Framework.send_message(putmsg)
                    reqcount = reqcount + 1
                if reqcount >= DynamoNode.N:
                    # nodelist may have more than N entries to allow for failures
                    break
            
    def get(self, key):
        nodelist = DynamoNode.chash.find_nodes(key, DynamoNode.N)
        self.pending_get[key] = set()
        reqcount = 0
        for node in nodelist:
            getmsg = GetReq(self, node, key)
            Framework.send_message(getmsg)
            reqcount = reqcount + 1
            if reqcount >= DynamoNode.N:
                # nodelist may have more than N entries to allow for failures
                break
            
    def rcvmsg(self, msg):
        if isinstance(msg, PutFwd):
            self.put(msg.key, msg.value, msg.metadata)

        elif isinstance(msg, PutReq):
            _logger.info("%s: store %s=%s", self, msg.key, msg.value)
            self.store[msg.key] = (msg.value, msg.metadata)
            putrsp = PutRsp(msg)
            Framework.send_message(putrsp)

        elif isinstance(msg, PutRsp):
            seqno = msg.metadata # replace with vector clock
            if (msg.key, seqno) in self.pending_put:
                self.pending_put[(msg.key, seqno)].add(msg.from_node)
                if len(self.pending_put[(msg.key, seqno)]) >= DynamoNode.W:
                    _logger.info("%s: written %d copies of %s=%s so done", self, DynamoNode.W, msg.key, msg.value)
                    _logger.debug("  copies at %s", [node.name for node in self.pending_put[(msg.key, seqno)]])
                    del self.pending_put[(msg.key, seqno)]

        elif isinstance(msg, GetFwd):
            self.get(msg.key)

        elif isinstance(msg, GetReq):
            _logger.info("%s: retrieve %s=?", self, msg.key)
            if msg.key in self.store:
                (value, metadata) = self.store[msg.key]
                getrsp = GetRsp(msg, value, metadata)
                Framework.send_message(getrsp)
            
        elif isinstance(msg, GetRsp):
            if msg.key in self.pending_get:
                self.pending_get[msg.key].add((msg.from_node, msg.value, msg.metadata))
                if len(self.pending_get[msg.key]) >= DynamoNode.R:
                    _logger.info("%s: read %d copies of %s=? so done", self, DynamoNode.R, msg.key)
                    _logger.debug("  copies at %s", [(node.name,value) for (node,value,_) in self.pending_get[msg.key]])
                    del self.pending_get[msg.key]

        else: 
            raise TypeError("Unexpected message type %s", msg.__class__)
        

class DynamoClientNode(Node):
    def __init__(self, name):
        Node.__init__(self, name)
    def put(self, key, value, metadata, destnode=None):
        if destnode is None:
            # Pick a random node to send the request to
            destnode = random.choice(DynamoNode.nodelist)
        putmsg = PutFwd(self, destnode, key, value, metadata)
        Framework.send_message(putmsg)
    def get(self, key, destnode=None):
        if destnode is None:
            # Pick a random node to send the request to
            destnode = random.choice(DynamoNode.nodelist)
        putmsg = GetFwd(self, destnode, key)
        Framework.send_message(putmsg)
    

for _ in range(50):
    DynamoNode()
a = DynamoClientNode('a')
a.put('A', 1, None)
Framework.schedule()
a.get('A')
Framework.schedule()
from history import History
print History.ladder()
