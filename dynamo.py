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
# PART 2
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
        self.pending_put_msg = {} # (key, sequence) => original client message
        self.pending_get = {} # key => set of (node, value, metadata) tuples
        # Rebuild the consistent hash table 
        DynamoNode.nodelist.append(self)
        DynamoNode.chash = ConsistentHashTable(DynamoNode.nodelist, DynamoNode.T)
# PART 3
    def rcv_clientput(self, msg):
        preference_list = DynamoNode.chash.find_nodes(msg.key, DynamoNode.N)
        # Determine if we are in the list
        if self not in preference_list:
            # Forward to the coordinator for this key
            _logger.info("put(%s=%s) maps to %s", msg.key, msg.value, preference_list)
            coordinator = preference_list[0]
            Framework.forward_message(msg, coordinator)
        else:
            # Store locally, using an incrementing local sequence number to 
            # distinguish multiple requests for the same key
            seqno = self.generate_sequence_number()
            _logger.info("%s, %d: store %s=%s", self, seqno, msg.key, msg.value)
            metadata = seqno # Override client metadata; this will be replaced with a vector clock later
            self.store[msg.key] = (msg.value, metadata)
            # Send out to other nodes, and keep track of who has replied
            self.pending_put[(msg.key, seqno)] = set([self])
            self.pending_put_msg[(msg.key, seqno)] = msg
            reqcount = 1
            for node in preference_list:
                if node != self:
                    # Send message to get other node in preference list to store
                    putmsg = PutReq(self, node, msg.key, msg.value, metadata)
                    Framework.send_message(putmsg)
                    reqcount = reqcount + 1
                if reqcount >= DynamoNode.N:
                    # preference_list may have more than N entries to allow for failed nodes
                    break
# PART 4
    def get(self, key):
        preference_list = DynamoNode.chash.find_nodes(key, DynamoNode.N)
        self.pending_get[key] = set()
        reqcount = 0
        for node in preference_list:
            getmsg = GetReq(self, node, key)
            Framework.send_message(getmsg)
            reqcount = reqcount + 1
            if reqcount >= DynamoNode.N:
                # preference_list may have more than N entries to allow for failed nodes
                break
# PART 5
    def rcv_put(self, putmsg):
        _logger.info("%s: store %s=%s", self, putmsg.key, putmsg.value)
        self.store[putmsg.key] = (putmsg.value, putmsg.metadata)
        putrsp = PutRsp(putmsg)
        Framework.send_message(putrsp)
# PART 6
    def rcv_putrsp(self, putrsp):
        seqno = putrsp.metadata # replace with vector clock
        if (putrsp.key, seqno) in self.pending_put:
            self.pending_put[(putrsp.key, seqno)].add(putrsp.from_node)
            if len(self.pending_put[(putrsp.key, seqno)]) >= DynamoNode.W:
                _logger.info("%s: written %d copies of %s=%s so done", self, DynamoNode.W, putrsp.key, putrsp.value)
                _logger.debug("  copies at %s", [node.name for node in self.pending_put[(putrsp.key, seqno)]])
                original_msg = self.pending_put_msg[(putrsp.key, seqno)]
                del self.pending_put[(putrsp.key, seqno)]
                del self.pending_put_msg[(putrsp.key, seqno)]
                # Reply to the original client
                client_putrsp = ClientPutRsp(original_msg)
                Framework.send_message(client_putrsp)
        else:
            pass # Superfluous reply
# PART 7
    def rcv_get(self, getmsg):
        _logger.info("%s: retrieve %s=?", self, getmsg.key)
        if getmsg.key in self.store:
            (value, metadata) = self.store[getmsg.key]
            getrsp = GetRsp(getmsg, value, metadata)
            Framework.send_message(getrsp)
# PART 8
    def rcv_getrsp(self, getrsp):
        if getrsp.key in self.pending_get:
            self.pending_get[getrsp.key].add((getrsp.from_node, getrsp.value, getrsp.metadata))
            if len(self.pending_get[getrsp.key]) >= DynamoNode.R:
                _logger.info("%s: read %d copies of %s=? so done", self, DynamoNode.R, getrsp.key)
                _logger.debug("  copies at %s", [(node.name,value) for (node,value,_) in self.pending_get[getrsp.key]])
                del self.pending_get[getrsp.key]
# PART 9
    def rcvmsg(self, msg):
        if isinstance(msg, ClientPut): self.rcv_clientput(msg)
        elif isinstance(msg, PutReq): self.rcv_put(msg)
        elif isinstance(msg, PutRsp): self.rcv_putrsp(msg)
        elif isinstance(msg, ClientGet): self.get(msg.key)
        elif isinstance(msg, GetReq): self.rcv_get(msg)
        elif isinstance(msg, GetRsp): self.rcv_getrsp(msg)
        else: raise TypeError("Unexpected message type %s", msg.__class__)
# PART 10
class DynamoClientNode(Node):
    def put(self, key, metadata, value, destnode=None):
        if destnode is None: # Pick a random node to send the request to
            destnode = random.choice(DynamoNode.nodelist)
        putmsg = ClientPut(self, destnode, key, value, metadata)
        Framework.send_message(putmsg)
    def get(self, key, destnode=None):
        if destnode is None: # Pick a random node to send the request to
            destnode = random.choice(DynamoNode.nodelist)
        putmsg = ClientGet(self, destnode, key)
        Framework.send_message(putmsg)
# PART 11
    def rcvmsg(self, msg):
        pass # @@@
# PART 12
for _ in range(50):
    DynamoNode()
a = DynamoClientNode('a')
a.put('K1', None, 1)
Framework.schedule()
a.get('K1')
Framework.schedule()
from history import History
print History.ladder()
