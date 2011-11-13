"""Implementation of Dynamo"""
import random
import logging

import logconfig
from node import Node
from timer import TimerManager
from framework import Framework
from hash_multiple import ConsistentHashTable
from dynamomessages import ClientPut, ClientGet, ClientPutRsp, ClientGetRsp
from dynamomessages import PutReq, GetReq, PutRsp, GetRsp
from dynamomessages import PingReq, PingRsp, DynamoRequestMessage
from merkle import MerkleTree

logconfig.init_logging()
_logger = logging.getLogger('dynamo')


# PART dynamonode
class DynamoNode(Node):
    timer_priority = 20
    T = 10  # Number of repeats for nodes in consistent hash table
    N = 3  # Number of nodes to replicate at
    W = 2  # Number of nodes that need to reply to a write operation
    R = 2  # Number of nodes that need to reply to a read operation
    nodelist = []
    chash = ConsistentHashTable(nodelist, T)

    def __init__(self):
        super(DynamoNode, self).__init__()
        self.local_store = MerkleTree()  # key => (value, metadata)
        self.pending_put_req = {}  # seqno => set of requests sent to other nodes
        self.pending_put_rsp = {}  # seqno => set of nodes that have stored
        self.pending_put_msg = {}  # seqno => original client message
        self.pending_get_req = {}  # seqno => set of requests sent to other nodes
        self.pending_get_rsp = {}  # seqno => set of (node, value, metadata) tuples
        self.pending_get_msg = {}  # seqno => original client message
        self.failed_nodes = []
        # Rebuild the consistent hash table
        DynamoNode.nodelist.append(self)
        DynamoNode.chash = ConsistentHashTable(DynamoNode.nodelist, DynamoNode.T)
        # Run a timer to retry failed nodes
        self.retry_failed_node("retry")

# PART storage
    def store(self, key, value, metadata):
        self.local_store[key] = (value, metadata)

    def retrieve(self, key):
        if key in self.local_store:
            return self.local_store[key]
        else:
            return (None, None)

# PART retry_failed_node
    def retry_failed_node(self, _):  # Permanently repeating timer
        if self.failed_nodes:
            node = self.failed_nodes.pop(0)
            # Send a test message to the oldest failed node
            pingmsg = PingReq(self, node)
            Framework.send_message(pingmsg)
        # Restart the timer
        TimerManager.start_timer(self, reason="retry", priority=15, callback=self.retry_failed_node)

    def rcv_pingreq(self, pingmsg):
        # Always reply to a test message
        pingrsp = PingRsp(pingmsg)
        Framework.send_message(pingrsp)

    def rcv_pingrsp(self, pingmsg):
        # Remove all instances of recovered node from failed node list
        while pingmsg.from_node in self.failed_nodes:
            self.failed_nodes.remove(pingmsg.from_node)

# PART rsp_timer_pop
    def rsp_timer_pop(self, reqmsg):
        self.failed_nodes.append(reqmsg.to_node)
        if not isinstance(reqmsg, DynamoRequestMessage):
            return
        # Send the request to an additional node by regenerating the preference list
        preference_list = DynamoNode.chash.find_nodes(reqmsg.key, DynamoNode.N, self.failed_nodes)
        for node in preference_list:
            if isinstance(reqmsg, PutReq) and reqmsg.msg_id in self.pending_put_req:
                sent_nodes = [req.to_node for req in self.pending_put_req[reqmsg.msg_id]]
                sent_nodes.append(self)
                if node not in sent_nodes:
                    putmsg = PutReq(self, node, reqmsg.key, reqmsg.value, reqmsg.metadata, msg_id=reqmsg.msg_id)
                    self.pending_put_req[reqmsg.msg_id].add(putmsg)
                    Framework.send_message(putmsg)
            elif isinstance(reqmsg, GetReq) and reqmsg.msg_id in self.pending_get_req:
                sent_nodes = [req.to_node for req in self.pending_get_req[reqmsg.msg_id]]
                if node not in sent_nodes:
                    getmsg = GetReq(self, node, reqmsg.key, msg_id=reqmsg.msg_id)
                    self.pending_get_req[reqmsg.msg_id].add(getmsg)
                    Framework.send_message(getmsg)

# PART rcv_clientput
    def rcv_clientput(self, msg):
        preference_list = DynamoNode.chash.find_nodes(msg.key, DynamoNode.N, self.failed_nodes)
        # Determine if we are in the list
        if self not in preference_list:
            # Forward to the coordinator for this key
            _logger.info("put(%s=%s) maps to %s", msg.key, msg.value, preference_list)
            coordinator = preference_list[0]
            Framework.forward_message(msg, coordinator)
        else:
            # Use an incrementing local sequence number to distinguish
            # multiple requests for the same key
            seqno = self.generate_sequence_number()
            _logger.info("%s, %d: put %s=%s", self, seqno, msg.key, msg.value)
            metadata = (self.name, seqno)  # For now, metadata is just sequence number at coordinator
            self.pending_put_req[seqno] = set()
            # Send out to preference list, and keep track of who has replied
            self.pending_put_rsp[seqno] = set()
            self.pending_put_msg[seqno] = msg
            reqcount = 0
            for node in preference_list:
                    self.pending_put_req[seqno].add(putmsg)
                # Send message to get node in preference list to store
                putmsg = PutReq(self, node, msg.key, msg.value, metadata, msg_id=seqno)
                Framework.send_message(putmsg)
                reqcount = reqcount + 1
                if reqcount >= DynamoNode.N:
                    # preference_list may have more than N entries to allow for failed nodes
                    break

# PART rcv_clientget
    def rcv_clientget(self, msg):
        preference_list = DynamoNode.chash.find_nodes(msg.key, DynamoNode.N, self.failed_nodes)
        seqno = self.generate_sequence_number()
        self.pending_get_req[seqno] = set()
        self.pending_get_rsp[seqno] = set()
        self.pending_get_msg[seqno] = msg
        reqcount = 0
        for node in preference_list:
            getmsg = GetReq(self, node, msg.key, msg_id=seqno)
            self.pending_get_req[seqno].add(getmsg)
            Framework.send_message(getmsg)
            reqcount = reqcount + 1
            if reqcount >= DynamoNode.N:
                # preference_list may have more than N entries to allow for failed nodes
                break

# PART rcv_put
    def rcv_put(self, putmsg):
        _logger.info("%s: store %s=%s", self, putmsg.key, putmsg.value)
        self.store(putmsg.key, putmsg.value, putmsg.metadata)
        putrsp = PutRsp(putmsg)
        Framework.send_message(putrsp)

# PART rcv_putrsp
    def rcv_putrsp(self, putrsp):
        seqno = putrsp.msg_id
        if seqno in self.pending_put_rsp:
            self.pending_put_rsp[seqno].add(putrsp.from_node)
            if len(self.pending_put_rsp[seqno]) >= DynamoNode.W:
                _logger.info("%s: written %d copies of %s=%s so done", self, DynamoNode.W, putrsp.key, putrsp.value)
                _logger.debug("  copies at %s", [node.name for node in self.pending_put_rsp[seqno]])
                # Tidy up tracking data structures
                original_msg = self.pending_put_msg[seqno]
                del self.pending_put_req[seqno]
                del self.pending_put_rsp[seqno]
                del self.pending_put_msg[seqno]
                # Reply to the original client
                client_putrsp = ClientPutRsp(original_msg)
                Framework.send_message(client_putrsp)
        else:
            pass  # Superfluous reply

# PART rcv_get
    def rcv_get(self, getmsg):
        _logger.info("%s: retrieve %s=?", self, getmsg.key)
        (value, metadata) = self.retrieve(getmsg.key)
        if value is not None:
            getrsp = GetRsp(getmsg, value, metadata)
            Framework.send_message(getrsp)

# PART rcv_getrsp
    def rcv_getrsp(self, getrsp):
        seqno = getrsp.msg_id
        if seqno in self.pending_get_rsp:
            self.pending_get_rsp[seqno].add((getrsp.from_node, getrsp.value, getrsp.metadata))
            if len(self.pending_get_rsp[seqno]) >= DynamoNode.R:
                _logger.info("%s: read %d copies of %s=? so done", self, DynamoNode.R, getrsp.key)
                _logger.debug("  copies at %s", [(node.name, value) for (node, value, _) in self.pending_get_rsp[seqno]])
                # Build up all the distinct values/metadata values for the response to the original request
                results = set([(value, metadata) for (node, value, metadata) in self.pending_get_rsp[seqno]])
                # Tidy up tracking data structures
                original_msg = self.pending_get_msg[seqno]
                del self.pending_get_req[seqno]
                del self.pending_get_rsp[seqno]
                del self.pending_get_msg[seqno]
                # Reply to the original client, including all received values
                client_getrsp = ClientGetRsp(original_msg,
                                             [value for (value, metadata) in results],
                                             [metadata for (value, metadata) in results])
                Framework.send_message(client_getrsp)
        else:
            pass  # Superfluous reply

# PART rcvmsg
    def rcvmsg(self, msg):
        if isinstance(msg, ClientPut):
            self.rcv_clientput(msg)
        elif isinstance(msg, PutReq):
            self.rcv_put(msg)
        elif isinstance(msg, PutRsp):
            self.rcv_putrsp(msg)
        elif isinstance(msg, ClientGet):
            self.rcv_clientget(msg)
        elif isinstance(msg, GetReq):
            self.rcv_get(msg)
        elif isinstance(msg, GetRsp):
            self.rcv_getrsp(msg)
        elif isinstance(msg, PingReq):
            self.rcv_pingreq(msg)
        elif isinstance(msg, PingRsp):
            self.rcv_pingrsp(msg)
        else:
            raise TypeError("Unexpected message type %s", msg.__class__)

# PART get_contents
    def get_contents(self):
        results = []
        for key, value in self.local_store.items():
            results.append("%s:%s" % (key, value[0]))
        return results


# PART clientnode
class DynamoClientNode(Node):
    def put(self, key, metadata, value, destnode=None):
        if destnode is None:  # Pick a random node to send the request to
            destnode = random.choice(DynamoNode.nodelist)
        putmsg = ClientPut(self, destnode, key, value, metadata)
        Framework.send_message(putmsg)

    def get(self, key, destnode=None):
        if destnode is None:  # Pick a random node to send the request to
            destnode = random.choice(DynamoNode.nodelist)
        getmsg = ClientGet(self, destnode, key)
        Framework.send_message(getmsg)

    def rsp_timer_pop(self, reqmsg):
        if isinstance(reqmsg, ClientPut):  # retry
            _logger.info("Put request timed out; retrying")
            self.put(reqmsg.key, reqmsg.metadata, reqmsg.value)
        elif isinstance(reqmsg, ClientGet):  # retry
            _logger.info("Get request timed out; retrying")
            self.get(reqmsg.key)

# PART clientrcvmsg
    def rcvmsg(self, msg):
        pass  # Client does nothing with results
