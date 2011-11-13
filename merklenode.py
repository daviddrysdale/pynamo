"""Node implementing a Merkle tree"""
import logging

import logconfig
from node import Node
from timer import Timer
from framework import Framework
from merklemessages import @@@
from merkle import MerkleTree

logconfig.init_logging()
_logger = logging.getLogger('merkle')


# PART merklenode
class DynamoNode(Node):

    def __init__(self):
        Node.__init__(self)

    def rcvmsg(self, msg):
        if isinstance(msg, ClientPut):
            self.rcv_clientput(msg)
        else:
            raise TypeError("Unexpected message type %s", msg.__class__)