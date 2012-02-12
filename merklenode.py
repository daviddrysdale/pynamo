"""Node implementing a Merkle tree"""
import logging

import logconfig
from node import Node

logconfig.init_logging()
_logger = logging.getLogger('merkle')


# PART merklenode
class MerkleNode(Node):
    def __init__(self):
        super(MerkleNode, self).__init__()

    def rcvmsg(self, msg):
        pass
