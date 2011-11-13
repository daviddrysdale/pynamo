"""Messages between Merkle nodes"""
from message import Message, ResponseMessage


class MerkleRequestMessage(Message):
    """Base class for Merkle request messages; all include the tree config info"""
    def __init__(self, from_node, to_node, depth, min_key, max_key, msg_id=None):
        super(MerkleRequestMessage, self).__init__(from_node, to_node, msg_id=msg_id)
        self.depth = depth
        self.min_key = min_key
        self.max_key = max_key

    def __str__(self):
        return "%s |%s| [%s,%s)" % (Message.__str__(self), self.depth, self.min_key, self.max_key)


class MerkleResponseMessage(ResponseMessage):
    """Base class for Merkle response messages; all include the tree config info"""
    def __init__(self, req):
        super(MerkleResponseMessage, self).__init__(req)
        self.depth = req.depth
        self.min_key = req.min_key
        self.max_key = req.max_key

    def __str__(self):
        return "%s |%s| [%s,%s)" % (Message.__str__(self), self.depth, self.min_key, self.max_key)
