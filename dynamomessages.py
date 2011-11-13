"""Messages between Dynamo nodes"""
from message import Message, ResponseMessage


class DynamoRequestMessage(Message):
    """Base class for Dynamo request messages; all include the key for the data object in question"""
    def __init__(self, from_node, to_node, key, msg_id=None):
        Message.__init__(self, from_node, to_node, msg_id=msg_id)
        self.key = key

    def __str__(self):
        return "%s %s=?" % (Message.__str__(self), self.key)


class DynamoResponseMessage(ResponseMessage):
    """Base class for Dynamo response messages; all include key and value (plus metadata)"""
    def __init__(self, req, value, metadata):
        ResponseMessage.__init__(self, req)
        self.key = req.key
        self.value = value
        self.metadata = metadata

    def __str__(self):
        return "%s %s=%s" % (Message.__str__(self), self.key, self.value)


class ClientPut(DynamoRequestMessage):
    def __init__(self, from_node, to_node, key, value, metadata, msg_id=None):
        DynamoRequestMessage.__init__(self, from_node, to_node, key, msg_id=msg_id)
        self.value = value
        self.metadata = metadata

    def __str__(self):
        return "ClientPut(%s=%s)" % (self.key, self.value)


class ClientPutRsp(DynamoResponseMessage):
    def __init__(self, req):
        DynamoResponseMessage.__init__(self, req, req.value, req.metadata)

    def __str__(self):
        return "ClientPutRsp(%s=%s)" % (self.key, self.value)


class PutReq(DynamoRequestMessage):
    def __init__(self, from_node, to_node, key, value, metadata, msg_id=None):
        DynamoRequestMessage.__init__(self, from_node, to_node, key, msg_id)
        self.value = value
        self.metadata = metadata

    def __str__(self):
        return "PutReq(%s=%s)" % (self.key, self.value)


class PutRsp(DynamoResponseMessage):
    def __init__(self, req):
        DynamoResponseMessage.__init__(self, req, req.value, req.metadata)

    def __str__(self):
        return "PutRsp(%s=%s)" % (self.key, self.value)


class ClientGet(DynamoRequestMessage):
    def __str__(self):
        return "ClientGet(%s=?)" % self.key


class ClientGetRsp(DynamoResponseMessage):
    def __init__(self, req, values, metadatas):
        DynamoResponseMessage.__init__(self, req, values, metadatas)

    def __str__(self):
        return "ClientGetRsp(%s=%s)" % (self.key, self.value)


class GetReq(DynamoRequestMessage):
    def __str__(self):
        return "GetReq(%s=?)" % self.key


class GetRsp(DynamoResponseMessage):
    def __str__(self):
        return "GetRsp(%s=%s)" % (self.key, self.value)


class PingReq(Message):
    def __str__(self):
        return "PingReq"


class PingRsp(ResponseMessage):
    def __str__(self):
        return "PingRsp"
