"""Messages between Dynamo nodes"""
from message import Message, ResponseMessage


class DynamoRequestMessage(Message):
    """Base class for Dynamo request messages; all include the key for the data object in question"""
    def __init__(self, from_node, to_node, key, msg_id=None):
        super(DynamoRequestMessage, self).__init__(from_node, to_node, msg_id=msg_id)
        self.key = key

    def __str__(self):
        return "%s(%s=?)" % (self.__class__.__name__, self.key)


class DynamoResponseMessage(ResponseMessage):
    """Base class for Dynamo response messages; all include key and value (plus metadata)"""
    def __init__(self, req, value, metadata):
        super(DynamoResponseMessage, self).__init__(req)
        self.key = req.key
        self.value = value
        self.metadata = metadata

    def __str__(self):
        return "%s(%s=%s)" % (self.__class__.__name__, self.key, self.value)


class ClientPut(DynamoRequestMessage):
    def __init__(self, from_node, to_node, key, value, metadata, msg_id=None):
        super(ClientPut, self).__init__(from_node, to_node, key, msg_id=msg_id)
        self.value = value
        self.metadata = metadata

    def __str__(self):
        return "ClientPut(%s=%s)" % (self.key, self.value)


class ClientPutRsp(DynamoResponseMessage):
    def __init__(self, req):
        super(ClientPutRsp, self).__init__(req, req.value, req.metadata)


class PutReq(DynamoRequestMessage):
    def __init__(self, from_node, to_node, key, value, metadata, msg_id=None, handoff=None):
        super(PutReq, self).__init__(from_node, to_node, key, msg_id)
        self.value = value
        self.metadata = metadata
        self.handoff = handoff

    def __str__(self):
        if self.handoff is None:
            return "PutReq(%s=%s)" % (self.key, self.value)
        else:
            return "PutReq(%s=%s, handoff=(%s))" % (self.key, self.value, ",".join([str(x) for x in self.handoff]))


class PutRsp(DynamoResponseMessage):
    def __init__(self, req):
        super(PutRsp, self).__init__(req, req.value, req.metadata)


class ClientGet(DynamoRequestMessage):
    pass


class ClientGetRsp(DynamoResponseMessage):
    pass


class GetReq(DynamoRequestMessage):
    pass


class GetRsp(DynamoResponseMessage):
    pass


class PingReq(Message):
    def __str__(self):
        return "PingReq"


class PingRsp(ResponseMessage):
    def __str__(self):
        return "PingRsp"
