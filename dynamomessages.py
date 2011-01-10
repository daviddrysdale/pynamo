from message import Message

class DynamoMessage(Message):
    """Base class for Dynamo messages; all include the key for the data object in question"""
    def __init__(self, from_node, to_node, key):
        Message.__init__(self, from_node, to_node)
        self.key = key
    def __str__(self):
        return "%s %s=?" % (Message.__str__(self), self.key)

class DynamoDataMessage(Message):
    """Base class for Dynamo messages that include key and value (plus metadata)"""
    def __init__(self, from_node, to_node, key, value, metadata):
        Message.__init__(self, from_node, to_node)
        self.key = key
        self.value = value
        self.metadata = metadata
    def __str__(self):
        return "%s %s=%s" % (Message.__str__(self), self.key, self.value)

class DynamoResponse(DynamoDataMessage):
    """Base class for Dynamo response messages that are replies to a DynamoMessage"""
    def __init__(self, req, value, metadata):
        DynamoDataMessage.__init__(self, req.to_node, req.from_node, req.key, value, metadata)

class DynamoDataResponse(DynamoDataMessage):
    """Base class for Dynamo response messages that are replies to a DynamoDataMessage"""
    def __init__(self, req):
        DynamoDataMessage.__init__(self, req.to_node, req.from_node, req.key, req.value, req.metadata)

class ClientPut(DynamoDataMessage):
    def __str__(self):
        return "ClientPut(%s=%s)" % (self.key, self.value)

class ClientPutRsp(DynamoDataResponse):
    def __str__(self):
        return "ClientPutRsp(%s=%s)" % (self.key, self.value)

class PutReq(DynamoDataMessage):
    def __str__(self):
        return "PutReq(%s=%s)" % (self.key, self.value)

class PutRsp(DynamoDataResponse):
    def __str__(self):
        return "PutRsp(%s=%s)" % (self.key, self.value)
        

class ClientGet(DynamoMessage):
    def __str__(self):
        return "ClientGet(%s=?)" % self.key

class ClientGetRsp(DynamoResponse):
    def __str__(self):
        return "ClientGetRsp(%s=%s)" % (self.key, self.value)

class GetReq(DynamoMessage):
    def __str__(self):
        return "GetReq(%s=?)" % self.key

class GetRsp(DynamoResponse):
    def __str__(self):
        return "GetRsp(%s=%s)" % (self.key, self.value)
