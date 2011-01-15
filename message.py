
class Message:
    """Base type for messages between Nodes"""
    def __init__(self, from_node, to_node, msg_id=None):
        self.from_node = from_node
        self.to_node = to_node
        self.msg_id = msg_id
    def __str__(self):
        return "%s->%s:" % (self.from_node, self.to_node)

class ResponseMessage(Message):
    """Base type for messages that are replies to existing messages"""
    def __init__(self, req):
        Message.__init__(self, req.to_node, req.from_node, msg_id=req.msg_id)
        self.response_to = req

# Internal messages used to indicate events in the environment
class NodeAction(Message):
    """Internal message indicating an action at a node"""
    def __init__(self, node): 
        Message.__init__(self, node, node)
    def __str__(self): 
        return str(self.node)

class TimerMessage(Message):
    """Internal message indicating a timer event at a node"""
    def __init__(self, node, reason, callback=None): 
        Message.__init__(self, node, node)
        self.reason = reason
        self.callback = callback
    def __str__(self):
        if self.reason is None:
            return "Timer"
        else:
            return "Timer(%s)" % self.reason
