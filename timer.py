"""Timer functionality"""

from collections import deque
import logging

from message import TimerMessage
from history import History

_logger = logging.getLogger('dynamo')

class Timer:
    pending = deque()

    @classmethod
    def pending_count(cls):
        return len(cls.pending)
    
    @classmethod
    def reset(cls):
        cls.pending = deque()

    @classmethod
    def start_timer(cls, node, reason=None, callback=None):
        """Start a timer for the given node, with an option reason code"""
        tmsg = TimerMessage(node, reason, callback=callback)
        cls.pending.append(tmsg)
        History.add("start", tmsg)
        return tmsg
    
    @classmethod
    def cancel_timer(cls, tmsg):
        """Cancel the given timer"""
        if tmsg in cls.pending:
            cls.pending.remove(tmsg)
            History.add("cancel", tmsg)
            
    @classmethod
    def pop_timer(cls):
        """Pop the first pending timer"""
        tmsg = cls.pending.popleft()
        if tmsg.callback is None:
            # Default to calling Node.timer_pop()
            tmsg.from_node.timer_pop(tmsg.reason)
        else:
            tmsg.callback(tmsg.reason)
        History.add("pop", tmsg)

