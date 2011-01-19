"""Timer functionality"""

import logging

from message import TimerMessage
from history import History

_logger = logging.getLogger('dynamo')

DEFAULT_PRIORITY = 10
def _priority(tmsg):
    priority = DEFAULT_PRIORITY
    node = tmsg.from_node
    if 'timer_priority' in node.__class__.__dict__:
        priority = int(node.__class__.__dict__['timer_priority'])
    return priority

class Timer:
    # List of pending timers, maintained in order of:
    #   node.timer_priority
    #   insertion
    pending = [] 

    @classmethod
    def pending_count(cls):
        return len(cls.pending)
    
    @classmethod
    def reset(cls):
        cls.pending = []

    @classmethod
    def start_timer(cls, node, reason=None, callback=None):
        """Start a timer for the given node, with an option reason code"""
        tmsg = TimerMessage(node, reason, callback=callback)
        _logger.debug("Start timer %s for node %s reason %s", id(tmsg), node, reason)
        History.add("start", tmsg)
        priority = _priority(tmsg)
        # Figure out where in the list to insert
        for ii in range(len(cls.pending)):
            if priority > _priority(cls.pending[ii]):
                cls.pending.insert(ii, tmsg)
                return tmsg
        cls.pending.append(tmsg)
        return tmsg
    
    @classmethod
    def cancel_timer(cls, tmsg):
        """Cancel the given timer"""
        if tmsg in cls.pending:
            _logger.debug("Cancel timer %s for node %s", id(tmsg), tmsg.from_node)
            cls.pending.remove(tmsg)
            History.add("cancel", tmsg)
            
    @classmethod
    def pop_timer(cls):
        """Pop the first pending timer"""
        tmsg = cls.pending.pop(0)
        _logger.debug("Pop timer %s for node %s", id(tmsg), tmsg.from_node)
        History.add("pop", tmsg)
        if tmsg.callback is None:
            # Default to calling Node.timer_pop()
            tmsg.from_node.timer_pop(tmsg.reason)
        else:
            tmsg.callback(tmsg.reason)

