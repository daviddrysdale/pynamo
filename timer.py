"""Timer functionality"""
import logging

from message import Timer
from history import History

_logger = logging.getLogger('dynamo')

DEFAULT_PRIORITY = 10


def _priority(tmsg):
    priority = DEFAULT_PRIORITY
    node = tmsg.from_node
    if 'timer_priority' in node.__class__.__dict__:
        priority = int(node.__class__.__dict__['timer_priority'])
    return priority


class TimerManager(object):
    # List of pending timers, maintained in order of priority then insertion
    pending = []  # list of (priority, tmsg) tuples

    @classmethod
    def pending_count(cls):
        return len(cls.pending)

    @classmethod
    def reset(cls):
        cls.pending = []

    @classmethod
    def start_timer(cls, node, reason=None, callback=None, priority=None):
        """Start a timer for the given node, with an option reason code"""
        if node.failed:
            return None
        tmsg = Timer(node, reason, callback=callback)
        History.add("start", tmsg)
        if priority is None:  # default to priority of the node
            priority = _priority(tmsg)
        _logger.debug("Start timer %s prio %d for node %s reason %s", id(tmsg), priority, node, reason)
        # Figure out where in the list to insert
        for ii in range(len(cls.pending)):
            if priority > cls.pending[ii][0]:
                cls.pending.insert(ii, (priority, tmsg))
                return tmsg
        cls.pending.append((priority, tmsg))
        return tmsg

    @classmethod
    def cancel_timer(cls, tmsg):
        """Cancel the given timer"""
        for (this_prio, this_tmsg) in cls.pending:
            if this_tmsg == tmsg:
                _logger.debug("Cancel timer %s for node %s reason %s", id(tmsg), tmsg.from_node, tmsg.reason)
                cls.pending.remove((this_prio, this_tmsg))
                History.add("cancel", tmsg)
                return

    @classmethod
    def pop_timer(cls):
        """Pop the first pending timer"""
        while True:
            (_, tmsg) = cls.pending.pop(0)
            if tmsg.from_node.failed:
                continue
            _logger.debug("Pop timer %s for node %s reason %s", id(tmsg), tmsg.from_node, tmsg.reason)
            History.add("pop", tmsg)
            if tmsg.callback is None:
                # Default to calling Node.timer_pop()
                tmsg.from_node.timer_pop(tmsg.reason)
            else:
                tmsg.callback(tmsg.reason)
            return
