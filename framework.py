"""Framework code for simulating networks"""
import copy
import logging
from collections import deque

from node import Node
from history import History
from timer import TimerManager
from message import ResponseMessage
import logconfig

logconfig.init_logging()
_logger = logging.getLogger('dynamo')


class Framework(object):
    cuts = []  # List of incommunicado sets of nodes
    queue = deque([])  # queue of pending messages
    pending_timers = {}  # request_message => timer

    @classmethod
    def reset(cls):
        cls.cuts = []
        cls.queue = deque([])
        cls.pending_timers = {}

    @classmethod
    def cut_wires(cls, from_nodes, to_nodes):
        History.add("announce", "Cut %s -> %s" % ([str(x) for x in from_nodes], [str(x) for x in to_nodes]))
        cls.cuts.append((from_nodes, to_nodes))

    @classmethod
    def reachable(cls, from_node, to_node):
        for (from_nodes, to_nodes) in cls.cuts:
            if from_node in from_nodes and to_node in to_nodes:
                return False
        return True

    @classmethod
    def send_message(cls, msg, expect_reply=True):
        """Send a message"""
        _logger.info("Enqueue %s->%s: %s", msg.from_node, msg.to_node, msg)
        cls.queue.append(msg)
        History.add("send", msg)
        # Automatically run timers for request messages if the sender can cope
        # with retry timer pops
        if (expect_reply and
            not isinstance(msg, ResponseMessage) and
            'rsp_timer_pop' in msg.from_node.__class__.__dict__ and
            callable(msg.from_node.__class__.__dict__['rsp_timer_pop'])):
            cls.pending_timers[msg] = TimerManager.start_timer(msg.from_node, reason=msg, callback=Framework.rsp_timer_pop)

    @classmethod
    def remove_req_timer(cls, reqmsg):
        if reqmsg in cls.pending_timers:
            # Cancel request timer as we've seen a response
            TimerManager.cancel_timer(cls.pending_timers[reqmsg])
            del cls.pending_timers[reqmsg]

    @classmethod
    def cancel_timers_to(cls, destnode):
        """Cancel all pending-request timers destined for the given node.
        Returns a list of the request messages whose timers have been cancelled."""
        failed_requests = []
        for reqmsg in cls.pending_timers.keys():
            if reqmsg.to_node == destnode:
                TimerManager.cancel_timer(cls.pending_timers[reqmsg])
                del cls.pending_timers[reqmsg]
                failed_requests.append(reqmsg)
        return failed_requests

    @classmethod
    def rsp_timer_pop(cls, reqmsg):
        # Remove the record of the pending timer
        del cls.pending_timers[reqmsg]
        # Call through to the node's rsp_timer_pop() method
        _logger.debug("Call on to rsp_timer_pop() for node %s" % reqmsg.from_node)
        reqmsg.from_node.rsp_timer_pop(reqmsg)

    @classmethod
    def forward_message(cls, msg, new_to_node):
        """Forward a message"""
        _logger.info("Enqueue(fwd) %s->%s: %s", msg.to_node, new_to_node, msg)
        fwd_msg = copy.copy(msg)
        fwd_msg.intermediate_node = fwd_msg.to_node
        fwd_msg.original_msg = msg
        fwd_msg.to_node = new_to_node
        cls.queue.append(fwd_msg)
        History.add("forward", fwd_msg)

    @classmethod
    def schedule(cls, msgs_to_process=None, timers_to_process=None):
        """Schedule given number of pending messages"""
        if msgs_to_process is None:
            msgs_to_process = 32768
        if timers_to_process is None:
            timers_to_process = 32768

        while cls._work_to_do():
            _logger.info("Start of schedule: %d (limit %d) pending messages, %d (limit %d) pending timers",
                         len(cls.queue), msgs_to_process, TimerManager.pending_count(), timers_to_process)
            # Process all the queued up messages (which may enqueue more along the way)
            while cls.queue:
                msg = cls.queue.popleft()
                if msg.to_node.failed:
                    _logger.info("Drop %s->%s: %s as destination down", msg.from_node, msg.to_node, msg)
                    History.add("drop", msg)
                elif not Framework.reachable(msg.from_node, msg.to_node):
                    _logger.info("Drop %s->%s: %s as route down", msg.from_node, msg.to_node, msg)
                    History.add("cut", msg)
                else:
                    _logger.info("Dequeue %s->%s: %s", msg.from_node, msg.to_node, msg)
                    if isinstance(msg, ResponseMessage):
                        # figure out the original request this is a response to
                        try:
                            reqmsg = msg.response_to.original_msg
                        except Exception:
                            reqmsg = msg.response_to
                        # cancel any timer associated with the original request
                        cls.remove_req_timer(reqmsg)
                    History.add("deliver", msg)
                    msg.to_node.rcvmsg(msg)
                msgs_to_process = msgs_to_process - 1
                if msgs_to_process == 0:
                    return

            # No pending messages; potentially pop a (single) timer
            if TimerManager.pending_count() > 0 and timers_to_process > 0:
                # Pop the first pending timer; this may enqueue work
                TimerManager.pop_timer()
                timers_to_process = timers_to_process - 1
            if timers_to_process == 0:
                return

    @classmethod
    def _work_to_do(cls):
        """Indicate whether there is work to do"""
        if cls.queue:
            return True
        if TimerManager.pending_count() > 0:
            return True
        return False


def reset():
    """Reset all message and other history"""
    Framework.reset()
    TimerManager.reset()
    History.reset()


def reset_all():
    """Reset all message and other history, and remove all nodes"""
    reset()
    Node.reset()
