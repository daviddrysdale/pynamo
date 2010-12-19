"""Code to track history of all activity"""

from bisect import bisect
import logging

_logger = logging.getLogger('dynamo')

class History:
    """
    History of everything that happened in the framework, as a list of (action, object) pairs.
    
    Possible values for the action are:
      'send'    - message sent 
      'drop'    - message dropped due to node down
      'cut'     - message dropped due to comms down
      'deliver' - message arrived
      'start'   - timer started
      'cancel'  - timer cancelled
      'pop'     - timer popped
      'fail'    - node failed
      'recover' - node recovered
      'add'     - node added to configuration
      'remove'  - node removed from configuration
    """
    history = []

    @classmethod
    def reset(cls):
        """Reset the history"""
        cls.history = []

    @classmethod
    def add(cls, action, obj):
        """Add an event to the historical record"""
        cls.history.append((action, obj))

    @classmethod
    def nodelist(cls):
        """Return a list of all nodes involved in the history"""
        nodeset = set()
        for (action, msg) in cls.history:
            if action == "send":
                # Every message must be sent, so just look at send actions
                nodeset.add(msg.from_node)
                nodeset.add(msg.to_node)
        nodelist = list(nodeset)
        nodelist.sort(key=lambda x:x.name)
        return nodelist

    @classmethod
    def ladder(cls, spacing=20, verbose_timers=False):
        """Generate the ladder diagram for a message history"""
        # First spin through all of the message history to find the set of Nodes involved
        nodelist = cls.nodelist()
        num_nodes = len(nodelist)
        included_nodes = set()
    
        # Line make-up is like this:
        #     0 1 2 3 4 5 6 7 8
        #     A . . . B . . . C
        # If m=number of cols between nodes and N=number of nodes
        # then overall line lengh = ((N-1)*(m+1)) + 1
        # (example above has N=3 m=3 => len=9=2*4+1)
        linelen = ((num_nodes - 1)*(spacing+1)) + 1
    
        # Figure out the column for each node
        column = {}
        for ii in range(num_nodes):
            column[nodelist[ii]] = ii * (spacing+1)
    
        vertlines = {} # Current vertical lines, msg=>column
        failed_nodes = set()
        lines = [_header_line(nodelist, spacing)]
        first_line = True
    
        # Step through all of the actions
        for action, msg in cls.history:
            # First, build up a line with the current set of vertical lines and leaders
            this_line = [' ' for jj in xrange(linelen)]
            for node, nodecol in column.items(): 
                if node in included_nodes:
                    if node in failed_nodes:
                        this_line[nodecol] = 'x'
                    else:
                        this_line[nodecol] = '.'
                else:
                    this_line[nodecol] = ' '
            for vertcol in vertlines.values(): this_line[vertcol] = '|'
                
            # Now look at this particular action
            if action == "send":
                # Pick a suitable spot for the vertical line for this message
                vertcol = _pick_column(vertlines, column, 
                                       column[msg.from_node], column[msg.to_node])
                vertlines[msg] = vertcol
                left2right = (column[msg.from_node] < vertcol)
                
                # Draw the horizontal line
                _draw_horiz(this_line, 
                            column[msg.from_node], 'o',
                            vertcol, '+')
                # Add the message text
                msgtext = str(msg)
                if left2right: #    o----+ Text
                    _write_text(this_line, vertcol+1, ' ' + msgtext)
                elif len(msgtext) > vertcol: #  +---o Text
                    _write_text(this_line, column[msg.from_node]+1, ' ' + msgtext)
                else: # Text +---o
                    _write_text(this_line, vertcol - len(msgtext) - 1, msgtext + ' ')
                
            elif action == "deliver" or action == "drop":
                left2right = (vertcol < column[msg.to_node])
                if action == "drop":
                    end_marker = 'X'
                elif left2right:
                    end_marker = '>'
                else:
                    end_marker = '<'
                # Find the existing vertline that corresponds to this message, and 
                # remove it
                vertcol = vertlines[msg]
                del vertlines[msg] 
    
                # Draw the horizontal line
                _draw_horiz(this_line,
                            vertcol, '+',
                            column[msg.to_node], end_marker)
            elif action == "cut":
                # Find the existing vertline that corresponds to this message, and 
                # remove it
                vertcol = vertlines[msg]
                del vertlines[msg] 
                this_line[vertcol] = 'X'
                
            elif action == "start":
                if verbose_timers:
                    _write_center(this_line, column[msg.from_node], "%s:Start" % msg)
                pass
            elif action == "pop":
                _write_center(this_line, column[msg.from_node], "%s:Pop" % msg)
            elif action == "cancel":
                if verbose_timers:
                    _write_center(this_line, column[msg.from_node], "%s:Cancel" % msg)
            elif action == "fail":
                _write_center(this_line, column[msg.from_node], "FAIL")
                failed_nodes.add(msg.from_node)
            elif action == "recover":
                _write_center(this_line, column[msg.from_node], "RECOVER")
                failed_nodes.remove(msg.from_node)
            elif action == "remove":
                included_nodes.remove(msg.from_node)
                continue # don't emit a line
            elif action == "add":
                included_nodes.add(msg.from_node)
                continue # don't emit a line
    
            # Put the array of characters together into a line, and add that to the list
            lines.append(''.join(this_line))
    
    
        # Build a final epilogue pair of lines
        lines.append(_header_line(nodelist, spacing))
        this_line = [' ' for jj in xrange(linelen)]
        for node, nodecol in column.items(): 
            if 'value' in node.__dict__ and node.value is not None:
                _write_center(this_line, nodecol, str(node.value))
        lines.append(''.join(this_line))
        return '\n'.join(lines)
            
def _header_line(nodelist, m):
    """Generate header line string with m columns between nodes"""
    header_line = ''
    spacer = ' ' * m
    for node in nodelist:
        if header_line != '': header_line = header_line + spacer
        header_line = header_line + node.name
    return header_line


def _pick_column(vertlines, columns, from_col, to_col):
    """Pick a column in (from_col, to_col) that is not one of the entries in vertlines or columns"""
    # Collate the disallowed columns
    not_allowed = set()
    for col in vertlines.values(): not_allowed.add(col)
    for col in columns.values(): not_allowed.add(col)

    # Pick the first free column close to the from_col
    if from_col == to_col:
        if from_col == 0:
            candidate = from_col + 1
            delta = 1
        else:
            candidate = from_col - 1
            delta = -1
    elif from_col < to_col:
        candidate = from_col + 1
        delta = 1
    else:
        candidate = from_col - 1
        delta = -1
    while candidate != to_col:
        if not candidate in not_allowed:
            return candidate
        candidate = candidate + delta
    
    # ALTERNATIVE IMPLEMENTATION, NOT USED:
    # Examine every possible candidate position and pick the one that is furthest
    # from any disallowed columns
    not_allowed_list = list(not_allowed)
    not_allowed_list.sort()
    if from_col < to_col:
        left = from_col + 1
        xright = to_col
    else:
        left = to_col + 1
        xright = from_col
    best_col = -1
    best_distance = 0
    for col in xrange(left, xright):
        # Calculate the distance to the nearest excluded column
        insert_point = bisect(not_allowed_list, col)
        if insert_point > 0: # look at not_allowed[insert_point-1]
            dist_left = (col - not_allowed_list[insert_point-1])
        else:
            dist_left = 99999
        if insert_point < len(not_allowed_list): # look at not_allowed[insert_point]
            dist_right = (not_allowed_list[insert_point] - col)
        else:
            dist_right = 99999
        distance = min(dist_left, dist_right)
        if distance > best_distance:
            best_distance = distance
            best_col = col
    if best_col == -1: raise ValueError("No free column found!")
    return best_col

def _draw_horiz(line, from_col, from_char, to_col, to_char):
    line[from_col] = from_char
    line[to_col] = to_char
    if from_col < to_col:
        left = from_col + 1
        xright = to_col
    else:
        left = to_col + 1
        xright = from_col 
    for jj in xrange(left, xright): line[jj] = '-'

def _write_text(line, col, text):
    # text may need to extend the array
    extend_by = (col + len(text)) - len(line)
    for ii in xrange(extend_by): line.append(' ')
    for c in text:
        line[col] = c
        col = col + 1

def _write_center(line, col, text):
    if (col > (len(text)/2)):
        col = col - (len(text)/2)
    _write_text(line, col, text)


