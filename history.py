"""Code to track history of all activity"""
from bisect import bisect
import logging

_logger = logging.getLogger('dynamo')


class AsciiGlyphs(object):
    """Set of glyphs to use in drawing history, in base ASCII"""
    BLANK = ' '
    FAILED_NODE = 'x'
    OK_NODE = '.'
    VERTICAL_LINE = '|'
    HORIZONTAL_LINE = '-'
    MSG_START = 'o'
    MSG_FORWARD = '+'
    MSG_NW = '+'
    MSG_NE = '+'
    MSG_SW = '+'
    MSG_SE = '+'
    MSG_END_LEFT = '<'
    MSG_END_RIGHT = '>'
    MSG_FAIL = 'X'
    COMMENT = '*'


class UnicodeGlyphs(AsciiGlyphs):
    VERTICAL_LINE = u'\u2502'
    HORIZONTAL_LINE = u'\u2500'
    MSG_NW = u'\u256f'
    MSG_NE = u'\u2570'
    MSG_SW = u'\u256e'
    MSG_SE = u'\u256d'


# Which set of line-drawing glyphs to use.
GLYPHS = AsciiGlyphs


class History(object):
    """
    History of everything that happened in the framework, as a list of (action, object) pairs.

    Possible values for the action are:
      'send'     - message sent
      'forward'  - message forwarded
      'drop'     - message dropped due to node down
      'cut'      - message dropped due to comms down
      'deliver'  - message arrived
      'start'    - timer started
      'cancel'   - timer cancelled
      'pop'      - timer popped
      'fail'     - node failed
      'recover'  - node recovered
      'add'      - node added to configuration
      'remove'   - node removed from configuration
      'announce' - overall message to be included in output
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
    def nodelist(cls, force_include=None, key=lambda x: x.name):
        """Return a list of all nodes involved in the history"""
        nodeset = set()
        for (action, msg) in cls.history:
            if action == "send" or action == "forward":
                # Every message must be sent, so just look at send actions
                nodeset.add(msg.from_node)
                nodeset.add(msg.to_node)
        if force_include is not None:
            for node in force_include:
                nodeset.add(node)
        nodelist = list(nodeset)
        nodelist.sort(key=key)
        return nodelist

    @classmethod
    def ladder(cls, spacing=20, verbose_timers=False, start_line=0, force_include=None, key=lambda x: x.name):
        """Generate the ladder diagram for a message history"""
        # First spin through all of the message history to find the set of Nodes involved
        nodelist = cls.nodelist(force_include, key=key)
        num_nodes = len(nodelist)
        included_nodes = set()

        # Line make-up is like this:
        #     0 1 2 3 4 5 6 7 8
        #     A . . . B . . . C
        # If m=number of cols between nodes and N=number of nodes
        # then overall line length = ((N-1)*(m+1)) + 1
        # (example above has N=3 m=3 => len=9=2*4+1)
        linelen = ((num_nodes - 1) * (spacing + 1)) + 1

        # Figure out the column for each node
        column = {}
        for ii in range(num_nodes):
            column[nodelist[ii]] = ii * (spacing + 1)

        vertlines = {}  # Current vertical lines, msg=>column
        failed_nodes = set()
        lines = [_header_line(nodelist, spacing)]
        lineno = 0

        # Step through all of the actions
        for ii in range(len(cls.history)):
            action, msg = cls.history[ii]
            lineno = lineno + 1
            # First, build up a line with the current set of vertical lines and leaders
            this_line = [GLYPHS.BLANK for jj in xrange(linelen)]
            for node, nodecol in column.items():
                if node in included_nodes:
                    if node in failed_nodes:
                        this_line[nodecol] = GLYPHS.FAILED_NODE
                    else:
                        this_line[nodecol] = GLYPHS.OK_NODE
                else:
                    this_line[nodecol] = GLYPHS.BLANK
            for vertcol in vertlines.values():
                this_line[vertcol] = GLYPHS.VERTICAL_LINE

            # Now look at this particular action
            if action == "send" or action == "forward":
                if action == "forward":
                    from_node = msg.intermediate_node
                    start_marker = GLYPHS.MSG_FORWARD
                else:
                    from_node = msg.from_node
                    start_marker = GLYPHS.MSG_START
                # Pick a suitable spot for the vertical line for this message
                vertcol = _pick_column(vertlines, column,
                                       column[from_node], column[msg.to_node])
                vertlines[msg] = vertcol
                left2right = (column[from_node] < vertcol)
                if left2right:
                    end_marker = GLYPHS.MSG_SW
                else:
                    end_marker = GLYPHS.MSG_SE

                # Draw the horizontal line
                _draw_horiz(this_line,
                            column[from_node], start_marker,
                            vertcol, end_marker)
                # Add the message text
                msgtext = str(msg)
                if left2right:  # o----+ Text
                    _write_text(this_line, vertcol + 1, GLYPHS.BLANK + msgtext)
                elif len(msgtext) > vertcol:  # +---o Text
                    _write_text(this_line, column[from_node] + 1, GLYPHS.BLANK + msgtext)
                else:  # Text +---o
                    _write_text(this_line, vertcol - len(msgtext) - 1, msgtext + GLYPHS.BLANK)

            elif action == "deliver" or action == "drop":
                # Find the existing vertline that corresponds to this message, and
                # remove it
                vertcol = vertlines[msg]
                del vertlines[msg]

                left2right = (vertcol < column[msg.to_node])
                if left2right:
                    start_marker = GLYPHS.MSG_NE
                else:
                    start_marker = GLYPHS.MSG_NW
                if action == "drop":
                    end_marker = GLYPHS.MSG_FAIL
                elif left2right:
                    end_marker = GLYPHS.MSG_END_RIGHT
                else:
                    end_marker = GLYPHS.MSG_END_LEFT

                # Draw the horizontal line
                _draw_horiz(this_line,
                            vertcol, start_marker,
                            column[msg.to_node], end_marker)
            elif action == "cut":
                # Find the existing vertline that corresponds to this message, and
                # remove it
                vertcol = vertlines[msg]
                del vertlines[msg]
                this_line[vertcol] = GLYPHS.MSG_FAIL

            elif action == "start":
                if verbose_timers:
                    _write_center(this_line, column[msg.from_node], "%s:Start" % msg)
                else:
                    continue
            elif action == "pop":
                # In non-verbose mode, only display a timer pop if it looks like it
                # produced some activity.
                if ((ii + 1 < len(cls.history) and cls.history[ii + 1][0] == "send") or
                    verbose_timers):
                    _write_center(this_line, column[msg.from_node], "%s:Pop" % msg)
                else:
                    continue
            elif action == "cancel":
                if verbose_timers:
                    _write_center(this_line, column[msg.from_node], "%s:Cancel" % msg)
                else:
                    continue
            elif action == "fail":
                if msg.from_node in column:
                    _write_center(this_line, column[msg.from_node], "FAIL")
                    failed_nodes.add(msg.from_node)
                else:
                    continue
            elif action == "recover":
                if msg.from_node in column:
                    _write_center(this_line, column[msg.from_node], "RECOVER")
                    failed_nodes.remove(msg.from_node)
                else:
                    continue
            elif action == "remove":
                included_nodes.remove(msg.from_node)
                continue  # don't emit a line
            elif action == "add":
                included_nodes.add(msg.from_node)
                continue  # don't emit a line
            elif action == "announce":
                indent = GLYPHS.COMMENT * ((linelen - len(msg) - 4) // 2)
                if lineno > start_line:
                    lines.append(' %s %s %s ' % (indent, msg, indent))
                continue  # line already emitted

            # Put the array of characters together into a line, and add that to the list
            if lineno > start_line:
                lines.append(''.join(this_line))

        # Build a final epilogue set of lines.  First, a header line
        lines.append(_header_line(nodelist, spacing))
        # Now accumulate the contents information from the nodes
        contents = {}  # node -> list of contents
        longest_conts = 0
        for node in column.keys():
            node_conts = node.get_contents()
            contents[node] = node_conts
            if len(node_conts) > longest_conts:
                longest_conts = len(node_conts)
        # Now typeset it
        for ii in range(longest_conts):
            this_line = [GLYPHS.BLANK for jj in xrange(linelen)]
            for node, nodecol in column.items():
                if ii < len(contents[node]):
                    _write_center(this_line, nodecol, str(contents[node][ii]))
            lines.append(''.join(this_line))
        return '\n'.join(lines)


def _header_line(nodelist, m):
    """Generate header line string with m columns between nodes"""
    header_line = ''
    spacer = GLYPHS.BLANK * m
    for node in nodelist:
        if header_line != '':
            header_line = header_line + spacer
        header_line = header_line + node.name
    return header_line


def _pick_column(vertlines, columns, from_col, to_col):
    """Pick a column in (from_col, to_col) that is not one of the entries in vertlines or columns"""
    # Collate the disallowed columns
    not_allowed = set()
    for col in vertlines.values():
        not_allowed.add(col)
    for col in columns.values():
        not_allowed.add(col)

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
        if insert_point > 0:  # look at not_allowed[insert_point - 1]
            dist_left = (col - not_allowed_list[insert_point - 1])
        else:
            dist_left = 99999
        if insert_point < len(not_allowed_list):  # look at not_allowed[insert_point]
            dist_right = (not_allowed_list[insert_point] - col)
        else:
            dist_right = 99999
        distance = min(dist_left, dist_right)
        if distance > best_distance:
            best_distance = distance
            best_col = col
    if best_col == -1:
        raise ValueError("No free column found!")
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
    for jj in xrange(left, xright):
        line[jj] = GLYPHS.HORIZONTAL_LINE


def _write_text(line, col, text):
    # text may need to extend the array
    extend_by = (col + len(text)) - len(line)
    for ii in xrange(extend_by):
        line.append(GLYPHS.BLANK)
    for c in text:
        line[col] = c
        col = col + 1


def _write_center(line, col, text):
    if (col > (len(text) / 2)):
        col = col - (len(text) / 2)
    _write_text(line, col, text)
