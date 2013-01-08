import re
import cStringIO as StringIO
import zlib

from mercury.change import *
from mercury.exceptions import *
from mercury.base85 import *

_ESCAPES = { r'\a': '\x07', r'\b': '\x08', r'\t': '\x09',
             r'\n': '\x0a', r'\v': '\x0b', r'\f': '\x0c',
             r'\r': '\x0d', r'\"': '"', r'\'': '\'', r'\\': '\\' }
_ESCAPE_RE = re.compile(r'\\(?:[abtnvfr]|u[A-Fa-f0-9]{1,4}|U[A-Fa-f0-9]{1,8}'
                        r'|(?:x[A-Fa-f0-9]{1,2}|[0-7]{1,3})|.)')
_UNI_RE = re.compile(r'(\\(?:u[A-Fa-f0-9]{4}|U[A-Fa-f0-9]{8}))')
_INTERESTING = re.compile(r'(?:\\.|")')
_SPACE = re.compile(r'\s+')
_GIT_RE = re.compile(r'diff\s+--git\s+')
_GIT_ARG_RE = re.compile(r'(?:[^/]+)/(.*)\s+(?:[^/]+)/\1')

def unescape_c(s, encoding='utf-8'):
    """Process C-style escapes, in a particular encoding.  The result is
    always a Unicode string; octal and hex escapes are interpreted in the
    specified encoding, NOT in Unicode."""
    def process_escape(match):
        esc = match.group(0)
        ch = _ESCAPES.get(esc, None)
        if ch:
            return ch
        elif esc.startswith(r'\x'):
            return chr(int(e[2:], 16))
        elif esc[1] in '01234567':
            return chr(int(e[1:], 8))
        else:
            return esc[1:]

    strings = []
    for c in _UNI_RE.split(s):
        if c.startswith(r'\u') or c.startswith(r'\U'):
            strings.append(unichr(int(c[2:], 16)))
        else:
            r = _ESCAPE_RE.sub(process_escape, c)
            if not isinstance(r, unicode):
                r = r.decode(encoding)
            strings.append(r)

    return u''.join(strings)

def scan_quoted(line, ndx, encoding):
    dlen = len(line)
    ndx += 1
    chunks = []
    done = False
    while ndx < dlen:
        m = _INTERESTING.search(line, ndx)
            
        if m:
            next = m.start(0)
        else:
            next = dlen
            
        chunk = line[ndx:next]
        if chunk:
            chunks.append(chunk)

        if not m:
            break

        ndx = m.end(0)
        match = m.group(0)
        if match.startswith('\\'):
            chunks.append(match)
        elif match == '"':
            done = True
            break

    return unescape_c(''.join(chunks), encoding), ndx

def extract_filename(line, encoding='utf-8'):
    """Attempt to extract the filename from a diff --git line."""
    m = _GIT_RE.match(line)
    if not m:
        return None
    
    ndx = m.end(0)
    dlen = len(line)

    if ndx >= dlen:
        return None
    
    if line[ndx] == '"':
        first, ndx = scan_quoted(line, ndx, encoding)

        m = _SPACE.match(line, ndx)
        if not m:
            return None
        
        ndx = m.end(0)

        if ndx < dlen and line[ndx] == '"':
            second, ndx = scan_quoted(line, ndx, encoding)
        else:
            second = line[ndx:]
    else:
        # The first argument is unquoted; if we find a double-quote, it
        # must be the start of the second one
        ndx2 = line.find('"', ndx)
        if ndx2 > -1:
            second, ndx3 = scan_quoted(line, ndx2, encoding)
            first = line[ndx:ndx2].rstrip(' \t\r\n')
        else:
            m = _GIT_ARG_RE.match(line, ndx)
            if not m:
                return None
            
            return m.group(1)

    try:
        first = first.split('/', 1)[1]
        second = second.split('/', 1)[1]
    except IndexError:
        return None

    if first != second:
        return None

    return first

_DATE_RE = re.compile(r'(?:\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}(?:\.\d+)?'
                      r'|(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun)\s+'
                      r'(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+'
                      r'\d{1,2}\s+\d{2}:\d{2}:\d{2}(?:\.\d+)?\s+\d{4})'
                      r'(?:\s+[+-]\d{2}:?\d{2})?\s*$')

def _find_name(s, defname, encoding, with_date=False):
    """Given a possible filename, attempt to decode it."""
    s = s.lstrip()
    
    if s.startswith('"'):
        name, ndx = scan_quoted(s, 0, encoding)
    else:
        name = None
        if with_date:
            m = _DATE_RE.search(s)
            if m:
                name = s[:m.start(0)]

        if not name:
            ndx = s.find('\t')
            if ndx < 0:
                ndx = s.find(' ')
            if ndx > 0:
                name = s[:ndx]
            else:
                name = s
        
    name = name.strip(' \t').split('/', 1)

    if len(name) < 2 or name[1] is None:
        return defname

    name = name[1]
    
    if defname is not None and name.startswith(defname):
        return defname

    return name

def _extract_name(s, encoding):
    name = s.strip()
    if name.startswith('"'):
        name, ndx = scan_quoted(name, 0, encoding)
    return name
    
def _handle_rename_from(line, match, change, defname, encoding):
    name = _extract_name(match.group(1), encoding)
        
    if not isinstance(change, Rename):
        change = Rename(source=name)
    else:
        change.source = name
    return change

def _handle_rename_to(line, match, change, defname, encoding):
    name = _extract_name(match.group(1), encoding)
        
    if not isinstance(change, Rename):
        change = Rename(dest=name)
    else:
        change.dest = name
    return change

def _handle_copy_from(line, match, change, defname, encoding):
    name = _extract_name(match.group(1), encoding)
        
    if not isinstance(change, Copy):
        change = Copy(source=name)
    else:
        change.source = name
    return change

def _handle_copy_to(line, match, change, defname, encoding):
    name = _extract_name(match.group(1), encoding)
        
    if not isinstance(change, Copy):
        change = Copy(dest=name)
    else:
        change.dest = name
    return change

def _handle_delete(line, match, change, defname, encoding):
    return Delete(defname, int(match.group(1), 8))

def _handle_new(line, match, change, defname, encoding):
    return Add(defname, int(match.group(1), 8))

def _handle_old_mode(line, match, change, defname, encoding):
    change.old_mode = int(match.group(1), 8)
    return change

def _handle_new_mode(line, match, change, defname, encoding):
    change.new_mode = int(match.group(1), 8)
    return change

def _handle_similarity(line, match, change, defname, encoding):
    change.similarity = int(match.group(1))
    return change

def _handle_dissimilarity(line, match, change, defname, encoding):
    change.dissimilarity = int(match.group(1))
    return change

def _handle_unified(line, next, change, defname, encoding):
    change.source = _find_name(line[4:], defname, encoding,
                               with_date=True)
    change.dest = _find_name(next[4:], defname, encoding,
                             with_date=True)

    return change

def _handle_context(line, next, change, defname, encoding):
    change.source = _find_name(line[4:], defname, encoding,
                               with_date=True)
    change.dest = _find_name(next[4:], defname, encoding,
                             with_date=True)
    
    return change

_git_handlers = (
    (re.compile(r'rename\s+(?:from|old)\s+(.*)$'),     _handle_rename_from),
    (re.compile(r'rename\s+(?:to|new)\s+(.*)$'),       _handle_rename_to),
    (re.compile(r'copy\s+(?:from)\s+(.*)$'),           _handle_copy_from),
    (re.compile(r'copy\s+(?:to)\s+(.*)$'),             _handle_copy_to),
    (re.compile(r'deleted\s+file\s+mode\s+([0-7]+)$'), _handle_delete),
    (re.compile(r'new\s+file\s+mode\s+([0-7]+)$'),     _handle_new),
    (re.compile(r'old\s+mode\s+([0-7]+)$'),            _handle_old_mode),
    (re.compile(r'new\s+mode\s+([0-7]+)$'),            _handle_new_mode),
    (re.compile(r'similarity\s+index\s+([0-9]+)$'),    _handle_similarity),
    (re.compile(r'dissimilarity\s+index\s+([0-9]+)$'), _handle_dissimilarity),
#    (re.compile(r'index\s+([A-Fa-f0-9]{40})..([A-Fa-f0-9]{40})(?:\s+([0-7]+))$'),
#     _handle_index),
)

_DIFF_RE = re.compile(r'diff\s+')
_UNIFIED_1_RE = re.compile(r'---\s+')
_UNIFIED_2_RE = re.compile(r'\+\+\+\s+')
_CONTEXT_1_RE = re.compile(r'\*\*\*\s+')
_CONTEXT_2_RE = re.compile(r'---\s+')
_BINARY_RE = re.compile(r'GIT\s+binary\s+patch')

_UNIFIED_HUNK_RE = re.compile(r'@@\s+-(\d+)(?:,(\d+))?\s+\+(\d+)(?:,(\d+))?\s+@@')
_CONTEXT_HUNK_RE = re.compile(r'(?:---|\*\*\*)\s+(\d+)(?:,(\d+))?\s+(?:---|\*\*\*)')

class Linebuffer(object):
    def __init__(self, file_or_str):
        if isinstance(file_or_str, basestring):
            self.f = StringIO.StringIO(file_or_str)
        else:
            self.f = file_or_str
        self.buffered = []
        
    def __iter__(self):
        return self
    
    def next(self):
        if self.buffered:
            return self.buffered.pop(0)
        return self.f.next()

    def push(self, line):
        self.buffered.append(line)

    def readline(self):
        try:
            return self.next()
        except StopIteration:
            return ''

def _parse_unified_header(line):
    m = _UNIFIED_HUNK_RE.match(line)
    if not m:
        return 0, 0, 0, 0
    
    sa, la, sb, lb = m.groups()
    if la is None:
        la = 1
    if lb is None:
        lb = 1
    sa = int(sa)
    la = int(la)
    sb = int(sb)
    lb = int(lb)

    return sa, la, sb, lb

def _parse_context_header(line):
    m = _CONTEXT_HUNK_RE.match(line)
    if not m:
        return 0, 0

    s, l = m.groups()
    if l is None:
        l = 1
    s = int(s)
    l = int(l)

    return s, l

def _parse_unified(change, line, source, encoding):
    """Parse a unified diff, adding it to the specified Change; returns
    the Change object."""
    sa, la, sb, lb = _parse_unified_header(line)

    # With non-git-format, it's possible that we won't detect an add or
    # delete until this point.  Handle it here by creating a new Change
    # object.
    if sa == 0 and la == 0 and not isinstance(change, Add):
        change = Add(change.dest, None)
    elif sb == 0 and lb == 0 and not isinstance(change, Delete):
        change = Delete(change.source, None)
                
    hunk = TextHunk(sa, la, sb, lb)
    while la or lb:
        hline = source.readline()
        if not hline or hline in ['\n', '\r\n']:
            hunk.lines.append((' ', hline))
            la -= 1
            lb -= 1
            continue

        hl = (hline[0], hline[1:])

        hunk.lines.append(hl)
        if hl[0] in ' -':
            la -= 1
        if hl[0] in ' +':
            lb -= 1

    # Check for \ No newline at end of file
    hline = source.readline()
    if hline.startswith(r'\ '):
        hunk.fix_final_newline()
    else:
        source.push(hline)

    change.hunks.append(hunk)
    
    return change

def _parse_context(change, line, source, encoding):
    """Parse a context diff, adding it to the specified Change; returns
    the Change object."""
    # Parse the first part
    sa, la = _parse_context_header(line)

    # With non-git-format, it's possible that we won't detect an add or
    # delete until this point.  Handle it here by creating a new Change
    # object.
    if sa == 0 and la == 0 and not isinstance(change, Add):
        change = Add(change.dest, None)

    hunk = TextHunk(sa, la, 0, 0)
    while la:
        hline = source.readline()
        if not hline:
            break
        if hline.startswith('---'):
            source.push(hline)
            break

        if hline.startswith('! ') or hline.startswith('- '):
            hunk.lines.append(('-', hline[2:]))
        elif hline.startswith('  '):
            hunk.lines.append((' ', hline[2:]))

    # Check for \ No newline at end of file
    hline = source.readline()
    if hline.startswith(r'\ '):
        hunk.fix_final_newline()
    else:
        source.push(hline)

    # Parse the second part
    sb, lb = _parse_context_header(line)

    # Only for non-git format
    if sb == 0 and lb == 0 and not isinstance(change, Delete):
        change = Delete(change.source, None)

    ndx = 1
    while lb:
        hline = source.readline()
        if not hline:
            break

        if hline.startswith('! ') or hline.startswith('+ '):
            n = ('+', hline[2:])
        elif hline.startswith('  '):
            n = (' ', hline[2:])

        while True:
            if ndx >= len(hunk.lines):
                h = (' ', '')
            else:
                h = hunk.lines[ndx]
            ndx += 1
            if h == n:
                break
            elif h[0] == '-':
                continue
            else:
                hunk.lines.insert(ndx - 1, n)

    # Check for \ No newline at end of file
    hline = source.readline()
    if hline.startswith(r'\ '):
        hunk.fix_final_newline()
    else:
        source.push(hline)

    change.hunks.append(hunk)

    return change

_METHOD_LINE_RE = re.compile(r'(\w+)\s+(\d+)')

def _parse_binary_hunk(change, source, encoding, reverse):
    """Parse a GIT binary patch hunk and return it."""
    line = source.readline()
    m = _METHOD_LINE_RE.match(line)
    if not m:
        source.push(line)
        return None

    method = m.group(1)
    if method not in ('literal', 'delta'):
        source.push(line)
        return None

    orig_len = long(m.group(2))
    data = []
    while True:
        line = source.readline().strip()

        # This is terminated by a blank line
        if not line:
            break
        
        if len(line) < 6 or (len(line) - 1) % 5:
            raise BadBinaryHunk('corrupt binary patch - bad line length')

        blen = ord(line[0])
        if blen >= ord('A') and blen <= ord('Z'):
            blen = blen - ord('A') + 1
        elif blen >= ord('a') and blen <= ord('z'):
            blen = blen - ord('a') + 27
        else:
            raise BadBinaryHunk('corrupt binary patch - bad length byte')

        new_data = base85_decode(line[1:])

        if len(new_data) < blen:
            raise BadBinaryHunk('corrupt binary patch - length mismatch')
        
        data.append(new_data[:blen])

    try:
        data = zlib.decompress(''.join(data))
    except:
        raise BadBinaryHunk('corrupt binary patch - unable to decompress')

    return BinaryHunk(orig_len, method, data, reverse)

def _parse_binary(change, source, encoding, default_filename):
    """Parse a GIT binary patch."""
    if change.source is None:
        change.source = default_filename
    if change.dest is None:
        change.dest = default_filename
    
    forward = _parse_binary_hunk(change, source, encoding, False)
    if not forward:
        raise BadBinaryHunk('unrecognised binary patch')
    change.hunks.append(forward)
    
    reverse = _parse_binary_hunk(change, source, encoding, True)
    if reverse:
        change.hunks.append(reverse)

    change.binary = True
    return change
    
def parse(file_or_str, encoding='utf-8'):
    """Parse a set of patches in diff format, yielding Change objects
    describing each of the changes therein."""
    
    source = Linebuffer(file_or_str)
    default_filename = None
    change = None
    scanning_git = False
    mode = None
    
    for line in source:
        if _DIFF_RE.match(line):
            if change:
                yield change

            scanning_git = bool(_GIT_RE.match(line))
            default_filename = extract_filename(line.rstrip(' \t\r\n'))
            change = Change()
        elif _UNIFIED_1_RE.match(line):
            next = source.readline()
            if not _UNIFIED_2_RE.match(next):
                
                source.push(next)
                continue
            change = _handle_unified(line, next, change, default_filename,
                                     encoding)
            mode = 'unified'
            scanning_git = False
        elif _CONTEXT_1_RE.match(line):
            next = source.readline()
            if not _CONTEXT_2_RE.match(next):
                source.push(next)
                continue
            change = _handle_context(line, next, change, default_filename,
                                     encoding)
            mode = 'context'
            scanning_git = False
        elif _BINARY_RE.match(line):
            change = _parse_binary(change, source, encoding, default_filename)
        elif mode == 'unified' and line.startswith('@'):
            change = _parse_unified(change, line, source, encoding)
        elif mode == 'context' and line.startswith('***************'):
            change = _parse_context(change, line, source, encoding)
        elif scanning_git:
            sline = line.rstrip(' \t\r\n')
            for rx, handler in _git_handlers:
                m = rx.match(sline)
                if m:
                    change = handler(sline, m, change, default_filename,
                                     encoding)
                    break

    if change:
        yield change
    
