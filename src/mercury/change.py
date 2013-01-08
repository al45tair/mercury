import binascii, re

from mercury.utils import every, decode_delta

class Change(object):
    kind = 'change'
    
    def __init__(self, source=None, dest=None):
        self.binary = False
        self.hunks = []
        self.source = source
        self.dest = dest

    def __repr__(self):
        return '%s(source=%r, dest=%r)' % (self.__class__.__name__,
                                           self.source, self.dest)

    def _unified_headers(self, extra_src='', extra_dest=''):
        src = self.source or '/dev/null'
        dest = self.dest or '/dev/null'
        return '--- %s\t%s\n+++ %s\t%s' % (src, extra_src,
                                           dest, extra_dest)
    
    def __str__(self):
        result = [self._unified_headers()]
        for h in self.hunks:
            result.append(str(h))
        return '\n'.join(result)
    
class Rename(Change):
    kind = 'rename'

class Copy(Change):
    kind = 'copy'

class Delete(Change):
    kind = 'delete'
    
    def __init__(self, source, old_mode):
        super(Delete, self).__init__(source=source)
        self.old_mode = old_mode

    def __repr__(self):
        return 'Delete(source=%r, old_mode=0%o)' % (self.source, self.old_mode)

    def _unified_headers(self, extra_src='', extra_dest=''):
        src = self.source
        dest = '/dev/null'
        return '--- %s\t%s\n+++ %s\t%s' % (src, extra_src,
                                           dest, extra_dest)

class Add(Change):
    kind = 'add'
    
    def __init__(self, dest, mode):
        super(Add, self).__init__(dest=dest)
        self.new_mode = mode

    def __repr__(self):
        return 'Add(dest=%r, mode=0%o)' % (self.source, self.new_mode)

    def _unified_headers(self, extra_src='', extra_dest=''):
        src = '/dev/null'
        dest = self.dest
        return '--- %s\t%s\n+++ %s\t%s' % (src, extra_src,
                                           dest, extra_dest)

class Hunk(object):
    binary = False

class TextHunk(Hunk):
    def __init__(self, start_a, len_a, start_b, len_b):
        self.start_a = start_a
        self.len_a = len_a
        self.start_b = start_b
        self.len_b = len_b
        self.lines = []

    def fix_final_newline(self):
        l = self.lines[-1]
        if l[1].endswith('\r\n'):
            nl = l[1][:-2]
        else:
            nl = l[1][:-1]
        self.lines[-1] = (l[0], nl)

    def __repr__(self):
        return 'TextHunk(%r, %r, %r, %r)' % (self.start_a,
                                             self.len_a,
                                             self.start_b,
                                             self.len_b)

    def __str__(self):
        result = ['@@ -%d,%d +%d,%d @@' % (self.start_a, self.len_a,
                                           self.start_b, self.len_b)]
        for l in self.lines:
            result.append('%s%s' % (l[0], l[1].rstrip('\r\n')))

        return '\n'.join(result)

_NOT_PRINTABLE = re.compile(r'[^ -~]')
class BinaryHunk(Hunk):
    binary = True
    
    def __init__(self, length, method, data=None, reverse=False):
        self.length = length
        self.method = method
        self.data = data
        self.reverse = reverse

    def __str__(self):
        reverse = ''
        if self.reverse:
            reverse = ' (reverse)'
        result = ['binary hunk type=%s len=%s%s' % (self.method,
                                                    self.length,
                                                    reverse)]

        def output(offset, chunk):
            hexchunk = ' '.join(list(every(binascii.b2a_hex(chunk), 2)))
            pchunk = _NOT_PRINTABLE.sub('.', chunk)
            result.append('%08x: %-47s  %s' % (offset, hexchunk, pchunk))
        
        if self.method == 'literal':
            offset = 0
            
            for offset in xrange(0, len(self.data) - 15, 16):
                output(offset, self.data[offset:offset+16])

            offset += 16
            if offset < len(self.data):
                output(offset, self.data[offset:])
        elif self.method == 'delta':
            todo = self.length
            try:
                for offset, kind, info in decode_delta(data):
                    if kind == 'source':
                        src_offset, size = info
                        if todo < size:
                            result.append('bad delta - incorrect length')
                            break

                        result.append('%08x: copy %s bytes from source at %08x'
                                      % (offset, size, src_offset))

                        todo -= size
                    elif kind == 'delta':
                        if todo < len(info):
                            result.append('bad delta - incorrect length')
                            break
                        
                        output(offset, info)
            except BadBinaryDelta, e:
                result.append('bad delta - %s' % e.args[0])
        else:
            result.append('<unknown method>')
            
        return '\n'.join(result)

        
