import datetime
from mercury.exceptions import *

def every(l, n):
    """Yield every piece of length n from list l."""
    for ndx in xrange(0, len(l) - n + 1, n):
        yield l[ndx:ndx+n]

def decode_delta(delta):
    """Decode a GIT-format binary delta, yielding tuples

    (offset, 'source', (src_offset, size))
    means copy size bytes from the original file at offset src_offset

    (offset, 'delta', byte_string)
    means copy byte_string

    In both cases, offset is the offset in the resulting file."""
    ptr = 0
    offset = 0
    dlen = len(delta)
    while ptr < dlen:
        cmd = delta[ptr]; ptr += 1
        if cmd & 0x80:
            # Copy from source (i.e. original)
            src_offset = 0
            size = 0
            if cmd & 0x01: src_offset = delta[ptr]; ptr += 1
            if cmd & 0x02: src_offset |= delta[ptr] << 8; ptr += 1
            if cmd & 0x04: src_offset |= delta[ptr] << 16; ptr += 1
            if cmd & 0x08: src_offset |= delta[ptr] << 24; ptr += 1

            if cmd & 0x10: size = delta[ptr]; ptr += 1
            if cmd & 0x20: size |= delta[ptr] << 8; ptr += 1
            if cmd & 0x40: size |= delta[ptr] << 16; ptr += 1

            if size == 0:
                size = 0x10000

            yield (offset, 'source', src_offset, size)

            offset += size
            todo -= size
        elif cmd:
            # Copy bytes from the delta
            if ptr + cmd > dlen:
                raise BadBinaryDelta('delta was truncated')

            yield (offset, 'delta', delta[ptr:ptr+cmd])
        else:
            raise BadBinaryDelta('unknown opcode 0x00')

class LRUCache(object):
    """A least-recently-used cache"""
    
    def __init__(self, cache_size):
        self.cache_size = cache_size
        self.cache = []
        self.cache_keys = []

    def __getitem__(self, key):
        try:
            ndx = self.cache_keys.index(key)
        except ValueError:
            return None

        value = self.cache[ndx]
        del self.cache[ndx]
        del self.cache_keys[ndx]
        self.cache_keys.append(key)
        self.cache.append(value)

        return value

    def __setitem__(self, key, value):
        try:
            ndx = self.cache_keys.index(key)
            del self.cache[ndx]
            del self.cache_keys[ndx]
        except ValueError:
            pass

        if len(self.cache) >= self.cache_size:
            del self.cache[0]
            del self.cache_keys[0]

        self.cache_keys.append(key)
        self.cache.append(value)

    def __len__(self):
        return len(self.cache)

    def __iter__(self):
        for n in xrange(0, len(self.cache)):
            yield (self.cache_keys[n], self.cache[n])

    def iteritems(self):
        return self.__iter__()

    def iterkeys(self):
        return iter(self.cache_keys)

_ZERO = datetime.timedelta(0)

class UTC(datetime.tzinfo):
    """UTC"""

    def utcoffset(self, dt):
        return _ZERO

    def tzname(self, dt):
        return "UTC"

    def dst(self, dt):
        return _ZERO

_UTC = UTC()

def datetime_from_timestamp(ts):
    return datetime.datetime.utcfromtimestamp(ts).replace(tzinfo=_UTC)
