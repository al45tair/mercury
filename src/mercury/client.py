import subprocess, os, os.path, struct
import cStringIO, datetime

from mercury.exceptions import *

class SimpleErrorHandler(object):
    """
    This class is meant to be used with execute() error handler argument.
    It remembers the return value the command returned if it's one of allowed
    values, which is only 1 if none are given. Otherwise it raises a CommandError.

    >>> e = SimpleErrorHandler()
    >>> bool(e)
    True
    >>> e('', 1, 'a', '')
    'a'
    >>> bool(e)
    False
    """
    def __init__(self, allowed=None):
        self._ret = 0
        if allowed is None:
            self._allowed = frozenset((1,))
        else:
            self._allowed = frozenset(allowed)

    def __call__(self, args, ret, out, err):
        self._ret = ret
        if self._ret not in self._allowed:
            raise CommandError(args, ret, out, err)
        return out

    def __nonzero__(self):
        return self._ret == 0

class PipeFileWrapper(object):
    """A file-like object used to provide reasonable error handling behaviour
    for the Client.get_file() method (and therefore the Repository.open() and
    Changeset.open() methods when a revision is specified)."""
    
    def __init__(self, process, encoding):
        self._process = process
        self._encoding = encoding

        # Read up to two output characters so that we can check for
        # errors... otherwise we'd have to wait for the first actual read,
        # which is too late really to indicate that an open() has failed.
        
        self._outbuf = self._process.stdout.read(1)
        if self._outbuf == '\r':
            self._outbuf += self._process.stdout.read(1)
        self._check_errors()
        
    def _check_errors(self):
        ret = self._process.poll()
        if ret is not None and ret != 0:
            err = self._process.stderr.read()
            raise PipeError(ret, err)
        
    def close(self):
        self._check_errors()
        self._process.wait()

    def flush(self):
        self._check_errors()

    def fileno(self):
        return self._process.stdin.fileno()

    def isatty(self):
        return False

    def __iter__(self):
        return self
    
    def next(self):
        self._check_errors()
        line = self.readline()
        if not line:
            raise StopIteration()
        if line.endswith('\r\n'):
            return line[:-2]
        elif line[-1] in '\r\n':
            return line[:-1]
        return line

    def read(self, size=None):
        self._check_errors()

        # If we have a buffered character, grab it first
        if self._outbuf:
            ob = self._outbuf
            self._outbuf = None

            if size is None:
                return ob + self._process.stdout.read()
            else:
                return self._outbuf + self._process.stdout.read(size - len(ob))
        
        if size is None:
            return self._process.stdout.read()
        else:
            return self._process.stdout.read(size)

    def readline(self, size=None):
        self._check_errors()

        # Deal with anything we have buffered first
        if self._outbuf:
            ob = self._outbuf
            if self._outbuf == '\r\n':
                self._outbuf = None
                return ob
            elif self._outbuf[0] in '\r\n':
                if len(self.outbuf) == 2:
                    self._outbuf = self._outbuf[1]
                else:
                    self._outbuf = None
                return ob[0]

            self._outbuf = None
            
            if size is None:
                return ob + self._process.stdout.readline()
            else:
                return ob + self._process.stdout.read(size - len(ob))
                
        if size is None:
            return self._process.stdout.readline()
        else:
            return self._process.stdout.readline(size)

    def readlines(self, sizehint=None):
        return list(self)

    def xreadlines(self):
        return iter(self)

    def seek(self, offset, whence=None):
        raise IOError('cannot seek a revision file')

    def tell(self):
        if self._outbuf:
            return self._process.stdout.tell() - len(self._outbuf)
        return self._process.stdout.tell()

    def truncate(self, size=None):
        raise IOError('cannot truncate a revision file')

    def write(self, str):
        raise IOError('cannot write to a revision file')

    def writelines(self, sequence):
        raise IOError('cannot write to a revision file')

    @property
    def closed(self):
        return self._process.stdout.closed

    @property
    def encoding(self):
        return self._encoding

    @property
    def mode(self):
        return self._process.stdout.mode

    @property
    def newlines(self):
        return self._process.stdout.newlines

    @property
    def softspace(self):
        return self._process.stdout.softspace
    
class Client(object):
    """A client of the Mercurial server process.  Do not use this directly;
    instead, use a Repository object."""

    def __init__(self, path=None, encoding='utf-8', configs=None, hg=None):
        if not hg:
            for searchpath in os.environ['PATH'].split(os.pathsep):
                possible_hg = os.path.join(searchpath, 'hg')
                if os.access(possible_hg, os.X_OK):
                    hg = possible_hg
                    break
            if not hg:
                raise MercurialNotFound('Could not find an hg executable in your PATH')
        
        if not path:
            path = os.getcwd()

        if not os.access(os.path.join(path, '.hg'), os.F_OK):
            raise NotARepositoryError('%s is not a valid Mercurial repository'
                                      % path)

        self._args = [hg, 'serve', '--cmdserver', 'pipe',
                      '--config', 'ui.interactive=True',
                      '--config', 'extensions.hglist=',
                      '-R', path]
        self._path = path

        if configs:
            self._args += ['--config'] + configs
        self._env = { 'HGPLAIN': '1' }
        self._env['HGENCODING'] = encoding
        self._default_encoding = encoding
        self._encoding = self._default_encoding
        self._server = None
        self._version = None
        self.debug = False

    def __enter__(self):
        return self

    def __exit(self, exc_type, exc_val, exc_tb):
        self.disconnect()

    def connect(self):
        """Spawns a new Mercurial server instance"""
        if self._server is not None:
            raise AlreadyConnected('This Client instance is already connected')

        env = dict(os.environ)
        env.update(self._env)
        
        self._server = subprocess.Popen(self._args,
                                        stdin=subprocess.PIPE,
                                        stdout=subprocess.PIPE,
                                        stderr=subprocess.PIPE,
                                        env=env)

        self._read_hello()

    def disconnect(self):
        """Destroys the Mercurial server instance, returning its exit code."""
        self._server.stdin.close()
        self._server.wait()
        ret = self._server.returncode
        self._server = None
        return ret

    @property
    def encoding(self):
        return self._encoding
    
    def _read(self):
        """Read a message from the server, returning a (channel, message) tuple.

        If the channel is an input channel, the server is requesting input,
        in which case the `message' field will contain the requested length."""
        data = self._server.stdout.read(5)
        if not data:
            raise ProtocolError()
        channel, length = struct.unpack('>cI', data)
        if channel in 'IL':
            return (channel, length)
        else:
            return (channel, self._server.stdout.read(length))

    def _write(self, data):
        """Write a message to the server."""
        self._server.stdin.write(struct.pack('>I', len(data)))
        self._server.stdin.write(data)
        self._server.stdin.flush()

    def _read_hello(self):
        """On initial connection to the server, Mercurial sends a `hello'
        message; this reads and parses it."""
        channel, message = self._read()

        if channel != 'o':
            raise ProtocolError('Expected a hello message')

        self._server_info = {}
        for line in message.split('\n'):
            key, value = line.split(':', 1)
            self._server_info[key.lower().strip()] = value.strip()

        caps = self._server_info.get('capabilities', None)
        if not caps:
            raise ProtocolError('Bad hello message; missing capabilities header')

        self._capabilities = frozenset(caps.split())

        if 'runcommand' not in self._capabilities:
            raise CapabilityError('Mercurial server must support runcommand')

        encoding = self._server_info.get('encoding', None)
        if not encoding:
            raise ProtocolError('Bad hello message; missing encoding header')

        self._encoding = encoding

    def _execute(self, args, inputs, outputs):
        if not self._server:
            self.connect()

        self._server.stdin.write('runcommand\n')
        self._write('\0'.join(args))

        while True:
            channel, data = self._read()

            if channel in inputs:
                self._write(inputs[channel](data))
            elif channel in outputs:
                outputs[channel](data)
            elif channel.isupper():
                raise ChannelError('unexpected data on required channel "%s"'
                                   % channel)
            
            if channel == 'r':
                return struct.unpack('>i', data)[0]

    def raw_execute(self, args, eh=None, prompt=None, input=None,
                    use_server=True, binary=False):
        """Send a command to the server to execute, returning any output.

        args are the command line arguments; it is safe to use quotes and
        other special characters in the argument string.

        eh is an error handler, called when the return code is non-zero.
        For instance:

          def myErrorHandler(returnCode, output, stderrOut):
              print 'The return code was %s' % returnCode
              print 'The server replied with %s' % output
              print 'The server generated the following errors: %s' % stderrOut

        If the error handler returns a value, that will be the returned
        value from the function.  If no error handler is specified, we
        raise a CommandError().

        prompt is called when the server asks for input; it receives the
        maximum number of bytes to return and a copy of the output so far
        from the server.

        input is called when the server asks for bulk data; it receives the
        maximum number of bytes to return."""

        if self.debug:
            print 'sending: %r' % args

        if not use_server:
            env = dict(os.environ)
            env.update(self._env)
            cmd = subprocess.Popen([self._args[0]] + args,
                                   stdin=subprocess.PIPE,
                                   stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE,
                                   env=env)
            if input:
              in_data = input(0)
            else:
              in_data = None
              
            out, err = cmd.communicate(in_data)

            ret = cmd.wait()
        else:
            out, err = cStringIO.StringIO(), cStringIO.StringIO()
            outputs = { 'o': out.write, 'e': err.write }

            inputs = {}
            if prompt is not None:
                inputs['L'] = lambda size: str(prompt(size, out.getvalue()))
            if input is not None:
                inputs['I'] = input

            ret = self._execute(args, inputs, outputs)
            out, err = out.getvalue(), err.getvalue()

        if not binary and isinstance(out, str):
            out = out.decode(self._encoding)
        if isinstance(err, str):
            err = err.decode(self._encoding)
            
        if ret:
            if self.debug:
                print 'failed with error: %r' % err
                
            if eh is None:
                raise CommandError(args, ret, out, err)
            else:
                return eh(args, ret, out, err)

        if self.debug:
            print 'got output: %r' % out

        return out

    def build_args(self, *args, **kwargs):
        """Convert arguments from Python form to something suitable for
        passing to the execute() method.

        Ordinary arguments are converted to strings (dates and times are
        supported by converting to ISO 8601 format).  If an argument is
        iterable, it is replaced by the results of its iterator.

        Keyword arguments turn into switches; e.g. r='foo' will become
        -r foo, while rev='foo' turns into --rev foo.

        Keyword arguments with the values None or False are omitted;
        those with the value True are specified without any additional
        data.  Keyword arguments whose value is iterable are repeated
        with each of the iteration results."""

        def convert(arg):
            if isinstance(arg, unicode):
                return arg.encode(self._encoding)
            elif isinstance(arg, datetime.date) \
                 or isinstance(arg, datetime.time) \
                 or isinstance(arg, datetime.datetime):
                return arg.isoformat()
            else:
                return str(arg)

        cmd = []
        for kw,arg in kwargs.iteritems():
            kw = kw.replace('_', '-')
            if kw != '-':
                if len(kw) > 1:
                    switch = '--' + kw
                else:
                    switch = '-' + kw

            if arg is None:
                continue
            elif isinstance(arg, bool):
                if arg:
                    cmd.append(switch)
            elif getattr(arg, '__iter__', None):
                for a in arg:
                    cmd.append(switch)
                    cmd.append(convert(a))
            else:
                cmd.append(switch)
                cmd.append(convert(arg))

        for arg in args:
            if arg is None:
                continue
            elif getattr(arg, '__iter__', None):
                for a in arg:
                    cmd.append(convert(a))
            else:
                cmd.append(convert(arg))

        return cmd

    def execute(self, cmd_name, *args, **kwargs):
        """Execute a command after building its arguments."""
        # Pull-out a few keyword args
        eh = kwargs.pop('eh', None)
        prompt = kwargs.pop('prompt', None)
        input = kwargs.pop('input', None)
        use_server = kwargs.pop('use_server', True)
        binary = kwargs.pop('binary', False)
        
        cmd = self.build_args(*args, **kwargs)
        return self.raw_execute([cmd_name] + cmd,
                                eh=eh, prompt=prompt, input=input,
                                use_server=use_server,
                                binary=binary)

    def get_file(self, name, mode, revision):
        """Spawns a new hg instance to obtain the content of the specified
        file at the specified revision.  Returns a file object."""
        if not mode in ['r', 'rb', 'rt']:
            raise ValueError('only "r", "rb" and "rt" are valid modes')

        text_mode = mode != 'rb'

        env = dict(os.environ)
        env.update(self._env)
        cmd = subprocess.Popen([self._args[0],
                                '-R', self._path,
                                'cat',
                                '-r', str(revision),
                                name],
                               stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE,
                               universal_newlines=text_mode,
                               env=env)

        return PipeFileWrapper(cmd, self._default_encoding)
    
    @property
    def version(self):
        """Return the hg version running as the command server as a tuple
        (major, minor, bugfix, build)"""
        if self._version is None:
            version = self.execute('version', '-q')
            v = list(re.match(r'.*?(\d+)\.(\d+)\.?(\d+)?(\+[0-9a-f-]+)?',
                              version).groups())

            for i in range(3):
                try:
                    v[i] = int(v[i])
                except TypeError:
                    v[i] = 0

            self._version = tuple(v)

        return self._version
