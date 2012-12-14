import weakref
import bisect
import datetime
import re
import os
import os.path
import errno
import tempfile
import socket
import pipes
import threading
import urlparse
import itertools

from mercury.client import Client, SimpleErrorHandler
from mercury.exceptions import *
from mercury.queryset import RepoQueryset, Queryset, SingleRevQueryset
from mercury.utils import every, LRUCache, datetime_from_timestamp
from mercury import diffparser

class AnnotatedString(unicode):
    __slots__ = ['user', 'file', 'date', 'changeset', 'line']

    _USER_RE = re.compile(r'<([^@>]+)@[^>]+>')
    @property
    def short_user(self):
        m = AnnotatedString._USER_RE.search(self.user)
        if not m:
            return self.user
        return m.group(1)

class GrepString(unicode):
    __slots__ = ['user', 'file', 'date', 'rev', 'line', 'type']
    
    _USER_RE = re.compile(r'<([^@>]+)@[^>]+>')
    @property
    def short_user(self):
        m = AnnotatedString._USER_RE.search(self.user)
        if not m:
            return self.user
        return m.group(1)

class GrepResult(object):
    __slots__ = ['user', 'file', 'date', 'rev', 'line', 'type']
    
    _USER_RE = re.compile(r'<([^@>]+)@[^>]+>')
    @property
    def short_user(self):
        m = AnnotatedString._USER_RE.search(self.user)
        if not m:
            return self.user
        return m.group(1)
    
class SimpleTzInfo(datetime.tzinfo):
    __slots__ = ['_offset']
    def __init__(self, offset):
        self._offset = offset

    def utcoffset(self, dt):
        return datetime.timedelta(minutes=self._offset)

    def dst(self, dt):
        return datetime.timedelta(0)

    def tzname(self, dt):
        if self._offset > 0:
            d, r = divmod(self._offset, 60)
            return '+%02s%02s' % (d, r)
        elif self._offset < 0:
            d, r = divmod(-self._offset, 60)
            return '-%02s%02s' % (d, r)

class Changeset(object):
    PUBLIC = 'public'
    DRAFT = 'draft'
    SECRET = 'secret'
    
    """Represents a changeset in a Mercurial repository."""
    def __init__(self, repo, rev, node, info=None):
        self._repo = repo
        self._rev = rev
        self._node = node
        self._parents = None
        self._manifest = None
        self._fetched = False
        if info:
            self._init_from_info(info)

    def __cmp__(self, other):
        if not isinstance(other, Changeset):
            raise TypeError('Can only compare Changeset objects with other Changeset objects')
        if self._rev > other.rev:
            return +1
        elif self._rev < other.rev:
            return -1
        else:
            return 0

    def __int__(self):
        return self._rev

    def __str__(self):
        return self._node[:12]

    def __repr__(self):
        return '<Changeset %s:%s>' % (self._rev, self._node[:12])

    def __hash__(self):
        return hash(self._rev)
        
    def __eq__(self, other):
        if not isinstance(other, Changeset):
            return False
        return self._repo == other._repo and self._rev == other._rev

    def __ne__(self, other):
        return not (self == other)

    def __len__(self):
        return len(self._repo._fetch_changes(self))
    
    def __iter__(self):
        return iter(self._repo._fetch_changes(self))

    def __getitem__(self, key):
        if not isinstance(key, (long, int)) and not isinstance(key, slice):
            raise TypeError()
        return self._repo._fetch_changes(self)[key]

    def __nonzero__(self):
        return True

    @property
    def repository(self):
        return self._repo
    
    @property
    def rev(self):
        return self._rev

    @property
    def node(self):
        return self._node

    @property
    def tags(self):
        if not self._fetched:
            self._fetch()
        return self._tags

    @property
    def branch(self):
        if not self._fetched:
            self._fetch()
        return self._branch

    @property
    def author(self):
        if not self._fetched:
            self._fetch()
        return self._author

    @property
    def desc(self):
        if not self._fetched:
            self._fetch()
        return self._desc
    
    @property
    def desc(self):
        if not self._fetched:
            self._fetch()
        return self._desc

    @property
    def date(self):
        if not self._fetched:
            self._fetch()
        return self._date

    @property
    def phase(self):
        if not self._fetched:
            self._fetch()
        return self._phase

    @phase.setter
    def phase(self, new_phase):
        if self._repo._set_phase(self._node, new_phase):
            self._phase = new_phase
        else:
            raise Exception('unable to set phase')

    def set_phase(self, new_phase, force=False):
        if self._repo._set_phase(self._node, new_phase, force=force):
            self._phase = new_phase
        else:
            raise Exception('unable to set phase')
    
    @property
    def parents(self):
        if self._parents is None:
            if not self._fetched:
                self._fetch()
                
            if self._p1rev == -1:
                self._parents = ()
            else:
                p1 = self._repo._get_lazy(self._p1rev, self._p1node)
                if self._p2rev == -1:
                    self._parents = (p1,)
                else:                    
                    p2 = self._repo._get_lazy(self._p2rev, self._p2node)
                    self._parents = (p1, p2)

        return self._parents

    @property
    def p1rev(self):
        return self._p1rev
    
    @property
    def p1node(self):
        return self._p1node
    
    @property
    def p2rev(self):
        return self._p2rev
    
    @property
    def p2node(self):
        return self._p2node    

    @property
    def children(self):
        return self._repo.query('children(%0)', self)

    @property
    def ancestors(self):
        return self._repo.query('ancestors(%0) and not %0', self)

    @property
    def descendants(self):
        return self._repo.query('descendants(%0) and not %0', self)

    def _fetch_manifest(self):
        self._manifest = self._repo._get_manifest(self._node)

    @property
    def manifest(self):
        if self._manifest is None:
            self._fetch_manifest()
        return self._manifest

    def changes(self,
                ignore_all_space=False,
                ignore_space_change=False,
                ignore_blank_lines=False,
                context=None):
        """Returns a generator that yields Change objects corresponding to
        this Changeset.  The result is not cached."""
        return self._repo.changes(change=cset,
                                  ignore_all_space=ignore_all_space,
                                  ignore_space_change=ignore_space_change,
                                  ignore_blank_lines=ignore_blank_lines,
                                  context=context)

    def _init_from_info(self, info):
        self._tags = info[2].split()
        self._branch = info[3]
        self._author = info[4]
        self._desc = info[5]
        utc = float(info[6].split('.', 1)[0])
        self._date = datetime_from_timestamp(utc)
        self._p1rev = int(info[7])
        self._p1node = info[8]
        self._p2rev = int(info[9])
        self._p2node = info[10]
        self._phase = info[11]
        self._fetched = True
        
    def _fetch(self):
        self._repo._fetch_lazy(self)

    def open(self, name, mode='r'):
        """Open the given file in this revision.  mode must be 'r', 'rb' or
        'rt'; you cannot write to a historic revision.

        Note: this may be an expensive operation."""
        return self._repo.open(name, mode, rev=self)

class BaseRepo(object):
    def __init__(self):
        self._live_changesets = weakref.WeakValueDictionary()
        
    def _cset_from_info(self, info):
        cset = self._live_changesets.get(info[1])
        if not cset:
            cset = Changeset(self, info[0], info[1], info)
            self._live_changesets[info[1]] = cset
        return cset
    
class RemoteRepository(BaseRepo):
    """Represents a remote Mercurial repository.  The only thing you can do
    with such an object is retrieve its URL."""
    def __init__(self, url):
        super(RemoteRepository, self).__init__()
        self._url = url

    @property
    def path(self):
        return None

    @property
    def url(self):
        return self._url

    def _get_manifest(self, node):
        raise RemoteRepositoryError('cannot fetch changeset manifest from remote repository')
    
    def _fetch_lazy(self, cset):
        raise RemoteRepositoryError('cannot fetch changeset information from remote repository')

    def _set_phase(self, cset, phase):
        raise RemoteRepositoryError('cannot set phase for changeset from remote repository')

    def query(self, *args, **kwargs):
        raise RemoteRepositoryError('cannot query a remote repository')

    def open(self, name, mode, rev):
        raise RemoteRepositoryError('cannot open a file from a remote repository')

_thread_local = threading.local()

class Repository(BaseRepo):
    """Represents a Mercurial repository."""
    _TEMPLATE = r'{rev}\0{node}\0{tags}\0{branch}\0{author}\0{desc}\0{date}\0{p1rev}\0{p1node}\0{p2rev}\0{p2node}\0{phase}\0'
    _LIST_TEMPLATE = r'{rev}\0{node}\0{name}\0'
    
    _LRU_CACHE_SIZE = 16

    def __new__(cls, path, encoding='utf-8', client=None):
        live_repos = getattr(_thread_local, 'live_repos', None)
        if live_repos is None:
            live_repos = weakref.WeakValueDictionary()
            _thread_local.live_repos = live_repos

        r = live_repos.get(path, None)
        if r is None:
            r = super(Repository, cls).__new__(cls, path, encoding, client)
            live_repos[path] = r

        return r
        
    def __init__(self, path, encoding='utf-8', client=None):
        if getattr(self, '_initialized', False):
            return
        self._initialized = True

        super(Repository, self).__init__()
        
        parsed = urlparse.urlparse(path)
        if parsed.scheme:
            if parsed.scheme != 'file':
                raise ValueError('cannot create a Repository for a remote repo; please clone it instead')
            else:
                url = path
                path = parsed.path
        else:
            url = urlparse.urlunparse(('file', '', os.path.abspath(path),
                                       '', '', ''))
            
        if client is None:
            client = Client(path, encoding)
        self._url = url
        self._path = path
        self._client = client
        self._lru_cache = []
        self._change_cache = LRUCache(16)
        
        # Replace clone() with a non-static version
        def new_clone(self, *args, **kwargs):
            return Repository.clone(self, *args, **kwargs)
        self.clone = new_clone

    @property
    def path(self):
        return self._path

    @property
    def url(self):
        return self._url
   
    @property
    def server_version(self):
        return self._client.version

    @property
    def changesets(self):
        return RepoQueryset(self)

    @property
    def heads(self):
        return self.changesets.heads()

    @property
    def tip(self):
        return self['tip']

    @property
    def current(self):
        return self['.']

    def _update_cache(self, cset):
        """Update the LRU changeset cache by adding the specified changeset"""
        # If this changeset is already in the cache, remove it
        try:
            self._lru_cache.remove(cset)
        except ValueError:
            pass

        # Add the changeset at the end
        if len(self._lru_cache) >= Repository._LRU_CACHE_SIZE:
            del self._lru_cache[0]
        self._lru_cache.append(cset)

    def _fetch_changes(self, cset):
        changes = self._change_cache[cset.node]
        if changes is None:
            changes = list(self.changes(change=cset))
            self._change_cache[cset.node] = changes
        return changes

    def _fetch(self, changeid, extra_args=[]):
        out = self._client.execute('log', *extra_args,
                                   template=Repository._TEMPLATE,
                                   r=changeid).split('\0')
        return [chunk for chunk in every(out, 12)]

    def _fetch_one(self, changeid):
        out = self._fetch(changeid, ['-l', '2'])
        if not out:
            return None
        elif len(out) > 1:
            raise ValueError('change id must select a single changeset')
        return out[0]

    def _set_phase(self, node, new_phase, force=False):
        public=False
        draft=False
        secret=False
        if new_phase == Changeset.PUBLIC:
            public=True
        elif new_phase == Changeset.DRAFT:
            draft=True
        elif new_phase == Changeset.SECRET:
            secret=True
        else:
            raise ValueError('bad phase')
        
        eh = SimpleErrorHandler()
        out = self._client.execute('phase', r=node,
                                   p=public, d=draft, s=secret, f=force,
                                   eh=eh)
        return bool(eh)
    
    def _get_manifest(self, node):
        out = self._client.execute('list', r=node, recursive=True, all=True,
                                   template=Repository._LIST_TEMPLATE,
                                   binary=True)
        manifest = []
        for rev,node,name in every(out.split('\0'), 3):
            rev = int(rev)
            if rev == -1:
                continue
            manifest.append((name, self._get_lazy(rev, node)))
        return manifest

    def _get_lazy(self, rev, node):
        assert len(node) == 40

        cset = self._live_changesets.get(node)
        if cset:
            if cset._fetched:
                self._update_cache(cset)
        else:
            cset = Changeset(self, rev, node)
            self._live_changesets[node] = cset

            # Don't update the LRU cache with lazy changesets; we defer that
            # until the actual _fetch.  This way, we don't pollute the LRU
            # cache with changesets we have no intention of actually fetching.

        return cset

    def _fetch_lazy(self, cset):
        info = self._fetch_one('id(%s)' % cset._node)
        cset._init_from_info(info)

        # This is the cache update mentioned above
        self._update_cache(cset)

    def __len__(self):
        out = self._client.execute('tip', template='{rev}')
        return int(out) + 1
    
    def __getitem__(self, changeid):
        if isinstance(changeid, slice):
            start, stop, stride = slice.indices(len(self))
            return [self[rev] for rev in xrange(start, stop, stride)]
                
        cset = self._live_changesets.get(changeid)
        if cset:
            self._update_cache(cset)
            return cset

        # We can't do a lazy fetch because we don't know the node id
        info = self._fetch_one(changeid)
        return self._cset_from_info(info)

    def _cset_from_info(self, info):
        cset = super(Repository, self)._cset_from_info(info)
        self._update_cache(cset)
        return cset
    
    def __iter__(self):
        return self.query('all()')

    def __reversed__(self):
        return self.query('reverse(all())')

    _PLACEHOLDER_RE = re.compile(r'%(?:%|(\d+)|([A-Za-z_][0-9A-Za-z_]*))')

    def query(self, query, *args, **kwargs):
        """Accepts a Mercurial revset or revision expression, and yields
        matching Changeset objects.

        The query expression is a string, and may contain placeholders
        corresponding to the arguments passed to the query() method.  Two
        types of placeholder are recognised; numeric placeholders of the
        form '%0' refer to numbered arguments, while placeholders of the
        form '%arg' refer to keyword arguments.  To embed a literal '%',
        use '%%'.  Thus

          repo.query('%0::%arg', 'foo', arg='bar')

        would correspond to the query

          'foo'::'bar'

        When formatting placeholders, strings are replaced with a quoted,
        escaped version, while Changeset arguments turn into a query for
        'id(<node>)' and datetime, date and time objects become an ISO 8601
        format string (with quotes).
        """
        def sub_args(matchobj):
            ndx = matchobj.group(1)
            key = matchobj.group(2)
            if ndx is not None:
                val = args[int(ndx)]
            elif key is not None:
                val = kwargs[key]
            else:
                return '%'
            
            if isinstance(val, Changeset):
                return 'id(%s)' % val.node
            elif isinstance(val, datetime.date) \
                     or isinstance(val, datetime.time) \
                     or isinstance(val, datetime.datetime):
                return '\'%s\'' % val.isoformat()
            elif isinstance(val, basestring):
                if isinstance(val, unicode):
                    val = val.encode('utf-8')
                return '\'%s\'' % re.escape(val)
            else:
                return str(val)

        if isinstance(query, unicode):
            query = query.encode('utf-8')
            
        fmt_query = Repository._PLACEHOLDER_RE.sub(sub_args, query)

        for info in self._fetch(fmt_query):
            cset = self._live_changesets.get(info[1])
            if not cset:
                cset = Changeset(self, info[0], info[1], info)
                self._live_changesets[info[1]] = cset
            self._update_cache(cset)
            yield cset

    def open(self, name, mode='r', rev=None):
        """Open the given file at the given revision.  If the revision is
        specified, mode must be read-only.

        Note that opening files of specified revisions may be an expensive
        operation."""
        if not os.path.isabs(name):
            name = os.path.join(self._path, name)
        if rev is None:
            return open(name, mode)
        else:
            rev = self._map_one_rev(rev)
            return self._client.get_file(name, mode, rev)

    @staticmethod
    def _map_revs(rev):
        """Map Changeset objects to their node values; also copes with
        iterables that generate Changeset objects (e.g. a Queryset or a
        list of Changesets)."""
        if isinstance(rev, Changeset):
            yield rev.node
        elif getattr(rev, '__iter__', None):
            for r in rev:
                if isinstance(r, Changeset):
                    yield r.node
                else:
                    yield r

    @staticmethod
    def _map_branches(branch):
        """Map branches; copes with the case where we have used a
        Queryset to generate branches."""
        if isinstance(branch, Queryset):
            return [cset.branch for cset in branch.branches()]
        else:
            return branch

    @staticmethod
    def _map_one_rev(rev):
        """Map a single Changeset object to its node value; also allows
        a Queryset that results in EXACTLY one Changeset."""
        if isinstance(rev, SingleRevQueryset):
            return rev.name
        if isinstance(rev, Queryset):
            rev = rev.get(exactly=1)[0]
        if isinstance(rev, Changeset):
            rev = rev.node
        return rev

    def _map_files(self, files):
        """Make any relative file paths relative to the repository,
        rather than the cwd.  Absolute paths are left alone."""
        if getattr(files, '__iter__', None):
            for f in files:
                if not os.path.isabs(f):
                    yield os.path.join(self._path, f)
                else:
                    yield f
        else:
            if not os.path.isabs(files):
                yield os.path.join(self._path, files)
            else:
                yield files

    # Commands
    # --------
    #
    # N.B. We DO NOT have methods for commands that operate on changesets;
    # those belong on the Changeset and/or Queryset object(s) instead.
    #
    # This may beg the question, what about the case where I want to inquire
    # about the *current* revision?  Well, you can use repo['.'] to get that,
    # or if that isn't clear enough, repo.current.
    #

    @staticmethod
    def init(dest='.', encoding='utf-8',
             ssh=None, remotecmd=None, insecure=False):
        """Create a new repository in the given directory.  If the given
        directory does not exist, it will be created.

        If no directory is given, the current directory is used.

        It is possible to specify an "ssh://" URL as the destination.
        See "hg help urls" for more information.

        dest      - The destination directory (.)
        encoding  - The character encoding to use (utf-8)
        ssh       - The SSH command to use, in case dest is a remote URL
        remotecmd - The path to the hg command on the remote side
        insecure  - If True, do not verify server certificate

        Returns a new Repository instance on success."""

        # Create a Client object and use it to do the init
        client = Client(dest, encoding)

        client.execute('init', dest,
                       e=ssh, remotecmd=remotecmd, insecure=insecure,
                       use_server=False)

        return Repository(dest, encoding, client)

    @staticmethod
    def clone(source, encoding='utf-8',
              dest=None, noupdate=False, updaterev=None,
              rev=None, branch=None, pull=False, uncompressed=False,
              ssh=None, remotecmd=None, insecure=False):
        """Create a copy of an existing repository in a new directory.

        If no destination directory name is specified, it defaults to the
        basename of the source.

        The location of the source is added to the new repository's ".hg/hgrc"
        file, as the default to be used for future pulls.

        Only local paths and "ssh://" URLs are supported as destinations. For
        "ssh://" destinations, no working directory or ".hg/hgrc" will be
        created on the remote side.

        source       - The source repository
        encoding     - The character encoding to use (utf-8)
        dest         - The destination directory (basename(source))
        noupdate     - Create an empty working copy
        updaterev    - The revision, tag or branch to check out
        rev          - Include only the specified changeset(s)
        branch       - Include only the specified branch(es)
        pull         - If True, use pull protocol to copy metadata
        uncompressed - Use uncompressed transfer
        ssh          - The SSH command to use
        remotecmd    - The hg command to run on the remote side
        insecure     - If True, do not verify server certificates

        Returns a new Repository object on success."""

        # Normalise the arguments a little
        if isinstance(source, Repository):
            source = source.path
            if dest is None:
                raise ValueError('must specify dest when cloning a Repository')

        updaterev = self._map_one_rev(updaterev)

        rev = self._map_revs(rev)
            
        # Create a Client object and use it to do the clone
        if dest is None:
            dest = os.path.basename(source)
        client = Client(dest, encoding)
        
        client.execute('clone', source, dest,
                       U=noupdate, u=updaterev,
                       r=rev, b=branch, pull=pull,
                       uncompressed=uncompressed,
                       e=ssh, remotecmd=remotecmd, insecure=insecure)

        return Repository(dest, encoding, client)
        
    def add(self, files=[], dry_run=False, subrepos=False,
            include=None, exclude=None):
        """Add the specified files on the next commit.

        If no files are given, add all files to the repository.

        dry_run   - do not perform actions
        subrepos - recurse into subdirectories
        include  - include names matching the given pattern(s)
        exclude  - exclude names matching the given pattern(s)
        
        Returns True if all files are successfully added."""
        files = self._map_files(files)
        
        eh = SimpleErrorHandler()
        self._client.execute('add', files,
                             n=dry_run, S=subrepos,
                             I=include, X=exclude,
                             eh=eh)

        return bool(eh)

    def addremove(self, files=[], similarity=None, dry_run=False, include=None,
                  exclude=None):
        """Add all new files and remove all missing files from the repository.

        New files are ignored if they match any of the patterns in ".hgignore". As
        with add, these changes take effect at the next commit.

        similarity - Used to detect renamed files. This option takes a
                     percentage between 0 (disabled) and 100 (files must be
                     identical) as its parameter. With a parameter greater
                     than 0, this compares every removed file with every added
                     file and records those similar enough as renames.
                     Detecting renamed files this way can be expensive. After
                     using this option, "hg status -C" can be used to check
                     which files were identified as moved or renamed. If not
                     specified, -s/--similarity defaults to 100 and only
                     renames of identical files are detected.
        dry_run    - do not perform actions
        include    - include names matching the given pattern(s)
        exclude    - exclude names matching the given pattern(s)

        Returns True if all files are successfully added/removed."""
        files = self._map_files(files)

        eh = SimpleErrorHandler()
        self._client.execute('addremove', files,
                             s=similarity, n=dry_run, I=include,
                             X=exclude,
                             eh=eh)
        
        return bool(eh)

    def annotate(self, files, rev=None, no_follow=False,
                 text=False, annotations=['changeset'],
                 include=None, exclude=None):
        """Obtain changeset information by line for each file in files.

        rev         - annotate the specified revision
        no_follow   - don't follow copies and renames
        text        - treat all files as text
        annotations - an iterable containing annotations we want
        include     - include names matching the given pattern(s)
        exclude     - exclude names matching the given pattern(s)

        Available annotations:

        user      - the author (in long form)
        changeset - the changeset (as a Changeset object)
        date      - the date (as a datetime.datetime)
        file      - the filename
        line      - the line number (as an int)

        Yields strings with the user, file, date, changeset and line properties
        for each line in each file.

        In addition, if you specify `user', you can obtain the short username
        by asking for the short_user property of the yielded line."""
        
        # Normalise the input
        files = self._map_files(files)
        rev = self._map_one_rev(rev)

        # Select annotations
        user = date = file = line = changeset = False

        for n in annotations:
            if n == 'user':
                user = True
            elif n == 'date':
                date = True
            elif n == 'file':
                file = True
            elif n == 'line':
                line = True
            elif n == 'changeset':
                changeset = True

        if not user and not date and not file and not line and not changeset:
            raise ValueError('you probably want to specify some annotations')

        # Get the output
        out = self._client.execute('annotate', files,
                                   r=rev, no_follow=no_follow,
                                   a=text, u=user, f=file, d=date,
                                   n=changeset, c=changeset,
                                   l=line,
                                   I=include, X=exclude,
                                   debug=True)

        # Build a regex to match the annotations
        regex = [r'^\s*']
        if user:
            regex.append(r'(?P<user>[A-Za-z_][A-Za-z0-9_-]*|\w[\w\s]*\s<[^>]+>)\s*')
        if changeset:
            regex.append(r'(?P<changeset>\d+ [A-Fa-f0-9]{40})\s*')
        if date:
            regex.append(r'(?P<date>(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun) '
                         r'(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Dec) '
                         r'\d+ \d+:\d+:\d+ \d+ [+-]?\d+)\s*')
        if file:
            regex.append(r'(?P<file>[^/\\]+)')
        if line:
            regex.append(r':(?P<line>\d+)')
        regex.append(r'$')
        regex = ''.join(regex)
        regex = re.compile(regex)

        # For each line, parse the annotations and stuff them onto an
        # AnnotatedString containing the line's text
        for l in out.splitlines():
            info, text = l.split(': ', 1)
            m = regex.match(info)
            
            out = AnnotatedString(text)
            if user:
                out.user = m.group('user')
            if changeset:
                rev, node = m.group('changeset').split(' ', 1)
                rev = int(rev)
                out.changeset = self._get_lazy(rev, node)
            if date:
                the_date,offset = m.group('date').rsplit(' ', 1)
                the_date = datetime.datetime.strptime(the_date,
                                                      '%a %b %d %H:%M:%S %Y')
                ofs = int(offset[:-2]) * 60
                was_negative = offset < 0
                if was_negative:
                    ofs = -ofs
                ofs += int(offset[-2:])
                if was_negative:
                    ofs = -ofs
                tzinfo = SimpleTzInfo(ofs)
                out.date = the_date.replace(tzinfo=tzinfo)
            if file:
                out.file = m.group('file')
            if line:
                out.line = int(m.group('line'))

            yield out
            
    def archive(self, dest, rev=None, no_decode=False, prefix=None, type=None,
                subrepos=False, include=None, exclude=None):
        """Create an unversioned archive of a repository revision.

        The archive type is detected automatically based on the file
        extension (or override using the `type' argument).

        Valid types are:

          'files' - a directory full of files
          'tar'   - tar archive, uncompressed
          'tbz2'  - tar archive, compressed using bzip2
          'tgz'   - tar archive, compressed using gzip
          'uzip'  - zip archive, uncompressed
          'zip'   - zip archive, compressed using deflate

        dest      - destination path
        rev       - revision to distribute
        no_decode - if True, do not pass files through decoders
        prefix    - directory prefix for files in the archive
        type      - type of distribution to create (as above)
        subrepos  - if True, recurse into subrepositories
        include   - include names matching the given pattern(s)
        exclude   - exclude names matching the given pattern(s)

        Returns True on success."""

        # Normalise the input
        rev = self._map_one_rev(rev)

        eh = SimpleErrorHandler()

        self._client.execute('archive', dest, r=rev, no_decode=no_decode,
                             p=prefix, t=type, S=subrepos, I=include,
                             X=exclude, eh=eh)

        return bool(eh)

    def backout(self, rev, merge=False, tool=None, message=None,
                logfile=None, date=None, user=None,
                include=None, exclude=None):
        """Prepare a new changeset with the effect of `rev' undone in the
        current working directory.

        If `rev' is the parent of the working directory, then this new
        changeset is committed automatically.  Otherwise, hg will merge
        the changes and the merged result will be left uncommitted.

        By default, the pending changeset will have one parent, maintaining
        a linear history.  If `merge' is True, the pending changeset will
        instead have two parents: the old parent of the working directory and
        a new child of `rev' that simply undoes `rev'.

        rev     - the revision to back-out
        merge   - if True, merge with old parent after back-out
        tool    - specify the merge tool to use
        message - the commit message
        logfile - read commit message from file
        date    - specify the commit date
        user    - specify the username of the committer
        include - include names matching the given pattern(s)
        exclude - exclude names matching the given pattern(s)

        Returns True on success."""

        if message and logfile:
            raise ValueError('cannot specify both a message and a logfile')

        rev = self._map_one_rev(rev)

        eh = SimpleErrorHandler()

        self._client.execute('backout', merge=merge, r=rev, t=tool, m=message,
                             l=logfile, d=date, u=user, I=include, X=exclude,
                             eh=eh)

        return bool(eh)

    def delete_bookmark(self, name):
        """Delete the bookmark specified by `name'."""
        eh = SimpleErrorHandler()
        
        self._client.execute('bookmark', name, d=True, eh=eh)

        return bool(eh)

    def rename_bookmark(self, old_name, new_name):
        """Rename the bookmark `old_name', giving it the name `new_name'."""
        eh = SimpleErrorHandler()

        self._client.execute('bookmark', new_name, m=old_name, eh=eh)

        return bool(eh)

    def deactivate_bookmark(self, name=None):
        """Deactivate the specified bookmark, or if none is specified, the
        currently active bookmark (if any)."""
        eh = SimpleErrorHandler()

        self._client.execute('bookmark', i=name, eh=eh)

        return bool(eh)

    def bookmark(self, name, rev=None, force=False):
        """Set a new bookmark `name' at the specified revision, or if none
        is specified, on the working directory's parent revision.

        If a bookmark with the name `name' already exists, this method will
        fail unless you set `force' to True.  This is to prevent accidental
        overwrites of existing bookmarks.

        If no revision is specified, this method will activate the bookmark.

        To delete, rename or deactivate a bookmark, see delete_bookmark(),
        rename_bookmark() and deactivate_bookmark() respectively.  To
        activate a bookmark, call update(), passing the bookmark's name
        or a bookmark Queryset obtained using
        
          repo.changesets.bookmark('<name>')

        Note that merely updating to a revision that has a bookmark DOES NOT
        cause it to activate.  You MUST use one of these two forms."""
        eh = SimpleErrorHandler()

        rev = self._map_one_rev(rev)
        self._client.execute('bookmark', name, rev=rev, force=force, eh=eh)

        return bool(eh)
        
    def bookmarks(self):
        """Return a tuple (active, bookmarks) containing:

           active    - the currently active bookmark, or None
           bookmarks - a dictionary mapping bookmark names to Changesets"""
        out = self._client.execute('bookmarks', debug=True)
        bookmarks = {}
        active = None
        
        if out.strip() != 'no bookmarks set':
            for line in out.splitlines():
                name, line = line[3:].split(' ', 1)
                rev, node = line.split(':')
                bookmarks[name] = self._get_lazy(int(rev), node)
                if line[:3].strip() == '*':
                    active = name

        return bookmarks

    def branch(self, name=None, clean=None, force=None):
        """When name is not given, return the current branch name.  Otherwise,
        set the working directory branch name (the branch will not exist in
        the repository until the next commit).  Standard practice recommends
        that primary development take place on the 'default' branch.

        name  - new branch name
        clean - reset branch name to parent branch name
        force - set branch name even if it shadows and existing branch"""

        if name and clean:
            raise ValueError('Cannot use both name and clean')

        self._client.execute('branch', name, f=force, C=clean)

        if name:
            return name
        elif not clean:
            return out.strip()
        else:
            return out[len('reset working directory to branch '):]

    def branches(self, active=False, closed=False):
        """Return a dictionary mapping branch names to Changesets.

        active - return only branches that have unmerged heads
        closed - return normal and closed branches"""
        
        out = self._client.execute('branches', a=active, c=closed,
                                   debug=True)
        branches = {}

        for line in out.strip().splitlines():
            namerev, node = line.rsplit(':', 1)
            name, rev = namerev.rsplit(' ', 1)
            name = name.strip()
            node = node.split()[0] # To get rid of ' (inactive)'
            branches[name] = self._get_lazy(int(rev), node)

        return branches

    def bundle(self, filename, dest=None, force=False, branch=None,
               base=None, rev=None, type='bzip2', ssh=None, remotecmd=None,
               insecure=False):
        """Create a compressed changegroup file collecting changesets not known
        to be in another repository.

        If you omit the destination repository, then hg assumes the destination
        will have all the nodes you specify using the `base' argument.

        If you omit the `dest', `base' and `rev' arguments, the bundle will
        contain all of the changesets in the repository.

        The bundle file, once made, can be transferred using conventional means
        and applied to another repository using the unbundle() or pull()
        methods, or equivalent hg commands.  This is useful when direct push
        and pull are not available or when exporting an entire repository
        is undesirable.

        Applying bundles preserves all changeset contents including permissions,
        copy/rename information, and revision history.

        Valid compression types are:

          'none'  - no compression
          'bzip2' - compress with bzip2
          'gzip'  - compress with gzip

        filename  - the name of the file to create
        dest      - the destination repository (can be a Repository object)
        force     - run even if the destination is unrelated
        rev       - one or more changesets intended to be bundled
        branch    - one or more branches you would like to bundle
        base      - one or more changesets we can assume are available at
                    the destination
        type      - the type of compression (default 'bzip2')
        ssh       - the SSH command to use
        remotecmd - the remote hg command to use
        insecure  - if True, do not verify the server certificate

        Returns True on success."""

        # Normalise the input
        if isinstance(dest, Repository):
            dest = dest.path

        rev = self._map_revs(rev)
        branch = self._map_branches(branch)
        base = self._map_revs(base)

        all = not rev and not branch and not base

        eh = SimpleErrorHandler()
        self._client.execute('bundle', filename, dest, f=force,
                             r=rev, b=branch, base=base, a=all,
                             t=type, e=ssh, remotecmd=remotecmd,
                             insecure=insecure, eh=eh)

        return bool(eh)

    def cat(self, files, rev=None, output=None, decode=False,
            include=None, exclude=None):
        """Retrieve the data for the specified files as they were at the
        given revision, or, if no revision is specified, the parent of the
        working directory, or tip if no revision is checked out.

        You can choose to output the data to a file, in which case its
        name will be generated from a format string specified in the `output'
        parameter.  Format specifiers are as follows:

          %%  - literal '%' character
          %H  - changeset hash (40 hex digits)
          %R  - changeset revision number
          %b  - basename of the exporting repository
          %h  - short-form changeset hash (12 hex digits)
          %m  - first line of the commit message
          %n  - zero-padded sequence number, starting at 1
          %r  - zero-padded changeset revision number
          %s  - basename of file being retrieved
          %d  - dirname of the file being retrieved, or '.' if in repository root
          %p  - root-relative path name of file being retrieved

        files   - one or more filenames to retrieve
        rev     - the revision to retrieve data for
        output  - if specified, a format string specifying the output filename(s)
        decode  - if True, apply any matching decode filter
        include - include names matching the given pattern(s)
        exclude - exclude names matching the given pattern(s)

        If `output' was specified, returns True on success; otherwise, returns
        the data.

        Note: for some purposes, you may prefer to use the open() method,
              either on the repository itself, or on the Changeset object.
              Doing so has the advantage of not tying up the server
              connection, and is more idiomatic Python.
        """
        rev = self._map_one_rev(rev)
        
        eh = SimpleErrorHandler()
        out = self._client.execute('cat', files, r=rev, o=output,
                                   decode=decode, I=include, X=exclude,
                                   eh=eh)

        if output:
            return bool(eh)
        else:
            return out

    def commit(self, message=None, logfile=None, addremove=False,
               close_branch=False, amend=False, date=None,
               user=None, include=None, exclude=None, subrepos=False,
               files=[]):
        """Commit changes to the repository.

        If a list of files is omitted, all changes reported by "hg status" will
        be committed.

        If you are committing the result of a merge, do not provide any
        filenames or include/exclude filters.
        
        Unlike the command line, you MUST provide either a commit message or
        a logfile.

        The amend flag can be used to amend the parent of the working directory
        with a new commit that contains the changes in the parent in addition
        to those currently reported by "hg status", if any.  The old commit
        is stored in a backup bundle in ".hg/strip-backup" (see "hg help bundle"
        and "hg help unbundle" on how to restore it).

        Message, user and date are taken from the amended commit unless
        specified.

        It is not possible to amend public changesets (see "hg help phases") or
        changesets that have children.

        message      - the commit message
        logfile      - read commit message from file
        addremove    - mark new/missing files as added
        close_branch - mark a branch as closed, hiding it from the branch list
        amend        - amend the parent of the working dir
        date         - specify the commit date
        user         - specify the username of the committer
        subrepos     - recurse into subrepositories
        include      - include names matching the given patterns
        exclude      - exclude names matching the given patterns
        files        - the files to commit (if empty, commit all changes)

        Returns a new Changeset on success."""

        if message is None and logfile is None:
            raise ValueError('you must provide a message or a logfile')
        elif message and logfile:
            raise ValueError('cannot specify both a message and a logfile')

        files = self._map_files(files)

        out = self._client.execute('commit', files,
                                   debug=True, m=message, A=addremove,
                                   close_branch=close_branch, amend=amend,
                                   d=date, u=user, l=logfile,
                                   I=include, X=exclude, S=subrepos)

        rev, node = out.splitlines()[-1].rsplit(':')
        rev = int(rev.split()[-1])

        return self._get_lazy(rev, node)

    def copy(self, source, dest, dry_run=False, after=False, force=False,
             include=None, exclude=None):
        """Mark files as copied for the next commit.

        By default, this method copies the contents of files as they exist
        in the working directory.  If you don't want to actually perform a
        copy, set after to True.

        If dest is a directory, copies are put into that directory.  If dest
        is a file, source must be a single file.

        Returns True on success."""
        source = self._map_files(source)
        dest = self._map_files(dest)
        
        eh = SimpleErrorHandler()
        self._client.execute('copy', source, dest, n=dry_run, A=after,
                             f=force, I=include, X=exclude, eh=eh)

        return bool(eh)

    def changes(self, files=[], rev=None, change=None, text=False,
                reverse=False, ignore_all_space=False, ignore_space_change=False,
                ignore_blank_lines=False, context=None, subrepos=False,
                include=None, exclude=None):
        """Generate Change objects between revisions for the specified files.

        files         -  the files to diff (if None, diff the entire repository)
        rev           -  0, 1 or 2 revisions; if none, compare the working
                         directory to its parent; if one, compare with
                         working directory to that revision; if two, compare
                         those revisions
        change        -  generate a diff for this changeset
        text          -  treat all files as text
        reverse       -  reverse the diff
        
        ignore_all_space    - ignore whitespace when comparing lines
        ignore_space_change - ignore changes in the amount of whitespace
        ignore_blank_lines  - ignore changes whose lines are all blank

        context       -  specify the amount of context (number of lines)
        include       -  include names matching the given patterns
        exclude       -  exclude names matching the given patterns
        subrepos      -  recurse into subrepositories

        Returns a generator that yields Change objects."""   
        return diffparser.parse(self.diff(files=files, rev=rev, change=change,
                                          text=text, git=True, reverse=reverse,
                                          ignore_all_space=ignore_all_space,
                                          ignore_space_change=ignore_space_change,
                                          ignore_blank_lines=ignore_blank_lines,
                                          unified=context, subrepos=subrepos,
                                          include=include, exclude=exclude))

    def diff(self, files=[], rev=None, change=None, text=False,
             git=False, nodates=False, show_function=False, reverse=False,
             ignore_all_space=False, ignore_space_change=False,
             ignore_blank_lines=False, unified=None,
             stat=False, subrepos=False, include=None, exclude=None):
        """Generate a diff between revisions for the specified files.

        files         -  the files to diff (if None, diff the entire repository)
        rev           -  0, 1 or 2 revisions; if none, compare the working
                         directory to its parent; if one, compare with
                         working directory to that revision; if two, compare
                         those revisions
        change        -  generate a diff for this changeset
        text          -  treat all files as text
        git           -  use git extended diff format
        nodates       -  omit dates from diff headers
        show_function -  show which function each change is in
        reverse       -  reverse the diff
        
        ignore_all_space    - ignore whitespace when comparing lines
        ignore_space_change - ignore changes in the amount of whitespace
        ignore_blank_lines  - ignore changes whose lines are all blank

        unified       -  specify the amount of context (number of lines)
        stat          -  generate a diffstat-style summary
        include       -  include names matching the given patterns
        exclude       -  exclude names matching the given patterns
        subrepos      -  recurse into subrepositories

        Returns a string containing the generated diff."""
        if change and rev:
            raise ValueError('cannot specify both change and rev')

        files = self._map_files(files)
        rev = self._map_revs(rev)
        change = self._map_one_rev(change)

        out = self._client.execute('diff', files, r=rev, c=change,
                                   a=text, g=git, nodates=nodates,
                                   p=show_function, reverse=reverse,
                                   w=ignore_all_space, b=ignore_space_change,
                                   B=ignore_blank_lines, U=unified, stat=stat,
                                   S=subrepos, I=include, X=exclude,
                                   binary=True)

        return out

    def export(self, rev, output=None, switch_parent=False, text=False,
               git=False, nodates=False):
        """Export the header and diffs for one or more changesets.

        You can choose to output the data to a file, in which case
        its name will be generated from a format string specified in the
        `output' parameter.  Format specifiers are as follows:

          %%  - literal '%' character
          %H  - changeset hash (40 hex digits)
          %R  - changeset revision number
          %b  - basename of the exporting repository
          %h  - short-form changeset hash (12 hex digits)
          %m  - first line of the commit message
          %n  - zero-padded sequence number, starting at 1
          %r  - zero-padded changeset revision number

        rev    - revision(s) to export as diffs
        output - if specified, a format string specifying the output filename(s)
        text   - if True, treat all files as text
        git    - use git extended diff format
        
        switch_parent - if True, diff against the second parent

        If `output' was specified, returns True on success; otherwise, returns
        the data."""

        rev = self._map_revs(rev)

        eh = SimpleErrorHandler()
        out = self._client.execute('export', r=rev, o=output, a=text, g=git,
                                   switch_parent=switch_parent, nodates=nodates,
                                   eh=eh)

        if output:
            return bool(eh)
        else:
            return out
        
    def forget(self, files, include=None, exclude=None):
        """Forget the specified files on the next commit.

        This only removes files from the current branch, not from the entire
        project history, and it does not delete them from the working directory.

        You can use the add() method to undo a forget().

        Returns True on success."""
        files = self._map_files(files)

        eh = SimpleErrorHandler()
        self._client.execute('forget', files, I=include, X=exclude, eh=eh)

        return bool(eh)

    def graft(self, rev, resume=False, log=False, currentdate=False,
              currentuser=False, date=None, user=None, tool=None,
              dry_run=False):
        """Copy changes from other branches onto the current branch.

        This command uses Mercurial's merge logic to copy individual changes
        from other branches without merging branches in the history
        graph. This is sometimes known as 'backporting' or
        'cherry-picking'. By default, graft will copy user, date, and
        description from the source changesets.

        Changesets that are ancestors of the current revision, that have
        already been grafted, or that are merges will be skipped.

        If `log' is True, log messages will have a comment appended of the
        form:

          (grafted from <node>)

        If a graft merge results in conflicts, the graft process is
        interrupted so that the current merge can be manually resolved.
        Once all conflicts are addressed, the graft can be continued by
        setting `resume' to True.

        Returns True on successful completion."""
        if currentdate and date:
            raise ValueError('cannot specify both currentdate and date')
        if currentuser and user:
            raise ValueError('cannot specify both currentuser and user')
        
        rev = self._map_revs(rev)

        eh = SimpleErrorHandler()
        out = self._client.execute('graft', r=rev, c=resume, log=log,
                                   D=currentdate, U=currentuser,
                                   d=date, u=user,
                                   t=tool, n=dry_run, eh=eh)
        return bool(eh)

    def grep(self, pattern, files=[], rev=None, no_follow=False,
             text=False, annotations=['rev', 'file', 'line'],
             ignore_case=False, match_text=True,
             include=None, exclude=None):
        """Search revisions of files for the specified pattern,
        which should be a Python-style regular expression.

        pattern     - the Python-style regexp to search for
        files       - files to search (if None, search entire repository)
        rev         - only search within the specified revision(s)
        no_follow   - don't follow copies and renames
        ignore_case - ignore case when matching
        match_text  - if False, don't include the matched text (default True)

        Available annotations:

          user        - the author (in long form)
          rev         - the revision number
          date        - the date (as a datetime.datetime)
          file        - the filename
          line        - line number of match
        * type        - the type of change (+ or -)

        * - if specified, grep() scans all revisions, rather than just
            finding the first match.

        Yields objects with the properties listed above; if `match_text'
        is True, the objects are strings containing the match text."""

        # Normalise the input
        files = self._map_files(files)
        rev = self._map_revs(rev)

        # Select annotations
        user = date = file = line = changeset = type_ = False

        for n in annotations:
            if n == 'user':
                user = True
            elif n == 'date':
                date = True
            elif n == 'file':
                file = True
            elif n == 'line':
                line = True
            elif n == 'changeset':
                changeset = True
            elif n == 'type':
                type_ = True

        if not user and not date and not file and not line and not changeset \
           and not type_ and not match_text:
            raise ValueError('you probably want either some annotations or the match text')

        out = self._client.execute('grep', pattern, files,
                                   r=rev, f=not no_follow,
                                   a=text, i=ignore_case,
                                   l=not match_text,
                                   n=line, u=user, d=date,
                                   I=include, X=exclude,
                                   v=True,
                                   print0=True)

        fields = 2 # filename rev
        if user:
            fields += 1
        if date:
            fields += 1
        if line:
            fields += 1
        if type_:
            fields += 1
        if match_text:
            fields += 1
            
        for l in every(out.split('\0'), fields):
            if match_text:
                out = GrepString(l[-1])
            else:
                out = GrepResult()

            out.file = l.pop(0)
            out.rev = int(l.pop(0))

            if line:
                out.line = int(l.pop(0))
            if type_:
                out.type = l.pop(0)
            if user:
                out.user = l.pop(0)
            if date:
                the_date,offset = l.pop(0).rsplit(' ', 1)
                the_date = datetime.datetime.strptime(the_date,
                                                      '%a %b %d %H:%M:%S %Y')
                ofs = int(offset[:-2]) * 60
                was_negative = offset < 0
                if was_negative:
                    ofs = -ofs
                ofs += int(offset[-2:])
                if was_negative:
                    ofs = -ofs
                tzinfo = SimpleTzInfo(ofs)
                out.date = the_date.replace(tzinfo=tzinfo)

            yield out

    def incoming(self, source=None, force=False, newest_first=False, bundle=None,
                 rev=None, bookmarks=False, branch=None, mode='changesets',
                 git=False, limit=None, no_merges=False, stat=False,
                 ssh=None, remotecmd=None, insecure=False, subrepos=False):
        """Finds new changesets found in the specified source.  If no source
        is specified, uses the default pull location.

        source    - the repository to examine (a path, URL or Repository object)
        rev       - one or more changesets to include
        branch    - one or more branches to include
        force     - if True, run even if the repository seems unrelated
        bookmarks - compare bookmarks
        limit     - the maximum number of changes to fetch
        no_merges - if True, ignore merges
        ssh       - specify the ssh command to use
        remotecmd - specify the remote hg command to use
        insecure  - if True, do not verify server certificates
        subrepos  - recurse into subrepositories
        mode      - must be one of 'changesets', 'patch' or 'diffstat'
                    (default is 'changesets')
        git       - if mode is 'patch' and this is True, use git extended diff
                    format
                    
        To simultaneously download a changeset bundle, set the bundle option,
        in which case the following options are relevant:
        
        bundle    - the name of a file into which to write a changeset bundle

        In 'changesets' mode, returns a list of Changeset objects; in 'patch'
        mode, returns a patch; in 'diffstat' mode, returns a diffstat-style
        summary of the changes found.

        Note that the Changeset objects returned by 'changesets' mode will
        ONLY be fully functional if you passed a path or Repository object;
        if you give a remote URL, their repository will be set to a
        RemoteRepository, and so you will get RemoteRepositoryError exceptions
        if you try to use some of their methods."""
        return self._in_out('incoming', repo_or_path=source,
                            force=force, newest_first=newest_first,
                            bundle=bundle, rev=rev, bookmarks=bookmarks,
                            branch=branch, mode=mode, git=git, limit=limit,
                            no_merges=no_merges, stat=stat,
                            ssh=ssh, remotecmd=remotecmd, insecure=insecure,
                            subrepos=subrepos)

    def outgoing(self, dest=None, force=False, newest_first=False,
                 rev=None, bookmarks=False, branch=None, mode='changesets',
                 git=False, limit=None, no_merges=False, stat=False,
                 ssh=None, remotecmd=None, insecure=False, subrepos=False):
        """Finds changesets not in the specified destination.  If no
        destination is specified, uses the default push location.

        dest      - the repository to examine (a path, URL or Repository object)
        rev       - one or more changesets to include
        branch    - one or more branches to include
        force     - if True, run even if the repository seems unrelated
        bookmarks - compare bookmarks
        limit     - the maximum number of changes to fetch
        no_merges - if True, ignore merges
        ssh       - specify the ssh command to use
        remotecmd - specify the remote hg command to use
        insecure  - if True, do not verify server certificates
        subrepos  - recurse into subrepositories
        mode      - must be one of 'changesets', 'patch' or 'diffstat'
                    (default is 'changesets')
        git       - if mode is 'patch' and this is True, use git extended diff
                    format
                    
        In 'changesets' mode, returns a list of Changeset objects; in 'patch'
        mode, returns a patch; in 'diffstat' mode, returns a diffstat-style
        summary of the changes found.

        Note that the Changeset objects returned by 'changesets' mode are
        fully functional and are attached to a Repository instance (either
        the one passed as `dest', or one created from it)."""
        return self._in_out('outgoing', repo_or_path=dest,
                            force=force, newest_first=newest_first,
                            rev=rev, bookmarks=bookmarks,
                            branch=branch, mode=mode, git=git, limit=limit,
                            no_merges=no_merges, stat=stat,
                            ssh=ssh, remotecmd=remotecmd, insecure=insecure,
                            subrepos=subrepos)
    
    def _in_out(self, direction, repo_or_path=None,
                force=False, newest_first=False, bundle=None,
                rev=None, bookmarks=False, branch=None, mode='changesets',
                git=False, limit=None, no_merges=False, stat=False,
                ssh=None, remotecmd=None, insecure=False, subrepos=False):
        """The actual implementation behind the incoming/outgoing methods."""
        
        if isinstance(repo_or_path, Repository):
            repo = repo_or_path
            path = repo_or_path.path
        elif mode == 'changesets':
            if repo_or_path is None:
                if direction == 'incoming':
                    default_path = self.paths('default')
                    if not default_path:
                        raise ValueError('repository has no default pull location, so you must specify the source repository')
                else:
                    default_path = self.paths('default_push')
                    if not default_path:
                        raise ValueError('repository has no default push location, so you must specify the destination repository')

                parsed = urlparse.urlparse(default_path)
                if parsed.scheme and parsed.scheme != 'file':
                    repo = RemoteRepository(default_path)
                    path = default_path
                else:
                    repo = Repository(default_path)
                    path = repo.path
            else:
                repo = Repository(repo_or_path)
                path = repo_or_path
        else:
            path = repo_or_path
            
        if mode not in ['changesets', 'patch', 'diffstat']:
            raise ValueError('mode must be one of "changesets", "patch", "diffstat"')

        patch = mode == 'patch'
        stat = mode == 'diffstat'

        if mode == 'changesets':
            template = Repository._TEMPLATE
        else:
            template = None
 
        rev = self._map_revs(rev)
        branch = self._map_branches(branch)

        out = self._client.execute(direction, path,
                                   force=force,
                                   n=newest_first,
                                   bundle=bundle,
                                   r=rev, B=bookmarks, b=branch,
                                   p=patch, g=git, l=limit,
                                   M=no_merges, stat=stat,
                                   template=template,
                                   e=ssh, remotecmd=remotecmd,
                                   S=subrepos)

        if mode == 'changesets':
            result = []
            out = out.split('\n', 2)[2]
            for info in every(out.split('\0'), 12):
                cset = repo._cset_from_info(info)
                result.append(cset)
            return result

        return out

    def ls(self, patterns=[], rev=None, all=False, sort=['name'],
           fields=['mode', 'user', 'size', 'rev', 'date', 'name'],
           subrepos=False, recursive=False):
        """List matching files in the repository.

        For each name given that is a file of a type other than a directory,
        displays its name as well as any requested, associated information. For
        each name given that is a directory, displays the names of files within
        that directory, as well as any requested, associated information.

        If no names are given, the contents of the working directory are
        displayed.

        The "sort" option can be set to a list of any of the following strings,
        optional prefixed with a '+' or a '-' to indicate sort direction:

          name          the name of the file
          rev           the last revision at which the file was changed
          date          the date of the last revision at which the file was
                        changed
          author        the name of the user who last changed the file
          user          the short name of the user who last changed the file
          size          the size of the file
          subrepo       the name of the subrepository

        The "fields" option controls the format of the returned information;
        it should be a list containing one or more of the following:

          name          the name of the file (a string, relative to repo root)
          mode          the UNIX mode of the file (an integer)
          size          the size of the file, in bytes (an integer)
          kind          an '@', '*', or '/' character depending on the type of the
                        file
          subrepo       if this file is in a subrepository, the path within the
                        outer repository (a string)
          rev           the Changeset at which the file last changed
          date          the date of the last revision at which the file was
                        changed (a datetime.datetime)
          author        the name of the user who last changed the file
          user          the short name of the user who last changed the file
          branch        the branch of the last revision at which the file was
                        changed
          desc          the description of that revision

        For each matching file, this method will generate a tuple containing
        one item for each item in the "fields" argument."""
        rev = self._map_one_rev(rev)

        sort = ','.join(sort)

        fieldmap = { 'name': '{name}',
                     'mode': '{mode}',
                     'size': '{size}',
                     'kind': '{kind}',
                     'subrepo': '{subrepo}',
                     'rev': '{subrepo}:{rev}:{node}',
                     'date': '{date}',
                     'author': '{author}',
                     'user': '{author|user}',
                     'branch': '{branch}',
                     'desc': '{desc}' }

        try:
            template = [fieldmap[f] for f in fields]
        except IndexError:
            raise BadFieldError('bad field in fields specification')
        
        template = r'\0'.join(template + [''])

        out = self._client.execute('list', patterns,
                                   r=rev, all=all, sort=sort,
                                   template=template,
                                   subrepos=subrepos,
                                   recursive=recursive,
                                   binary=True)

        subreps = {}
        
        for t in every(out.split('\0'), len(fields)):
            result = []
            for item,field in itertools.izip(t, fields):
                if field in ('mode', 'size'):
                    result.append(int(item))
                elif field == 'rev':
                    s,r,n = item.split(':')
                    r = int(r)
                    if r == -1:
                        result.append(None)
                    elif s:
                        sr = subreps.get(s, None)
                        if sr is None:
                            sr = Repository(os.path.join(self._path, s))
                            subreps[s] = sr
                        result.append(sr._get_lazy(r, n))
                    else:
                        result.append(self._get_lazy(r, n))
                elif field == 'date':
                    utc = float(item.split('.', 1)[0])
                    result.append(datetime_from_timestamp(utc))
                elif field != 'name':
                    result.append(item.decode(self._client.encoding))
                else:
                    # name is, necessarily, binary
                    result.append(item)
            yield tuple(result)

    def patch(self, patches=[], strip=1, force=False, no_commit=False,
              bypass=False, exact=False, import_branch=False,
              message=None, logfile=None, date=None, user=None,
              similarity=None, patch=None):
        """Import an ordered set of patches.

        Specify ONE of the following:

        patches - the filenames or URLs of the patches you wish to apply
        patch   - a string or file-like object containing a patch

        and then

        strip         - the number of directories to strip (like the patch -p
                        option).
        force         - if True, patch even if there are outstanding changes in
                        the working directory
        no_commit     - if True, update the working directory but do not commit
        bypass        - if True, apple patches without touching the working
                        directory
        exact         - apply the patch to the nodes from which it was generated
                        (the patch must be in git format for this to work)
        import_branch - use any branch information in the patch
        message       - use the specified text as a commit message
        logfile       - read the commit message from the specified file
        date          - record the specified date as the commit date
        user          - record the specified user as the committer
        similarity    - set the similarity threshold for guessing renamed
                        files (between 0 and 100)

        Returns True on success."""
        if patches and patch:
            raise ValueError('you cannot specify both patches and patch')
        elif not patches and not patch:
            raise ValueError('you must specify either patches or patch')
        if message and logfile:
            raise ValueError('you cannot specify both message and logfile')
        elif not message and not logfile:
            raise ValueError('you must specify either message or logfile')
        if no_commit and bypass:
            raise ValueError('it makes no sense to specify no_commit and bypass')

        if patches:
            patches=self._map_files(patches)
            input=None
        else:
            patches='-'
            if isinstance(patch, basestring):
                input=lambda size: patch
            else:
                def read_patch(size):
                    if size == 0:
                        return patch.read()
                    else:
                        return patch.read(size)
                input=read_patch

        eh = SimpleErrorHandler()
        self._client.execute('import', patches, p=strip, f=force,
                             no_commit=no_commit, bypass=bypass, exact=exact,
                             import_branch=import_branch, m=message,
                             l=logfile, d=date, u=user, s=similarity,
                             eh=eh)

        return bool(eh)

    def config(self, *args):
        """Retrieve configuration information.

        With no arguments, return all configuration as a dictionary.
        
        With one argument of the form <section>.<name>, retrieve just that
        item and return it.

        With one or more arguments of the form <section>, retrieve just
        those sections as a dictionary."""
        if len(args) == 1 and args[0].find('.') >= 0:
            return self._client.execute('showconfig', args[0]).strip()
        
        out = self._client.execute('showconfig', args)
        result = {}

        for line in out.splitlines():
            ks, v = line.split('=', 1)
            ks = ks.split('.')
            d = result
            for k in ks[:-1]:
                d = d.setdefault(k, {})
            d[ks[-1]] = v.strip()

        return result

    def paths(self, name=None):
        """Return a dictionary mapping symbolic names to repository paths.

        If `name' is specified, returns just the specified path, or None if
        it is not found."""
        eh = SimpleErrorHandler()
        
        out = self._client.execute('paths', name, eh=eh)

        if name is not None:
            if not bool(eh):
                return None
            return out.strip()
        
        result = {}
        for line in out.splitlines():
            name, path = out.split(' = ', 1)
            result[name] = path.strip()
            
        return result
    
    def pull(self, source=None, update=False, force=False, rev=None,
             bookmark=None, branch=None, ssh=None, remotecmd=None,
             insecure=False, rebase=False, tool=None):
        """Pull changes from a remote repository to a local one.

        This finds all changes from the repository at the specified path or
        URL and adds them to this Repository.  By default, this does not
        update the copy of the project in the working directory.

        If source is None, the default path will be used.  In addition to
        a path or a URL, source may be a Repository object.

        source    - The path or URL of the remote repository, or a Repository
                    object referencing it.
        update    - If True, update the working copy (default False)
        force     - Run even when the remote repository is unrelated
        rev       - One or more remote changesets intended to be added
                    (this can be an iterable that returns Changesets)
        bookmark  - One or more bookmarks to pull
        branch    - One or more branches to pull
        ssh       - The SSH command to use
        remotecmd - The remote hg command to run
        insecure  - If True, do not verify server certificates
        rebase    - If True, rebase the working copy (default False)
        tool      - Specify the merge tool for rebase
        
        If `update' is False, returns True on success; otherwise it returns
        a tuple containing the number of files in each state:

           (updated, merged, removed, unresolved)"""

        # Normalise the input
        if isinstance(source, Repository):
            source = source.path

        rev = self._map_revs(rev)
        
        eh = SimpleErrorHandler()
        out = self._client.execute('pull', source, r=rev, u=update, f=force,
                                   B=bookmark, b=branch, e=ssh,
                                   remotecmd=remotecmd,
                                   insecure=insecure, rebase=rebase, t=tool,
                                   eh=eh)

        if update:
            return tuple([int(x) for x in self._UPDATE_RESULT_RE.findall(out)])
        
        return bool(eh)

    def push(self, dest=None, force=False, rev=None, bookmark=None,
             branch=None, new_branch=False, ssh=None, remotecmd=None,
             insecure=False):
        """Push changesets from this repository to the specified destination,
        or to the default destination if none is specified.

        This operation is symmetrical to pull: it is identical to a pull in
        the destination repository from the current one.

        By default, push will not allow the creation of new heads at the
        destination, since multiple heads would make it unclear which head to
        use.  In this situation, it is recommended to pull and merge before
        pushing.

        dest      - The path or URL of the remote repository, or a Repository
                    object referencing it.
        force     - Run even if the push would normally be rejected (e.g. for
                    creating a new head)
        rev       - One or more changesets intended to be pushed
                    (can be an iterable that returns Changesets)
        bookmark  - One or more bookmarks to push
        branch    - One or more branches to push
        ssh       - The SSH command to use
        remotecmd - The remote hg command to run
        insecure  - If True, do not verify server certificates

        Returns True on success."""

        # Normalise the input
        if isinstance(dest, Repository):
            dest = dest.path

        rev = self._map_revs(rev)

        eh = SimpleErrorHandler()
        self._client.execute('push', dest, f=force, r=rev, B=bookmark,
                             b=branch, new_branch=new_branch,
                             e=ssh, remotecmd=remotecmd, insecure=insecure,
                             eh=eh)

        return bool(eh)

    def locate(self, patterns, rev=None, fullpath=False, include=None,
               exclude=None):
        """Locate files matching specific patterns.

        patterns - one or more patterns to look for
        rev      - specifies the revision to search
        fullpath - if True, generate absolute paths
        include  - include names matching the given pattern(s)
        exclude  - exclude names matching the given pattern(s)

        Returns a list of the files located."""
        rev = self._map_rev(rev)

        out = self._client.execute('locate', patterns, r=rev, print0=True,
                                   f=fullpath, I=include, X=exclude)

        return out.split('\0')

    def merge(self, rev=None, force=False, tool=None, interact='abort'):
        """Merge working directory with the specified revision.  If no other
        revision is specified and the current branch contains exactly two
        heads, merge with the other head; otherwise, a revision must be
        specified.

        force    - force a merge with outstanding changes
        rev      - the revision to merge
        tool     - the merge tool used for file merges
        interact - either 'abort' (the default),
                   False (which means answering 'y' automatically),
                   or a function that accepts output and returns one
                   of the expected choices (a single character).

        Returns a tuple containing the number of files in each state:

           (updated, merged, removed, unresolved)"""
        rev = self._map_revs(rev)

        y=False
        if interact == 'abort':
            prompt = lambda size, output: ''
        elif interact == False:
            y=True
        else:
            prompt = lambda size, output: interact(output) + '\n'

        eh = SimpleErrorHandler()
        out = self._client.execute('merge', r=rev, f=force, t=tool,
                                   eh=eh, prompt=prompt)

        return tuple([int(x) for x in self._UPDATE_RESULT_RE.findall(out)])

    def move(self, source, dest, dry_run=False, after=False, force=False,
             include=None, exclude=None):
        """Mark files as copied and to-be-removed for the next commit.

        By default, this method copies the contents of files as they exist
        in the working directory.  If you don't want to actually perform a
        copy, set after to True.

        If dest is a directory, copies are put into that directory.  If dest
        is a file, source must be a single file.

        Returns True on success."""
        eh = SimpleErrorHandler()
        self._client.execute('move', source, dest, n=dry_run, A=after,
                             f=force, I=include, X=exclude, eh=eh)

        return bool(eh)

    rename = move

    def remove(self, files, after=False, force=False,
               include=None, exclude=None):
        """Remove the specified files on the next commit.

        By default, this method deletes the files from the working
        directory.  If you don't want that, set after to True.

        Returns True on success."""
        files = self._map_files(files)

        eh = SimpleErrorHandler()
        self._client.execute('remove', files,
                             A=after, f=force, I=include, X=exclude, eh=eh)

        return bool(eh)

    def resolve(self, files=[], all=False, mode='list',
                tool=None, include=None, exclude=None):
        """Redo merges or set/view the merge status of files.
        
        Merges with unresolved conflicts are often the result of
        non-interactive merging using the "internal:merge" configuration
        setting, or a command- line merge tool like "diff3". The resolve
        command is used to manage the files involved in a merge, after "hg
        merge" has been run, and before "hg commit" is run (i.e. the working
        directory must have two parents). See "hg help merge-tools" for
        information on configuring merge tools.

        files   - file(s) to operate on
        mode    - one of 'list', 'mark', 'unmark' or 'remerge'
        all     - if True, select all unresolved files
        tool    - specify merge tool, for remerge
        include - include names matching the given patterns
        exclude - exclude names matching the given patterns

        If mode is 'list', returns a list of (status, file) tuples, where
        status is 'U' (unresolved) or 'R' (resolved).  Otherwise, returns
        True on success."""
        if files and all:
            raise ValueError('you cannot set all to True and specify files')
        if mode not in ['list', 'mark', 'unmark', 'remerge']:
            raise ValueError('bad mode - must be list, mark, unmark or remerge')
        
        files = self._map_files(files)

        eh = SimpleErrorHandler()
        out = self._client.execute('resolve', files, a=all,
                                   l=(mode == 'list'),
                                   m=(mode == 'mark'),
                                   u=(mode == 'unmark'),
                                   t=tool,
                                   I=include,
                                   X=exclude)

        if mode == 'list':
            results = []
            for line in out.splitlines():
                status, filename = line.split(' ', 1)
                results.append ((status, filename.strip()))
            return results
        
        return bool(eh)
    
    def revert(self, files=[], all=False, date=None, rev=None,
               no_backup=False, include=None, exclude=None, dry_run=False):
        """Restore files to their checkout state.

        With no revision specified, revert the specified files or directories
        to the contents they had in the parent of the working directory. This
        restores the contents of files to an unmodified state and unschedules
        adds, removes, copies, and renames. If the working directory has two
        parents, you must explicitly specify a revision.

        Using the `rev' or `date' options, revert the given files or
        directories to their states as of a specific revision. Because revert does
        not change the working directory parents, this will cause these files to
        appear modified. This can be helpful to "back out" some or all of an
        earlier change. See Changeset.backout() for a related method.

        Modified files are saved with a .orig suffix before reverting. To disable
        these backups, set no_backup to True.

        Returns True on success."""
        if files is None and not all:
            raise ValueError('you must either specify files to revert, or set all to True')
        elif files and all:
            raise ValueError('you cannot specify both files to revert *and* set all to True')

        files = self._map_files(files)
        rev = self._map_one_rev(rev)

        eh = SimpleErrorHandler()
        self._client.execute('revert', files, a=all, d=date, r=rev,
                             C=no_backup, I=include, X=exclude, n=dry_run,
                             eh=eh)

        return bool(eh)

    def status(self, files=[], all=False, modified=False, added=False,
               removed=False, deleted=False, clean=False, unknown=False,
               ignored=False, copies=False, rev=None, change=None,
               include=None, exclude=None, subrepos=False):
        """Return the status of files in the working directory as a list of
        (<status>, <path>) pairs, where <status> can be:

          'modified', 'added', 'removed', 'clean', 'missing',
          'untracked', 'ignored', 'original'"""

        if rev and change:
            raise ValueError('cannot specify both rev and change')

        files = self._map_files(files)
        rev = self._map_one_rev(rev)
        change = self._map_one_rev(rev)

        out = self._client.execute('status', files, A=all, m=modified,
                                   a=added, r=removed, d=deleted, c=clean,
                                   u=unknown, i=ignored, C=copies,
                                   rev=rev, change=change,
                                   I=include, X=exclude,
                                   S=subrepos, print0=True)

        result = []
        for entry in out.split('\0'):
            if entry:
                if entry[0] == ' ':
                    result.append((Repository.ORIGINAL, entry[2:]))
                else:
                    status_map = { 'M': 'modified',
                                   'A': 'added',
                                   'R': 'removed',
                                   'C': 'clean',
                                   '!': 'missing',
                                   '?': 'untracked',
                                   'I': 'ignored',
                                   ' ': 'original' }
                    status, name = entry.split(' ', 1)
                    result.append((status_map[status], name))
                    
        return result
    
    def summary(self, remote=False):
        """Return a dictionary containing a summary of the working directory
        state, including parents, branch, commit status, and available updates.

        If `remote' is True, this will check the default paths for incoming
        and outgoing changes, which can be time-consuming.

        The returned dictionary will contain at least the following:

          'parent': a list of Changesets (of length 0, 1 or 2)
          'branch': the current branch
          'clean' : True if the working directory is clean, False otherwise
          'commit': a dictionary of counts for each repository status
          'update': number of available updates

        If you're using bookmarks, it will also contain

          'bookmarks'      : a list of bookmarks associated with parent
          'active-bookmark': the active bookmark, if any

        If `remote' is True, it will also contain a tuple of counts

          'remote': (in, in bookmarks, out, out bookmarks)

        and if you're using the mq extension, it will have

          'mq'    : (applied count, unapplied          %%  - literal '%' character
          %H  - changeset hash (40 hex digits)
          %R  - changeset revision number
          %b  - basename of the exporting repository
          %h  - short-form changeset hash (12 hex digits)
          %m  - first line of the commit message
          %n  - zero-padded sequence number, starting at 1
          %r  - zero-padded changeset revision number
          %%  - literal '%' character
          %H  - changeset hash (40 hex digits)
          %R  - changeset revision number
          %b  - basename of the exporting repository
          %h  - short-form changeset hash (12 hex digits)
          %m  - first line of the commit message
          %n  - zero-padded sequence number, starting at 1
          %r  - zero-padded changeset revision number
 count)

        Any entries returned by Mercurial that we do not understand will also
        form a part of the dictionary."""
        out = self._client.execute('summary', remote=remote).splitlines()

        result = {}
        while out:
            line = out.pop(0)
            name, value = line.split(': ', 1)

            if name == 'parent':
                parent, tags = value.split(' ', 1)
                rev, node = parent.split(':')
                rev = int(rev)

                value = result.get('parent', [])
                
                if rev != -1:
                    # Ignore the message
                    out.pop(0)
                    # Sadly we can't use the node value from summary, because
                    # it's truncated and there's no way to get summary to output
                    # a full-length ID :-(
                    #
                    # The result is that we can't be lazy here.
                    cset = self[rev]
                    value.append(cset)
            elif name == 'branch':
                pass
            elif name == 'commit':
                if value.endswith('(clean)'):
                    value = value[:-7]
                    result['clean'] = True
                else:
                    result['clean'] = False

                countvals = value.split(', ')
                value = {}
                for countval in countvals:
                    count, name = countval.split(' ', 1)
                    count = int(count)
                    name = name.lower().strip()
                    # For consistency, map "deleted" to "missing"
                    if name == 'deleted':
                        name = 'missing'
                    value[name] = count
            elif name == 'update':
                if value == '(current)':
                    value = 0
                else:
                    value = int(value.split(' ', 1)[0])
            elif remote and name == 'remote':
                if value == '(synced)':
                    value = (0, 0, 0, 0)
                else:
                    in_count = in_bookmarks = out_count = out_bookmarks = 0

                    for v in value.split(', '):
                        count, v = v.split(' ', 1)
                        count = int(count)
                        if v == 'outgoing':
                            out_count = count
                        elif v == 'incoming':
                            in_count = count
                        elif v == 'incoming bookmarks':
                            in_bookmarks = count
                        elif v == 'outgoing bookmarks':
                            out_bookmarks = count

                    value = (in_count, in_bookmarks,
                             out_count, out_bookmarks)
            elif name == 'mq':
                applied = unapplied = 0
                for v in value.split(', '):
                    count, v = v.split(' ', 1)
                    count = int(count)
                    if v == 'applied':
                        applied = count
                    elif v == 'unapplied':
                        unapplied = count
                        
                value = (applied, unapplied)
            elif name == 'bookmarks':
                active = None
                bookmarks = []
                for v in value.split():
                    if v.startswith('*'):
                        v = v[1:]
                        active = v
                    bookmarks.append(v)
                value = bookmarks

                result['active-bookmark'] = active
                
            result[name] = value

        return result

    def recover(self):
        """Recover from an interrupted commit or pull.  Should only be
        necessary when Mercurial suggests it.

        Returns True on success."""
        eh = SimpleErrorHandler()

        self._client.execute('recover', eh=eh)

        return bool(eh)

    def rollback(self, dry_run=False, force=False):
        """Roll-back the last transaction (dangerous)

        There is only one level of rollback, and there is no way to undo
        a rollback.

        dry_run - do not perform actions
        force   - ignore safety measures (not recommended)

        Returns True on success."""
        eh = SimpleErrorHandler()

        out = self._client.execute('rollback', n=dry_run, f=force, eh=eh)

        return bool(eh)

    def remove_tag(self, name):
        """Remove the tag specified by `name'."""
        eh = SimpleErrorHandler()

        self._client.execute('tag', name, remove=True, eh=eh)

        return bool(eh)

    def tag(self, name, rev=None, message=None, date=None, user=None,
            force=False, local=False):
        """Set a new tag `name' at the specified revision, or if none is
        specified, on the working directory's parent revision.

        If a tag with the name `name' already exists, this method will
        fail unless you set `force' to True.  This is to prevent accidental
        overwrites of existing tags.

        Creating non-local tags creates a new commit, and is normally only
        done at the head of a branch.  If you try to create a tag with the
        working directory anywhere other than at a branch head, this method
        will fail unless `force' is True.

        Returns True on success."""
        eh = SimpleErrorHandler()

        rev = self._map_one_rev(rev)
        self._client.execute('tag', name, r=rev, l=local, f=force,
                             m=message, d=date, u=user, eh=eh)

        return bool(eh)
    
    def tags(self):
        """Return a list of repository tags as (name, changeset, is_local)"""
        out = self._client.execute('tags', v=True, debug=True)

        result = []
        for line in out.splitlines():
            is_local = line.endswith(' local')
            if is_local:
                line = line[:-6]
            name, rev = line.rsplit(' ', 1)
            rev, node = rev.split(':')
            cset = self._get_lazy(int(rev), node)
            result.append((name.strip(), cset, is_local))
            
        return result

    def unbundle(self, files, update=False):
        """Apple one or more changegroup files generated by the bundle() method.

        If `update' is False, returns True on success, otherwise, if `update'
        is True, returns a tuple containing the number of files in each state:

           (updated, merged, removed, unresolved)"""

        eh = SimpleErrorHandler()
        
        out = self._client.execute('unbundle', files, u=update, eh=eh)

        if update:
            return tuple([int(x) for x in self._UPDATE_RESULT_RE.findall(out)])

        return bool(eh)
    
    _UPDATE_RESULT_RE = re.compile(r'(?:^|,\s+)(\d+)[\s\w]+', re.M)
    
    def update(self, rev=None, clean=False, check=False, date=None):
        """Update the repository's working directory to the specified changeset.
        If no changeset is specified, update to the tip of the current named
        branch and move the current bookmark (see "hg help bookmarks").

        clean - discard uncommitted changes (NO BACKUP)
        check - only update if there are no uncommitted changes
        date  - select the tipmost revision matching date
        rev   - the revision to which to update

        Returns a tuple containing the number of files in each state:

           (updated, merged, removed, unresolved)"""

        if clean and check:
            raise ValueError('clean and check cannot both be True')
        if rev is not None and date is not None:
            raise ValueError('you cannot specify both rev and date')

        rev = self._map_one_rev(rev)

        out = self._client.execute('update', r=rev, C=clean, c=check, d=date)

        return tuple([int(x) for x in self._UPDATE_RESULT_RE.findall(out)])

    def verify(self):
        """Verify the integrity of the repository.

        Returns True on success."""

        eh = SimpleErrorHandler()

        self._client.execute('verify', eh)

        return bool(eh)
    
