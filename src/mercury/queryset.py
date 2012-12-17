import re
import datetime

def escape_quote(str):
    return str.replace('\'', '\\\'')

class Queryset(object):
    """Represents a query on a Mercurial repository."""
    _BOOLEAN_EXPRS = frozenset(('branchpoint', 'bumped', 'closed',
                                'draft', 'extinct', 'head', 'hidden',
                                'merge', 'obsolete', 'public', 'secret',
                                'unstable', 'transplanted'))
    _STRING_EXPRS = frozenset(('adds', 'author', 'bisect', 'bookmark',
                               'contains', 'converted', 'desc', 'file',
                               'filelog', 'follow', 'grep', 'id', 'keyword',
                               'modifies', 'outgoing', 'removes',
                               'tag', 'user'))
    
    def __init__(self, base=None):
        if isinstance(base, Queryset):
            self._repo = base._repo
            self._base = base
        else:
            self._repo = base
            self._base = None
        
    def exclude(self, *args, **kwargs):
        """Exclude the specified items from the result."""
        return ExcludeQueryset(self, *args, **kwargs)

    def filter(self, *args, **kwargs):
        """Filter the query set to select the specified items."""
        return FilterQueryset(self, *args, **kwargs)

    def order_by(self, *args):
        """Sort the query set by the specified item."""
        return OrderByQueryset(self, *args)

    def _convert(self, item, parens=False):
        """Convert the specified item to a form suitable for a query."""
        from mercury.repo import Changeset
        
        if isinstance(item, Changeset):
            return 'id(%s)' % item.node
        elif isinstance(item, datetime.date) \
             or isinstance(item, datetime.time) \
             or isinstance(item, datetime.datetime):
            return '\'%s\'' % item.isoformat()
        elif isinstance(item, basestring):
            if isinstance(item, unicode):
                item = item.encode('utf-8')
            return '\'%s\'' % escape_quote(item)
        elif parens and isinstance(item, Queryset):
            return '(%s)' % item
        else:
            return str(item)

    def _terms_from_items(self, *items, **kwitems):
        terms = []
        for item in items:
            terms.append (self._convert(item, parens=True))

        for kw,val in kwitems.iteritems():
            if kw in Queryset._STRING_EXPRS:
                terms.append('%s(\'%s\')' % (kw, escape_quote(val)))
            elif kw in Queryset._BOOLEAN_EXPRS:
                if val:
                    terms.append('%s()' % kw)
            elif kw == 'common_ancestor_of':
                csets = tuple(val)
                terms.append('ancestor(%s,%s)' % (self._convert(csets[0]),
                                                  self._convert(csets[1])))
            elif kw == 'ancestors_of':
                terms.append('ancestors(%s)' % self._convert(val))
            elif kw == 'branch':
                terms.append('branch(%s)' % self._convert(val))
            elif kw == 'children_of':
                terms.append('children(%s)' % self._convert(val))
            elif kw == 'date':
                terms.append('date(\'%s\')' % val.isoformat())
            elif kw == 'date__lt':
                fmt = val.isoformat()
                terms.append('(date(<\'%s\') and not date(\'%s\'))'
                             % (fmt, fmt))
            elif kw == 'date__lte':
                terms.append('date(<\'%s\')' % val.isoformat())
            elif kw == 'date__gt':
                fmt = val.isoformat()
                terms.append('(date(>\'%s\') and not date(\'%s\'))'
                             % (fmt, fmt))
            elif kw == 'date__gte':
                terms.append('date(>\'%s\')' % val.isoformat())
            elif kw == 'date__between':
                val = tuple(val)
                terms.append('date(\'%s\' to \'%s\')' % (val[0].isoformat(),
                                                         val[1].isoformat()))
            elif kw == 'newer_than_days':
                terms.append('date(-%s)' % int(val))
            elif kw == 'older_than_days':
                terms.append('not date(-%s)' % int(val))
            elif kw == 'descendant_of':
                terms.append('descendants(%s)' % self._convert(val))
            elif kw == 'created_from':
                terms.append('destination(%s)' % self._convert(val))
            elif kw == 'metadata':
                subterms = []
                for k,v in val.iteritems():
                    if v:
                        if isinstance(v, unicode):
                            v = v.encode('utf-8')
                        else:
                            v = str(v)
                            
                        subterms.append('extra(\'%s\', \'%s\')' % (escape_quote(k),
                                                                   escape_quote(v)))
                    else:
                        subterms.append('extra(\'%s\')' % escape_quote(k))
                terms.append('(%s)' % ' and '.join(subterms))
            elif kw == 'heads_of':
                terms.append('heads(%s)' % self._convert(val))
            elif kw == 'newest_in':
                terms.append('max(%s)' % self._convert(val))
            elif kw == 'oldest_in':
                terms.append('min(%s)' % self._convert(val))
            elif kw == 'created':
                terms.append('origin(%s)' % self._convert(val))
            elif kw == 'parents_of':
                terms.append('parents(%s)' % self._convert(val))
            elif kw == 'first_parent_of':
                terms.append('p1(%s)' % self._convert(val))
            elif kw == 'second_parent_of':
                terms.append('p2(%s)' % self._convert(val))
            elif kw == 'roots_of':
                terms.append('roots(%s)' % self._convert(val))
            elif kw == 'between':
                val = tuple(val)
                terms.append('%s::%s' % (self._convert(val[0], parens=True),
                                         self._convert(val[1], parens=True)))
            elif kw == 'between_revisions':
                val = tuple(val)
                terms.append('%s:%s' % (self._convert(val[0], parens=True),
                                        self._convert(val[1], parens=True)))
                             
        return terms

    def _terms(self):
        return []
    
    def __str__(self):
        terms = self._terms()
        if not terms:
            return 'all()'
        return ' and '.join(terms)

    def _results(self):
        if getattr(self, '_cached_results', None) is None:
            self._cached_results = list(self._repo.query(str(self)))
        return self._cached_results

    def __len__(self):
        return len(self._results())

    def __getitem__(self, key):
        # In cases involving simple slices, we can generate a new queryset
        # as the result, rather than actually performing the query.  If you
        # use a step, that forces us to do the query, though we might build
        # an optimized queryset first to select only those items that will
        # be relevant.
        if isinstance(key, slice):
            stopset = None
            startset = None
            
            if key.start is not None:
                if key.start <= 0:
                    startset = LastQueryset(self, -key.start)
                else:
                    startset = self - FirstQueryset(self, key.start)
            if key.stop is not None:
                if key.stop >= 0:
                    stopset = FirstQueryset(self, key.stop)
                else:
                    stopset = self - LastQueryset(self, -key.stop)

            qset = self
            if startset is not None:
                if stopset is not None:
                    qset = stopset & startset
                else:
                    qset = startset
            else:
                qset = stopset
                
            if key.step is not None:
                return qset[::key.step]
            else:
                return qset
                
        return self._results()[key]

    def __iter__(self):
        return iter(self._results())

    def __reversed__(self):
        return iter(ReversedQueryset(self))

    def __and__(self, other):
        return AndQueryset(self, other)

    def __or__(self, other):
        return OrQueryset(self, other)
    
    def __add__(self, other):
        return OrQueryset(self, other)
    
    def __sub__(self, other):
         return DiffQueryset(self, other)

    def __pow__(self, other):
        return ParentQueryset(self, other)
    
    def __invert__(self):
        return NotQueryset(self)

    def get(self, exactly=None, min=None, max=None):
        """Retrieve results, making sure that we have between min and max
        of them.  Raises ValueError if the number of results is out of range."""
        if exactly is not None:
            min = max = exactly
        if max is None:
            results = list(self)
        else:
            results = list(self[:(max+1)])
        if min is not None and len(results) < min:
            raise ValueError('expected at least %s results' % min)
        elif max is not None and len(results) > max:
            raise ValueError('expected at most %s results' % max)
        return results

    def difference(self, other):
        return DiffQueryset(self, other)
    
    def intersection(self, other):
        return AndQueryset(self, other)

    def union(self, other):
        return OrQueryset(self, other)

    def invert(self):
        """Returns a queryset whose result is the inverse of this queryset."""
        return NotQueryset(self)

    def range(self, first, last):
        """Returns all changesets that are descendants of `first' and
        ancestors of `last', including `first' and `last' themselves.
        `first' and `last' may be None."""
        return RangeQueryset(self, first, last)

    def revrange(self, first, last):
        """Returns all changesets with revision numbers between `first' and
        `last', both inclusive."""
        return RevRangeQueryset(self, first, last)

    def reversed(self):
        """Returns a queryset whose results are the same as this queryset, but
        reversed."""
        return ReversedQueryset(self)

    def parents(self, n=1):
        return ParentQueryset(self, n)

    def ancestor(self, cset1, cset2):
        """Returns a Queryset containing the greatest common ancestor of the
        specified changesets."""
        return CommonAncestorQueryset(self, cset1, cset2)

    def ancestors(self):
        """Return a Queryset containing the ancestors of every member of this
        queryset."""
        return AncestorsQueryset(self)

    def branches(self):
        """Return a Queryset containing the branches of the changesets in this
        Queryset."""
        return BranchesQueryset(self)
    
    def children(self):
        """Return a Queryset containing the children of every member of this
        queryset."""
        return ChildrenQueryset(self)

    def descendants(self):
        """Return a Queryset containing the descendants of every member of this
        queryset."""
        return DescendantsQueryset(self)

    def destination(self):
        """Return a Queryset containing those changesets that were created by
        a graft, transplant or rebase operation using the changesets in this
        queryset."""
        return DestinationQueryset(self)

    def heads(self):
        """Return a Queryset containing those changesets that have no children
        within this queryset. c.f. roots()"""
        return HeadsQueryset(self)

    def newest(self):
        """Return a Queryset containing the most recent (i.e. highest rev number)
        changeset in this queryset."""
        return MaxQueryset(self)

    def oldest(self):
        """Return a Queryset containing the least recent (i.e. lowest reve number)
        changeset in this queryset."""
        return MinQueryset(self)

    def origin(self):
        """Return a Queryset containing those changesets that were specified
        as a source for a graft, transplant or rebase that created the changsets
        in this queryset."""
        return OriginQueryset(self)

    def first_parents(self):
        """Return a Queryset containing the first parents of each changeset
        in this queryset."""
        return ParentQueryset(self, 1)

    def second_parents(self):
        """Return a Queryset containing the second parents of each changeset
        in this queryset."""
        return ParentQueryset(self, 2)

    def parents(self):
        """Return a Queryset containing all of the parents of each changeset
        in this queryset."""
        return ParentsQueryset(self)

    def roots(self):
        """Return a Queryset containing all changesets in this queryset that
        have no parent in this queryset. c.f. heads()"""
        return RootsQueryset(self)

    def transplanted(self):
        """Return a Queryset containing those changesets in this queryset
        that have been transplanted."""
        return TransplantedQueryset(self)

class RepoQueryset(Queryset):
    def bookmark(self, name):
        """Return a Queryset representing just the specified bookmark."""
        return BookmarkQueryset(name)

    def tip(self, name):
        """Return a Queryset representing just the tip."""
        return TipQueryset()

    def tag(self, name):
        """Return a Queryset representing just the specified tag."""
        return TagQueryset(name)

    def rev(self, revision):
        """Return a Queryset representing just the specified revision."""
        return RevQueryset(revision)

    def node(self, node):
        """Return a Queryset representing just the specified node."""
        return NodeQueryset(node)

    def branch(self, name):
        """Return a Queryset representing just the given branch."""
        return BranchQueryset(name)

class SingleRevQueryset(Queryset):
    """This is a base class for the Queryset subclasses that always
    retrieve exactly one revision and for which alternative syntax
    exists such that we don't need to retrieve them before passing
    them to commands that expect a single revision."""

    @property
    def name(self):
        raise Exception('YOU MUST IMPLEMENT THIS!')
    
class BookmarkQueryset(SingleRevQueryset):
    def __init__(self, name):
        super(BookmarkQueryset, self).__init__()
        self._name = name

    def _terms(self):
        return ['bookmark(\'%s\')' % escape_quote(self._name)]

    @property
    def name(self):
        return self._name

class TipQueryset(SingleRevQueryset):
    def _terms(self):
        return ['tip()']

    @property
    def name(self):
        return 'tip'

class TagQueryset(SingleRevQueryset):
    def __init__(self, name):
        super(TagQueryset, self).__init__()
        self._name = name

    def _terms(self):
        return ['tag(\'%s\')' % escape_quote(self._name)]

    @property
    def name(self):
        return self._name

class RevQueryset(SingleRevQueryset):
    def __init__(self, revision):
        if not isinstance(revision, (long, int)):
            raise TypeError('revisions are specified using an integer')
        super(RevQueryset, self).__init__()
        self._rev = revision

    def _terms(self):
        return ['rev(%s)' % self._rev]

    @property
    def name(self):
        return '%s' % self._rev

class NodeQueryset(SingleRevQueryset):
    NODE_RE = re.compile('^[A-Fa-f0-9]{1,40}$')
    def __init__(self, node):
        if not isinstance(node, basestring) or not self.NODE_RE.match(node):
            raise TypeError('nodes are specified using a hex string of up to 40 characters')
        super(RevQueryset, self).__init__()
        self._node = node

    def _terms(self):
        return ['node(%s)' % self._node]

    @property
    def name(self):
        return self._node

class BranchQueryset(SingleRevQueryset):
    def __init__(self, name):
        super(BranchQueryset, self).__init__()
        self._name = name

    def _terms(self):
        return ['branch(\'%s\')' % escape_quote(self._name)]

    @property
    def name(self):
        return self._name

class TransplantedQueryset(Queryset):
    def _terms(self):
        return ['transplanted(%s)' % self._base]

class OriginQueryset(Queryset):
    def _terms(self):
        return ['origin(%s)' % self._base]

class MaxQueryset(Queryset):
    def _terms(self):
        return ['max(%s)' % self._base]

class MinQueryset(Queryset):
    def _terms(self):
        return ['min(%s)' % self._base]

class HeadsQueryset(Queryset):
    def _terms(self):
        return ['heads(%s)' % self._base]

class RootsQueryset(Queryset):
    def _terms(self):
        return ['roots(%s)' % self._base]

class FirstQueryset(Queryset):
    def __init__(self, base, count=1):
        super(FirstQueryset, self).__init__(base)
        self._count = count

    def _terms(self):
        if self._count == 1:
            return ['first(%s)' % self._base]
        else:
            return ['first(%s, %s)' % (self._base, self._count)]

class LastQueryset(Queryset):
    def __init__(self, base, count=1):
        super(LastQueryset, self).__init__(base)
        self._count = count

    def _terms(self):
        if self._count == 1:
            return ['last(%s)' % self._base]
        else:
            return ['last(%s, %s)' % (self._base, self._count)]

class AncestorsQueryset(Queryset):
    def _terms(self):
        return ['ancestors(%s)' % self._base]

class DestinationQueryset(Queryset):
    def _terms(self):
        return ['destination(%s)' % self._base]

class BranchesQueryset(Queryset):
    def _terms(self):
        return ['branch(%s)' % self._base]

class ChildrenQueryset(Queryset):
    def _terms(self):
        return ['children(%s)' % self._base]

class ParentsQueryset(Queryset):
    def _terms(self):
        return ['parents(%s)' % self._base]

class DescendantsQueryset(Queryset):
    def _terms(self):
        return ['descendants(%s)' % self._base]

class CommonAncestorQueryset(Queryset):
    def __init__(self, base, cset1, cset2):
        if not isinstance(cset1, (Changeset, long, int)) \
               or not isinstance(cset2, (Changeset, long, int)):
            raise TypeError('Ancestor queries argument must be a Changeset or a revision number')

        super(CommonAncestorQueryset, self).__init__(base)
        self._cset1 = cset1
        self._cset2 = cset2

    def _terms(self):
        return self._base.terms() \
               + ['ancestor(%s, %s)' % (self._convert(self._cset1,
                                                      self._cset2))]

class ParentQueryset(Queryset):
    def __init__(self, base, n):
        if not isinstance(n, (long, int)) or n < 0 or n > 2:
            raise TypeError('Parent index must be an integer between 0 and 2')
        super(ParentQueryset, self).__init__(base)
        self._n = n

    def _terms(self):
        return ['(%s)^%s' % (self._base, self._n)]

class RangeQueryset(Queryset):
    def __init__(self, base, first, last):
        if first is not None and not isinstance(first, (Changeset, long, int)):
            raise TypeError('First element in range must be a Changeset, a revision number, or None')
        if last is not None and not isinstance(last, (Changeset, long, int)):
            raise TypeError('Last element in range must be a Changeset, a revision number, or None')

        super(RangeQueryset, self).__init__(base)
        self._first = first
        self._last = last

    def _terms(self):
        return self._base._terms() + ['%s::%s' % (self._convert(first,
                                                                parens=True),
                                                  self._convert(last,
                                                                parens=True))]
    
class RevRangeQueryset(Queryset):
    def __init__(self, base, first, last):
        if first is not None and not isinstance(first, (Changeset, long, int)):
            raise TypeError('First element in range must be a Changeset, a revision number, or None')
        if last is not None and not isinstance(last, (Changeset, long, int)):
            raise TypeError('Last element in range must be a Changeset, a revision number, or None')

        super(RangeQueryset, self).__init__(base)
        self._first = first
        self._last = last

    def _terms(self):
        return self._base._terms() + ['%s:%s' % (self._convert(first,
                                                               parens=True),
                                                 self._convert(last,
                                                               parens=True))]
    
class AndQueryset(Queryset):
    def __init__(self, base, other):
        if base._repo != other._repo:
            raise ValueError('Cannot AND Querysets from different repositories')

        super(AndQueryset, self).__init__(base)
        self._other = other
        
    def _terms(self):
        return self._base._terms() + self._other._terms()

class OrQueryset(Queryset):
    def __init__(self, base, other):
        if base._repo != other._repo:
            raise ValueError('Cannot OR Querysets from different repositories')
        
        super(OrQueryset, self).__init__(base)
        self._other = other

    def _terms(self):
        return self._base._terms() + self._other._terms()

    def __str__(self):
        terms = self._terms()
        if not terms:
            return 'all()'
        return ' or '.join(terms)

class NotQueryset(Queryset):
    def _terms(self):
        return ['not (%s)' % self._base]

class DiffQueryset(Queryset):
    def __init__(self, base, other):
        if base._repo != other._repo:
            raise ValueError('Cannot subtract Querysets from different repositories')

        super(DiffQueryset, self).__init__(base)
        self._other = other

    def _terms(self):
        return ['(%s) - (%s)' % (self._base, self._other)]

class ReversedQueryset(Queryset):
    def _terms(self):
        return ['reverse(%s)' % self._base]

class ExcludeQueryset(Queryset):
    def __init__(self, base, *args, **kwargs):
        super(ExcludeQueryset, self).__init__(base)
        self._items = args
        self._kwitems = kwargs

    def _terms(self):
        return self._base._terms() \
               + ['not (%s)' \
                  % ' or '.join(self._terms_from_items(*self._items,
                                                       **self._kwitems))]


class FilterQueryset(Queryset):
    def __init__(self, base, *args, **kwargs):
        super(FilterQueryset, self).__init__(base)
        self._items = args
        self._kwitems = kwargs

    def _terms(self):
        return self._base._terms() \
               + self._terms_from_items(*self._items, **self._kwitems)

class OrderByQueryset(Queryset):
    def __init__(self, base, *args):
        super(OrderByQueryset, self).__init__(base)
        self._items = args

    def _terms(self):
        if len(self._items):
            return ['sort(%s, %s)' % (self._base, ', '.join(self._items))]
        else:
            return ['sort(%s)' % self._base]

                                     
