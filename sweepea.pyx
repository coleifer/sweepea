from cpython cimport datetime
import logging
import operator
import threading
import uuid
from collections import namedtuple
from inspect import isclass

import apsw


class _ConnectionLocal(threading.local):
    def __init__(self, **kwargs):
        super(_ConnectionLocal, self).__init__(**kwargs)
        self.closed = True
        self.conn = None
        self.transactions = []


cdef class _callable_context_manager(object):
    def __call__(self, fn):
        def inner(*args, **kwargs):
            with self:
                return fn(*args, **kwargs)
        return inner


class Database(object):
    def __init__(self, filename=':memory:', pragmas=None, **kwargs):
        self.filename = filename
        self.pragmas = pragmas or ()
        self.connect_params = kwargs
        self._local = _ConnectionLocal()
        self._lock = threading.Lock()
        self.query_builder = QueryBuilder()

    def __enter__(self):
        """
        Use the database as a context manager, opening a connection upon
        entering the wrapped block and beginning a transaction. At the end
        of the wrapped block the transaction is committed and the connection
        closed, unless an exception occurs, in which case the transaction
        is rolled back.
        """
        self.connect()
        self.begin()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Clean up the context manager, committing or rolling back and closing
        the connection.
        """
        if exc_val is not None:
            try:
                self.rollback()
            except:
                pass
        else:
            self.commit()
        if not self._local.closed:
            self.close()

    def __call__(self, fn):
        """
        Use the database as a decorator. Follows the same logic as the
        context manager, except the scope is the wrapped function.
        """
        def inner(*args, **kwargs):
            with self:
                return fn(*args, **kwargs)
        return inner

    def connect(self):
        """
        Open a connection to the database. The connection will be initialized
        using the ``_initialize_connection`` method, which sets ``PRAGMAs``,
        and registers user-defined callbacks.

        :returns: ``True``.

        .. note::
            If an open connection already exists, that connection will be
            closed and discarded.
        """
        with self._lock:
            if not self._local.closed:
                self.close()
            self._local.conn = apsw.Connection(
                self.filename,
                **self.connect_params)
            self._local.closed = False
            self._initialize_connection(self._local.conn)
        return True

    def _initialize_connection(self, conn):
        if self.pragmas:
            cursor = conn.cursor()
            for pragma, value in self.pragmas:
                cursor.execute('PRAGMA %s = %s;' % (pragma, value))
            cursor.close()

    def close(self):
        """
        Close the currently open connection. If no connection is open the
        function is a no-op.

        :returns: ``True`` if a connection was closed, ``False`` if no
            action was taken.
        """
        with self._lock:
            if not self._local.closed:
                self._local.conn.close()
                self._local.closed = True
                return True
            return False

    def connection(self):
        """
        Returns a connection to the database, creating one if one does not
        already exist.

        :returns: A connection to the database.
        """
        if self._local.closed:
            self.connect()
        return self._local.conn

    def cursor(self):
        """
        Returns a cursor on the current connection. If no connection is
        currently open, one will be created.

        :returns: A cursor.
        """
        local = self._local
        if local.closed:
            self.connect()
        return local.conn.cursor()

    def execute_sql(self, sql, params=None):
        """
        Execute a SQL query, returning a cursor that can be used to iterate
        over any results, or retrieve the number of rows modified, etc.

        If no connection is currently open, one will be created.

        :returns: A cursor.
        """
        cursor = self.cursor()
        cursor.execute(sql, params or ())
        return cursor

    def last_insert_id(self, cursor):
        """
        Retrieve the primary key of the most-recently inserted row on this
        connection.
        """
        return cursor.getconnection().last_insert_rowid()

    def rows_affected(self, cursor):
        """
        Retrieve the number of rows affected by the most recently executed
        data-modifying query on this connection.
        """
        return cursor.getconnection().changes()

    def get_autocommit(self):
        """
        Return a boolean indicating whether the underlying connection is in
        autocommit mode.
        """
        return self.connection().getautocommit()

    def push_transaction(self, transaction):
        self._local.transactions.append(transaction)

    def pop_transaction(self):
        self._local.transactions.pop()

    def transaction_depth(self):
        return len(self._local.transactions)

    def atomic(self):
        """
        Execute statements in either a transaction or a savepoint. The
        outer-most call to *atomic* will use a transaction, and any subsequent
        nested calls will use savepoints.

        ``atomic`` can be used as either a context manager or a decorator.

        .. note::
            For most use-cases, it makes the most sense to always use
            ``atomic`` when you wish to execute queries in a transaction.
            The benefit of using ``atomic`` is that you do not need to
            manually keep track of the transaction stack depth, as this will
            be managed for you.
        """
        return _atomic(self)

    def transaction(self):
        """
        Execute statements in a transaction using either a context manager or
        decorator. If an error is raised inside the wrapped block, the
        transaction will be rolled back, otherwise statements are committed
        when exiting. Transactions can also be explicitly rolled back or
        committed within the transaction block by calling
        :py:meth:`~transaction.rollback` or :py:meth:`~transaction.commit`.
        If you manually commit or roll back, a new transaction will be started
        automatically.

        Nested blocks can be wrapped with ``transaction`` - the database
        will keep a stack and only commit when it reaches the end of the outermost
        function / block.
        """
        return _transaction(self)

    def savepoint(self):
        """
        Execute statements in a savepoint using either a context manager or
        decorator. If an error is raised inside the wrapped block, the
        savepoint will be rolled back, otherwise statements are committed when
        exiting. Like :py:meth:`~Database.transaction`, a savepoint can also
        be explicitly rolled-back or committed by calling
        :py:meth:`~savepoint.rollback` or :py:meth:`~savepoint.commit`. If you
        manually commit or roll back, a new savepoint **will not** be created.

        Savepoints can be thought of as nested transactions.

        :param str sid: An optional string identifier for the savepoint.
        """
        return _savepoint(self)

    def begin(self, lock='DEFERRED'):
        """
        Begin a new transaction.

        :param str lock: DEFERRED, IMMEDIATE or EXCLUSIVE.

        See https://www.sqlite.org/lang_transaction.html for details.
        """
        self.cursor().execute('BEGIN %s;' % lock)

    def commit(self):
        """
        Commit the currently open transaction. If no transaction is open,
        an ``apsw.SQLError`` will be raised.
        """
        self.cursor().execute('COMMIT;')

    def rollback(self):
        """
        Roll-back the currently open transaction. If no transaction is open,
        an ``apsw.SQLError`` will be raised.
        """
        self.cursor().execute('ROLLBACK;')

    def get_tables(self):
        """
        Returns a sorted list of tables in the database.
        """
        cursor = self.execute_sql('SELECT name FROM sqlite_master WHERE '
                                  'type = ? ORDER BY name;', ('table',))
        return [row[0] for row in cursor.fetchall()]

    def get_indexes(self, table):
        """
        Returns a list of index metadata for the given table. Index metadata
        is returned as a 4-tuple consisting of:

        * Index name.
        * SQL used to create the index.
        * Names of columns being indexed.
        * Whether the index is unique.
        """
        query = ('SELECT name, sql FROM sqlite_master '
                 'WHERE tbl_name = ? AND type = ? ORDER BY name')
        cursor = self.execute_sql(query, (table, 'index'))
        index_to_sql = dict(cursor.fetchall())

        # Determine which indexes have a unique constraint.
        unique_indexes = set()
        cursor = self.execute_sql('PRAGMA index_list("%s")' % table)
        for row in cursor.fetchall():
            name = row[1]
            is_unique = int(row[2]) == 1
            if is_unique:
                unique_indexes.add(name)

        # Retrieve the indexed columns.
        index_columns = {}
        for index_name in sorted(index_to_sql):
            cursor = self.execute_sql('PRAGMA index_info("%s")' % index_name)
            index_columns[index_name] = [row[2] for row in cursor.fetchall()]

        return [(
            name,
            index_to_sql[name],
            index_columns[name],
            name in unique_indexes)
            for name in sorted(index_to_sql)]

    def get_columns(self, table):
        """
        Returns a list of column metadata for the given table. Column
        metadata is returned as a 4-tuple consisting of:

        * Column name.
        * Data-type column was declared with.
        * Whether the column can be NULL.
        * Whether the column is the primary key.
        """
        cursor = self.execute_sql('PRAGMA table_info("%s")' % table)
        return [(row[1], row[2], not row[3], bool(row[5]))
                for row in cursor.fetchall()]

    def get_primary_keys(self, table):
        """
        Returns a list of column(s) that comprise the table's foreign key.
        """
        cursor = self.execute_sql('PRAGMA table_info("%s")' % table)
        return [row[1] for row in cursor.fetchall() if row[-1]]

    def get_foreign_keys(self, table):
        """
        Returns a list of foreign key metadata for the given table. Foreign
        key metadata is returned as a 3-tuple consisting of:

        * Source column name, i.e. the column on the given table.
        * Destination table.
        * Destination column.
        """
        cursor = self.execute_sql('PRAGMA foreign_key_list("%s")' % table)
        return [(row[3], row[2], row[4]) for row in cursor.fetchall()]

    def select(self, *selection):
        """
        Generate a ``SELECT`` query using the :py:class:`SelectQuery` helper.

        :param selection: List of columns, values, etc to select.
        :returns: A :py:class:`SelectQuery` instance.
        """
        if selection:
            return SelectQuery(self).select(*selection)
        return SelectQuery(self)

    def insert(self, field_dict=None, rows=None, query=None):
        """
        Generate an ``INSERT`` query using the :py:class:`InsertQuery` helper.

        :param field_dict: A dictionary mapping column name to value.
        :param rows: A list of dictionaries mapping column name to value.
            Note: **all dictionaries must have the same keys**.
        :param SelectQuery query: A ``SELECT`` query to use as the source for
            the rows to be inserted.
        :returns: A :py:class:`InsertQuery` instance.
        """
        insert_query = InsertQuery(self)
        if field_dict is not None or rows is not None or query is not None:
            return insert_query.values(field_dict, rows, query)
        return insert_query

    def update(self, **values):
        """
        Generate an ``UPDATE`` query using the :py:class:`UpdateQuery` helper.

        :param dict values: A dictionary mapping column name to value or
            value expression.
        :returns: A :py:class:`UpdateQuery` instance.
        """
        query = UpdateQuery(self)
        if values:
            return query.set(**values)
        return query

    def delete(self):
        """
        Generate a ``DELETE`` query using the :py:class:`DeleteQuery` helper.
        This function accepts no parameters.

        :returns: A :py:class:`DeleteQuery` instance.
        """
        return DeleteQuery(self)

    def __getattr__(self, name):
        """
        Magic attributes are assumed to be tables, so that ``db.tweets`` will
        create a reference to a ``tweets`` table.
        """
        return Table((name,))


cdef class _atomic(_callable_context_manager):
    cdef:
        object db
        object _helper

    def __init__(self, db):
        self.db = db

    def __enter__(self):
        if self.db.transaction_depth() == 0:
            self._helper = self.db.transaction()
        else:
            self._helper = self.db.savepoint()
        return self._helper.__enter__()

    def __exit__(self, exc_type, exc_val, exc_tb):
        return self._helper.__exit__(exc_type, exc_val, exc_tb)


cdef class _transaction(_callable_context_manager):
    cdef:
        object db
        basestring lock
        bint _orig

    def __init__(self, db, lock='DEFERRED'):
        self.db = db
        self.lock = lock

    cpdef _begin(self):
        self.db.begin(self.lock)

    cpdef commit(self, begin=True):
        self.db.commit()
        if begin:
            self._begin()

    cpdef rollback(self, begin=True):
        self.db.rollback()
        if begin:
            self._begin()

    def __enter__(self):
        if self.db.transaction_depth() == 0:
            self._begin()
        self.db.push_transaction(self)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            if exc_type:
                self.rollback(False)
            elif self.db.transaction_depth() == 1:
                try:
                    self.commit(False)
                except:
                    self.rollback(False)
                    raise
        finally:
            self.db.pop_transaction()


cdef class _savepoint(_callable_context_manager):
    cdef:
        object db
        basestring sid, quoted_sid

    def __init__(self, db, sid=None):
        self.db = db
        self.sid = sid or 's' + uuid.uuid4().hex
        self.quoted_sid = "%s" % self.sid

    cpdef _execute(self, basestring query):
        self.db.execute_sql(query)

    cpdef commit(self):
        self._execute('RELEASE SAVEPOINT %s;' % self.quoted_sid)

    cpdef rollback(self):
        self._execute('ROLLBACK TO SAVEPOINT %s;' % self.quoted_sid)

    def __enter__(self):
        self._execute('SAVEPOINT %s;' % self.quoted_sid)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            self.rollback()
        else:
            try:
                self.commit()
            except:
                self.rollback()
                raise


cdef dict comparison_map = {
    0: '<',
    2: '=',
    4: '>',
    1: '<=',
    3: '!=',
    5: '>=',
}

cdef returns_clone(method):
    def inner(self, *args, **kwargs):
        clone = self.clone()
        method(clone, *args, **kwargs)
        return clone
    return inner


cdef class Node(object):
    cdef:
        public bint _negated
        public basestring _alias

    node_type = 'node'

    def __init__(self):
        self._negated = False

    cdef clone_base(self):
        return type(self)()

    cpdef clone(self):
        clone_obj = self.clone_base()
        clone_obj._negated = self._negated
        clone_obj._alias = self._alias
        return clone_obj

    @returns_clone
    def alias(self, alias=None):
        self._alias = alias

    cpdef asc(self):
        return Asc(self)

    cpdef desc(self):
        return Desc(self)

    def __add__(self, rhs):
        return Expression(self, '+', rhs)
    def __sub__(self, rhs):
        return Expression(self, '-', rhs)
    def __mul__(self, rhs):
        return Expression(self, '*', rhs)
    def __div__(self, rhs):
        return Expression(self, '/', rhs)
    def __xor__(self, rhs):
        return Expression(self, '#', rhs)
    def __and__(self, rhs):
        return Expression(self, 'AND', rhs)
    def __or__(self, rhs):
        return Expression(self, 'OR', rhs)

    def __radd__(self, rhs):
        return Expression(rhs, '+', self)
    def __rsub__(self, rhs):
        return Expression(rhs, '-', self)
    def __rmul__(self, rhs):
        return Expression(rhs, '*', self)
    def __rdiv__(self, rhs):
        return Expression(rhs, '/', self)
    def __rxor__(self, rhs):
        return Expression(rhs, '^', self)
    def __rand__(self, rhs):
        return Expression(rhs, 'AND', self)
    def __ror__(self, rhs):
        return Expression(rhs, 'OR', self)

    def __pow__(self, rhs, x):
        return Expression(self, 'GLOB', rhs)
    def __mod__(self, rhs):
        return Expression(self, 'LIKE', rhs)
    def __lshift__(self, rhs):
        return Expression(self, 'IN', rhs)
    def __rshift__(self, rhs):
        return Expression(self, 'IS', rhs)

    def bin_and(self, rhs):
        return Expression(self, '&', rhs)
    def bin_or(self, rhs):
        return Expression(self, '|', rhs)
    def modulo(self, rhs):
        return Expression(self, '%', rhs)
    def in_(self, rhs):
        return Expression(self, 'IN', rhs)
    def not_in(self, rhs):
        return Expression(self, 'NOT IN', rhs)
    def is_null(self, is_null=True):
        op = 'IS' if is_null else 'IS NOT'
        return Expression(self, op, None)

    cpdef contains(self, rhs):
        return Expression(self, 'LIKE', '%%%s%%' % rhs)
    cpdef startswith(self, rhs):
        return Expression(self, 'LIKE', '%s%%' % rhs)
    cpdef endswith(self, rhs):
        return Expression(self, 'LIKE', '%%%s' % rhs)
    cpdef between(self, low, high):
        return Expression(self, 'BETWEEN', Expression(low, 'AND', high))
    cpdef regexp(self, rhs):
        return Expression(self, 'REGEXP', rhs)
    cpdef concat(self, rhs):
        return Expression(self, '||', rhs)
    cpdef match(self, rhs):
        return Expression(self, 'MATCH', rhs)

    def __pos__(self):
        return self.asc()
    def __neg__(self):
        return self.desc()

    def __invert__(self):
        clone = self.clone()
        clone._negated = not self._negated
        return clone

    def __richcmp__(self, rhs, operation):
        return Expression(self, comparison_map[operation], rhs)


cdef class _Ordering(Node):
    cdef:
        Node node

    direction = None
    node_type = 'ordering'

    def __init__(self, node):
        self.node = node

    cdef clone_base(self):
        return type(self)(self.node)


cdef class Asc(_Ordering):
    direction = 'ASC'


cdef class Desc(_Ordering):
    direction = 'DESC'


cdef class Expression(Node):
    cdef:
        readonly object lhs
        readonly object rhs
        readonly basestring op
        readonly bint flat

    node_type = 'expression'

    def __init__(self, lhs, op, rhs, flat=False):
        super(Expression, self).__init__()
        self.lhs = lhs
        self.op = op
        self.rhs = rhs
        self.flat = flat

    def __repr__(self):
        return '<Expression: %s %s %s>' % (self.lhs, self.op, self.rhs)

    cdef clone_base(self):
        return Expression(self.lhs, self.op, self.rhs, self.flat)


cdef class Entity(Node):
    cdef:
        readonly tuple path
        readonly basestring str_path

    node_type = 'entity'

    def __init__(self, path):
        super(Entity, self).__init__()
        self.path = path
        self.str_path = '.'.join('"%s"' % p for p in self.path)

    def __repr__(self):
        return '<Entity: %s>' % self.str_path

    def __hash__(self):
        return hash('Entity.%s' % self.str_path)

    def __richcmp__(self, rhs, operation):
        return Expression(self, comparison_map[operation], rhs)

    def __getattr__(self, name):
        return Entity(self.path + (name,))

    cdef clone_base(self):
        return type(self)(self.path)

    cdef Entity tail(self):
        return Entity((self.path[-1],))


cdef class Table(Entity):
    cdef:
        dict columns

    def __init__(self, path):
        super(Table, self).__init__(path)
        self.columns = {}  # Keep a cache of column instances handy.

    def __getattr__(self, name):
        key = (self._alias, name)
        if key not in self.columns:
            if self._alias:
                self.columns[key] = Entity((self._alias, name))
            else:
                self.columns[key] = Entity(self.path + (name,))
        return self.columns[key]


cdef class SQL(Node):
    cdef:
        basestring sql
        tuple params

    node_type = 'sql'

    def __init__(self, sql, *params):
        super(SQL, self).__init__()
        self.sql = sql
        self.params = params

    def __repr__(self):
        if self.params:
            return '<SQL: %s %s>' % (self.sql, self.params)
        return '<SQL: %s>' % self.sql

    cdef clone_base(self):
        return type(self)(self.sql, self.params)


cdef class Function(Node):
    cdef:
        readonly basestring name
        readonly tuple arguments
        bint _coerce

    node_type = 'function'

    def __init__(self, name, arguments, coerce=True):
        super(Function, self).__init__()
        self.name = name
        self.arguments = arguments
        self._coerce = coerce

    def __repr__(self):
        return '<Function: %s(%s)>' % (
            self.name, ', '.join(repr(a) for a in self.arguments))

    def __getattr__(self, attr):
        def decorator(*args):
            return Function(attr, args)
        return decorator

    cdef clone_base(self):
        return Function(self.name, self.arguments, self._coerce)


fn = Function(None, None)


cdef class Clause(Node):
    cdef:
        basestring glue
        bint parens
        tuple nodes

    node_type = 'clause'

    def __init__(self, nodes, glue=' ', parens=False):
        super(Clause, self).__init__()
        self.glue = glue
        self.parens = parens
        if not isinstance(nodes, tuple):
            nodes = tuple(nodes)
        self.nodes = nodes

    def __repr__(self):
        cdef str content
        content = self.glue.join(repr(p) for p in self.nodes)
        if self.parens:
            content = '(%s)' % content
        return '<Clause: %s>' % content

    cdef clone_base(self):
        return type(self)(self.nodes, self.glue, self.parens)


cdef class CommaClause(Clause):
    def __init__(self, nodes):
        super(CommaClause, self).__init__(nodes, glue=', ')


cdef class EnclosedClause(Clause):
    def __init__(self, nodes):
        super(EnclosedClause, self).__init__(nodes, glue=', ', parens=True)


cdef class Check(SQL):
    def __init__(self, check_expression):
        super(Check, self).__init__('CHECK (%s)' % check_expression)


cdef class QueryBuilder(object):
    """
    QueryBuilder is responsible for accepting a subclass of ``Node`` and
    converting it into the appropriate SQL / params.
    """
    cdef dict _parse_map

    def __init__(self, parse_extensions=None):
        self._parse_map = {
            'expression': self.parse_expression,
            'function': self.parse_function,
            'sql': self.parse_sql,
            'clause': self.parse_clause,
            'entity': self.parse_entity,
            'ordering': self.parse_ordering,
            'field': self.parse_field,
            'model': self.parse_model,
            'model_instance': self.parse_model_instance,
        }
        if parse_extensions:
            self._parse_map.update(parse_extensions)

    cpdef tuple build_query(self, Clause clause):
        """
        Entry-point for the query-builder. Accepts a Clause (a space-separated
        list of nodes) and returns a 2-tuple consisting of a SQL query and
        a tuple of parameters.
        """
        return self.parse_node(clause, None)

    cdef tuple parse_node(self, node, converter):
        cdef:
            basestring node_type = '', sql
            bint is_model
            tuple params

        try:
            node_type = node.node_type
        except AttributeError:
            sql, params = ('?', (node,))
        else:
            # Differentiate between model classes and model instances.
            is_model = node_type == 'model'
            if is_model and isinstance(node, Model):
                node_type = 'model_instance'

            try:
                sql, params = self._parse_map[node_type](self, node, converter)
            except KeyError:
                if node_type == 'query':
                    sql, params = node.sql(nested=True)
                else:
                    raise ValueError('Unsupported node type.')

            # Because Model is a class, _alias and _negated are exposed as
            # descriptors on the Node class. Or, in other words, this check
            # only applies to instances.
            if not is_model:
                if node._negated:
                    sql = 'NOT %s' % sql
                if node._alias:
                    sql = ' '.join((sql, 'AS', node._alias))

        if converter:
            params = tuple(map(converter, params))

        return sql, params

    cdef tuple parse_ordering(self, _Ordering node, converter):
        cdef basestring node_sql
        cdef tuple node_params
        node_sql, node_params = self.parse_node(node.node, converter)
        return (node_sql + ' %s' % node.direction, node_params)

    cdef tuple parse_sql(self, SQL sql, converter):
        return sql.sql, sql.params

    cdef tuple parse_expression(self, Expression expr, converter):
        cdef basestring lhs, rhs
        cdef tuple largs, rargs
        if isinstance(expr.lhs, Field):
            converter = expr.lhs.db_value
        lhs, largs = self.parse_node(expr.lhs, converter)
        rhs, rargs = self.parse_node(expr.rhs, converter)
        if expr.flat:
            return ('%s %s %s' % (lhs, expr.op, rhs), largs + rargs)
        else:
            return ('(%s %s %s)' % (lhs, expr.op, rhs), largs + rargs)

    cdef tuple parse_function(self, Function func, converter):
        cdef list param_sql = []
        cdef tuple param_accum = (), tmp_params = ()
        cdef basestring tmp_sql
        cdef Node argument
        for argument in func.arguments:
            tmp_sql, tmp_params = self.parse_node(argument, converter)
            param_sql.append(tmp_sql)
            param_accum += tmp_params
        return ('%s(%s)' % (func.name, ', '.join(param_sql)), param_accum)

    cdef tuple parse_entity(self, Entity entity, converter):
        return entity.str_path, ()

    cdef tuple parse_field(self, Field field, converter):
        return self.parse_entity(field.column, field.db_value)

    cdef tuple parse_model(self, model, converter):
        return model._meta.table.str_path, ()

    cdef tuple parse_model_instance(self, model, converter):
        return ('?', (model,))

    cdef tuple parse_clause(self, Clause clause, converter):
        cdef list node_sql = []
        cdef tuple param_accum = (), tmp_params = ()
        cdef basestring tmp_sql, sql = ''
        cdef object node

        for node in clause.nodes:
            tmp_sql, tmp_params = self.parse_node(node, converter)
            node_sql.append(tmp_sql)
            param_accum += tmp_params

        if clause.parens:
            sql = '('
        sql += clause.glue.join(node_sql)
        if clause.parens:
            sql += ')'

        return (sql, param_accum)


cdef class CursorWrapper(object):
    """
    CursorWrapper provides efficient iteration over the results of a query.
    The CursorWrapper, by default, caches each processed row so that multiple
    iterations or indexing operations do not result in multiple queries.
    Additionally, the CursorWrapper is designed to be extensible, allowing
    raw tuples to be converted into more useful data-structures like
    ``dict``, ``namedtuple``, or any generic object.

    Typically you will simply iterate over the ``CursorWrapper``, or perform
    indexing operations. In these scenarios, results will be cached to make
    repeat operations performant.

    To iterate over the results *without* caching each row, you can use the
    :py:meth:`CursorWrapper.iterator` method (a-la Django). This is
    particularly useful when iterating over very large result sets or when
    you know you only need a single-shot iteration.
    """
    cdef:
        object cursor
        readonly int count
        int index
        bint initialized
        readonly bint populated
        readonly list row_cache

    def __init__(self, cursor):
        self.cursor = cursor
        self.count = 0
        self.index = 0
        self.initialized = False
        self.populated = False
        self.row_cache = []

    def __iter__(self):
        if self.populated:
            return iter(self.row_cache)
        return ResultIterator(self)

    cdef initialize(self):
        pass

    cdef iterate(self):
        cdef tuple row
        row = self.cursor.fetchone()
        if not row:
            self.populated = True
            self.cursor.close()
            raise StopIteration
        elif not self.initialized:
            self.initialize()  # Lazy initialization.
            self.initialized = True
        self.count += 1
        self.row_cache.append(self.process_row(row))
        return self.row_cache[-1]

    cdef process_row(self, tuple row):
        return row

    def iterator(self):
        """Efficient one-pass iteration over the result set."""
        while True:
            yield self.iterate()

    cdef fill_cache(self, float n=0):
        cdef:
            ResultIterator iterator

        n = n or float('Inf')
        if n < 0:
            raise ValueError('Negative values are not supported.')

        iterator = ResultIterator(self)
        iterator.index = self.count
        while not self.populated and (n > self.count):
            try:
                iterator.next()
            except StopIteration:
                break


cdef class DictCursorWrapper(CursorWrapper):
    """
    CursorWrapper implementation that converts row tuples into dictionaries.
    """
    cdef:
        list columns
        int ncols

    cdef initialize(self):
        cdef tuple description = self.cursor.getdescription()
        self.columns = [t[0][t[0].find('.') + 1:]
                        for t in description]
        self.ncols = len(description)

    cdef dict _row_to_dict(self, tuple row):
        cdef:
            dict result = {}
            int i = 0
        for i in range(self.ncols):
            result[self.columns[i]] = row[i]
        return result

    cdef process_row(self, tuple row):
        return self._row_to_dict(row)


cdef class NamedTupleCursorWrapper(CursorWrapper):
    """
    CursorWrapper implementation that converts row tuples into namedtuples.
    """
    cdef:
        object tuple_class

    cdef initialize(self):
        self.tuple_class = namedtuple('Row', self.cursor.getdescription)

    cdef process_row(self, tuple row):
        return self.tuple_class(*row)


cdef class ObjectCursorWrapper(DictCursorWrapper):
    """
    CursorWrapper implementation that converts row tuples into an arbitrary
    constructor function. The constructor function should accept ``**kwargs``-
    style keyword arguments, which will be the columns and associated values.
    """
    cdef:
        object constructor

    def __init__(self, cursor, constructor):
        super(ObjectCursorWrapper, self).__init__(cursor)

    cdef process_row(self, tuple row):
        cdef dict row_dict = self._row_to_dict(row)
        return self.constructor(**row_dict)


cdef class ResultIterator(object):
    """
    Iterator used by CursorWrapper implementations.
    """
    cdef:
        CursorWrapper cursor_wrapper
        int index

    def __init__(self, cursor_wrapper):
        self.cursor_wrapper = cursor_wrapper
        self.index = 0

    def __next__(self):
        if self.index < self.cursor_wrapper.count:
            obj = self.cursor_wrapper.row_cache[self.index]
        elif not self.cursor_wrapper.populated:
            self.cursor_wrapper.iterate()
            obj = self.cursor_wrapper.row_cache[self.index]
        else:
            raise StopIteration
        self.index += 1
        return obj


cpdef Clause select(selection, from_list=None, joins=None, where=None,
                    group_by=None, having=None, order_by=None, limit=None,
                    offset=None, distinct=None, model=None):
    """
    Low-level function to construct a ``SELECT`` query.

    :returns: A :py:class:`Clause` representing a ``SELECT`` query that is
        ready to be passed to a :py:class:`QueryBuilder`.
    """
    cdef:
        basestring command = 'SELECT'
        list parts

    if distinct:
        command += ' DISTINCT'
    parts = [SQL(command), CommaClause(selection)]
    if from_list is not None:
        parts.append(SQL('FROM'))
        parts.append(CommaClause(from_list))

    if joins is not None:
        pass

    if where is not None:
        parts.append(SQL('WHERE'))
        parts.append(where)

    if group_by:
        parts.append(SQL('GROUP BY'))
        parts.append(CommaClause(group_by))

    if having is not None:
        parts.append(SQL('HAVING'))
        parts.append(having)

    if order_by:
        parts.append(SQL('ORDER BY'))
        if isinstance(order_by, (list, tuple)):
            order_by = CommaClause(order_by)
        parts.append(order_by)

    if limit > 0:
        parts.append(SQL('LIMIT ?', limit))
    if offset > 0:
        parts.append(SQL('OFFSET ?', offset))

    return Clause(parts)


cdef tuple simple_key(key, model):
    cdef Field field = None

    if isinstance(key, Entity):
        key = key.path[-1]

    if model and key in model._meta.combined:
        field = model._meta.combined[key]
        key = field.column_name

    return key, field


cpdef Clause update(table, values, where=None, limit=None, on_conflict=None,
                    model=None):
    """
    Low-level function to construct an ``UPDATE`` query.

    :returns: A :py:class:`Clause` representing an ``UPDATE`` query that is
        ready to be passed to a :py:class:`QueryBuilder`.
    """
    cdef:
        list parts = []
        basestring key
        object value
        list values_list = []
        Entity e_key

    if on_conflict:
        parts.append(SQL('UPDATE OR %s' % on_conflict))
    else:
        parts.append(SQL('UPDATE'))

    parts.extend((table, SQL('SET')))
    for key, value in values.items():
        # Attempt to get the field for the key so we can convert the value
        # to the appropriate SQLite type.
        key, field = simple_key(key, model)
        e_key = Entity((key,))
        if field is not None:
            value = field.db_value(value)

        values_list.append(Expression(e_key, '=', value, True))

    parts.append(Clause(values_list, glue=', '))
    if where is not None:
        parts.append(SQL('WHERE'))
        parts.append(where)

    if limit > 0:
        parts.append(SQL('LIMIT ?', limit))

    return Clause(parts)


cpdef Clause insert(values, table, on_conflict=None, model=None):
    """
    Low-level function to construct an ``INSERT`` query.

    :returns: A :py:class:`Clause` representing an ``INSERT`` query that is
        ready to be passed to a :py:class:`QueryBuilder`.
    """
    cdef:
        basestring clean_key
        bint have_fields = False
        dict converters = {}
        Entity e_key
        Field field
        list clean_fields = []
        list fields  # The first portion of the INSERT declaring columns.
        list keys
        list parts = []
        list tmp_values
        list value_clauses  # The values portion of the INSERT.

    if on_conflict:
        parts.append(SQL('INSERT OR %s INTO' % on_conflict))
    else:
        parts.append(SQL('INSERT INTO'))

    parts.append(table)
    if isinstance(values, Clause):
        # Assume values is a query.
        parts.append(values)
        return Clause(parts)

    if isinstance(values, dict):
        keys = values.keys()
        for key in keys:
            clean_key, field = simple_key(key, model)
            if field is not None:
                converters[key] = field.db_value
            clean_fields.append(Entity((clean_key,)))
        have_fields = True
        values = (values,)

    if isinstance(values, (list, tuple)):
        value_clauses = []
        for row_dict in values:
            if not have_fields:
                keys = row_dict.keys()
                for key in keys:
                    clean_key, field = simple_key(key, model)
                    if field is not None:
                        converters[key] = field.db_value
                    clean_fields.append(Entity((clean_key,)))
                have_fields = True

            tmp_values = []
            for key in keys:
                if key in converters:
                    tmp_values.append(converters[key](row_dict[key]))
                else:
                    tmp_values.append(row_dict[key])

            value_clauses.append(EnclosedClause(tmp_values))

        parts.extend((
            EnclosedClause(clean_fields),
            SQL('VALUES'),
            CommaClause(value_clauses)))
    else:
        raise ValueError('Unsupported values type: %s' % type(values))

    return Clause(parts)


cpdef Clause delete(table, where=None, limit=None, offset=None, model=None):
    """
    Low-level function to construct a ``DELETE`` query.

    :returns: A :py:class:`Clause` representing a ``DELETE`` query that is
        ready to be passed to a :py:class:`QueryBuilder`.
    """
    cdef:
        list parts = [SQL('DELETE FROM'), table]

    if where is not None:
        parts.append(SQL('WHERE'))
        parts.append(where)

    if limit > 0:
        parts.append(SQL('LIMIT ?', limit))
    if offset > 0:
        parts.append(SQL('OFFSET ?', offset))

    return Clause(parts)


cdef class Query(Node):
    """
    Base class from which all other query types are derived.

    Clauses are exposed as chainable-methods, with queries being built up
    a clause at a time. Query instances are copied with each change, so you
    can safely re-use parts or whole queries.
    """
    cdef:
        object database
        object model
        QueryBuilder qb
        public Node _where

    node_type = 'query'

    def __init__(self, database, model=None):
        super(Query, self).__init__()
        self.database = database
        self.model = model
        self.qb = self.database.query_builder
        self._where = None

    cpdef clone(self):
        query = type(self)(self.database, self.model)
        query._alias = self._alias
        query._negated = self._negated
        return self._clone_attributes(query)

    cpdef _clone_attributes(self, query):
        if self._where is not None:
            query._where = self._where.clone()
        return query

    cdef list _model_shorthand(self, tuple args):
        cdef:
            list accum = []
            object arg

        for arg in args:
            if isclass(arg) and issubclass(arg, Model):
                accum.extend(arg._meta.sorted_fields)
            else:
                accum.append(arg)
        return accum

    cdef _add_query_clauses(self, initial, expressions, conjunction=None):
        reduced = reduce(operator.and_, expressions)
        if initial is None:
            return reduced
        conjunction = conjunction or operator.and_
        return conjunction(initial, reduced)

    @returns_clone
    def where(self, *expressions):
        """
        Add one or more ``WHERE`` clauses to the current query. If multiple
        expressions are passed in, they will be *AND-ed* together to form
        a single expression.
        """
        self._where = self._add_query_clauses(self._where, expressions)

    cpdef tuple sql(self, bint nested=False):
        raise NotImplementedError

    cdef _execute(self):
        cdef:
            basestring sql
            tuple params
        sql, params = self.sql()
        return self.database.execute_sql(sql, params)

    def execute(self):
        """Execute the query."""
        raise NotImplementedError

    def scalar(self, as_tuple=False):
        """
        Return a single result from the query. If ``as_tuple`` is specified,
        then the row itself is returned. Otherwise the default is to return
        only the first column in the first result row.
        """
        row = self._execute().fetchone()
        if row and not as_tuple:
            return row[0]
        return row


cdef class SelectQuery(Query):
    cdef:
        public list _select, _from, _group_by, _order_by
        public Node _having
        public int _limit, _offset
        public bint _distinct, _dicts, _namedtuples
        CursorWrapper cursor_wrapper

    def __init__(self, database, model=None):
        super(SelectQuery, self).__init__(database, model)
        self._select = None
        self._from = None
        self._group_by = None
        self._having = None
        self._order_by = None
        self._limit = 0
        self._offset = 0
        self._distinct = False
        self._dicts = False
        self._namedtuples = False
        self.cursor_wrapper = None
        if model is not None:
            self._from = [model._meta.table]

    cpdef _clone_attributes(self, query):
        query = super(SelectQuery, self)._clone_attributes(query)
        if self._select is not None:
            query._select = list(self._select)
        if self._from is not None:
            query._from = self._from
        if self._group_by is not None:
            query._group_by = list(self._group_by)
        if self._having is not None:
            query._having = self._having.clone()
        if self._order_by is not None:
            query._order_by = list(self._order_by)
        query._limit = self._limit
        query._offset = self._offset
        query._distinct = self._distinct
        query._dicts = self._dicts
        query._namedtuples = self._namedtuples
        return query

    @returns_clone
    def select(self, *selection):
        if not selection and self.model:
            selection = (self.model,)
        self._select = self._model_shorthand(selection)

    @returns_clone
    def from_(self, *sources):
        self._from = list(sources)

    @returns_clone
    def group_by(self, *grouping):
        self._group_by = self._model_shorthand(grouping)

    @returns_clone
    def having(self, *expressions):
        self._having = self._add_query_clauses(self._having, expressions)

    @returns_clone
    def order_by(self, *ordering):
        self._order_by = list(ordering)

    @returns_clone
    def limit(self, lim):
        self._limit = lim

    @returns_clone
    def offset(self, off):
        self._offset = off

    @returns_clone
    def paginate(self, page, paginate_by=20):
        if page > 0:
            page -= 1
        self._limit = paginate_by
        self._offset = page * paginate_by

    @returns_clone
    def distinct(self, is_distinct=True):
        self._distinct = is_distinct

    @returns_clone
    def dicts(self, dicts=True):
        self._dicts = dicts

    @returns_clone
    def namedtuples(self, namedtuples=True):
        self._namedtuples = namedtuples

    cpdef _aggregate(self, Node aggregation=None):
        if aggregation is None:
            aggregation = fn.Count(SQL('*'))
        query = self.select(aggregation).order_by()
        return query.scalar()

    def count(self, clear_limit=False):
        if self._distinct or self._group_by or self._limit or self._offset:
            return self.wrapped_count(clear_limit=clear_limit)

        return self._aggregate() or 0

    cpdef int wrapped_count(self, bint clear_limit=False):
        cdef:
            basestring sql, wrapped
            tuple params, result
        clone = self.order_by()
        if clear_limit:
            clone._limit = clone._offset = 0

        sql, params = clone.sql()
        wrapped = 'SELECT COUNT(1) FROM (%s) AS wrapped_select' % sql
        result = self.database.execute_sql(wrapped, params).fetchone()
        return result and result[0] or 0

    cpdef bint exists(self):
        clone = self.select(SQL('1')).paginate(1, 1)
        return bool(clone.scalar())

    cpdef get(self):
        clone = self.paginate(1, 1)
        try:
            return next(iter(clone.execute()))
        except StopIteration:
            raise DoesNotExist

    def first(self):
        res = self.execute()
        res.fill_cache(1)
        try:
            return res._result_cache[0]
        except IndexError:
            pass

    cpdef tuple sql(self, bint nested=False):
        cdef:
            basestring sql
            Clause query
            list selection
            tuple params

        if nested and self.model and len(self._select) > 1:
            selection = [self.model._meta.primary_key]
        else:
            selection = self._select

        query = select(
            selection,
            self._from,
            None,  # Joins.
            self._where,
            self._group_by,
            self._having,
            self._order_by,
            self._limit,
            self._offset,
            self._distinct,
            self.model)
        sql, params = self.qb.build_query(query)
        if nested:
            sql = '(%s)' % sql
        return sql, params

    cdef CursorWrapper get_default_cursor_wrapper(self):
        return CursorWrapper(self._execute())

    def execute(self):
        if self.cursor_wrapper:
            return self.cursor_wrapper
        else:
            if self._dicts:
                return DictCursorWrapper(self._execute())
            elif self._namedtuples:
                return NamedTupleCursorWrapper(self._execute())
            return self.get_default_cursor_wrapper()

    def __iter__(self):
        return iter(self.execute())

    def iterator(self):
        return iter(self.execute().iterator())

    def __getitem__(self, value):
        res = self.execute()
        if isinstance(value, slice):
            index = value.stop
        else:
            index = value
        if index is not None and index >= 0:
            index += 1
        res.fill_cache(index)
        return res.row_cache[value]


cdef class UpdateQuery(Query):
    cdef:
        public Table _table
        public dict _update
        public basestring _on_conflict
        public int _limit, _offset

    def __init__(self, database, model=None, table=None):
        super(UpdateQuery, self).__init__(database, model)
        if model is not None and table is None:
            table = model._meta.table
        self._table = table
        self._update = None
        self._on_conflict = None
        self._limit = 0
        self._offset = 0

    cpdef _clone_attributes(self, query):
        query = super(UpdateQuery, self)._clone_attributes(query)
        if self._update is not None:
            query._update = dict(self._update)
        if self._table is not None:
            query._table = self._table
        query._limit = self._limit
        query._offset = self._offset
        query._on_conflict = self._on_conflict
        return query

    @returns_clone
    def table(self, table):
        self._table = table

    @returns_clone
    def set(self, **values):
        self._update = values

    @returns_clone
    def on_conflict(self, action=None):
        self._on_conflict = action

    cpdef tuple sql(self, bint nested=False):
        cdef Clause query
        query = update(
            self._table,
            self._update,
            self._where,
            self._limit,
            self._on_conflict,
            self.model)
        return self.qb.build_query(query)

    def execute(self):
        return self.database.rows_affected(self._execute())


cdef class InsertQuery(Query):
    cdef:
        public dict _field_dict
        public SelectQuery _query
        public list _rows
        public Table _table
        public basestring _on_conflict

    def __init__(self, database, model=None):
        super(InsertQuery, self).__init__(database, model)
        self._field_dict = None
        self._query = None
        self._rows = None
        self._table = None
        self._on_conflict = None
        if model is not None:
            self._table = model._meta.table

    cpdef _clone_attributes(self, query):
        query = super(InsertQuery, self)._clone_attributes(query)
        if self._field_dict is not None:
            query._field_dict = dict(self._field_dict)
        if self._query is not None:
            query._query = self._query
        if self._rows is not None:
            query._rows = self._rows
        if self._table is not None:
            query._table = self._table
        query._on_conflict = self._on_conflict
        return query

    @returns_clone
    def values(self, field_dict=None, rows=None, query=None):
        if len(filter(None, (field_dict, rows, query))) > 1:
            raise ValueError('Only one of "field_dict", "rows" and "query" '
                             'can be specified.')
        self._field_dict = self._rows = self._query = None
        if field_dict:
            self._field_dict = field_dict
        elif rows:
            self._rows = rows
        elif query:
            self._query = query

    @returns_clone
    def into(self, table):
        self._table = table

    @returns_clone
    def on_conflict(self, action=None):
        self._on_conflict = action

    cpdef tuple sql(self, bint nested=False):
        cdef:
            object values
            Clause query
        if self._field_dict is not None:
            values = self._field_dict
        elif self._rows is not None:
            values = self._rows
        elif self._query is not None:
            values = self._query
        else:
            raise ValueError('No data to insert.')
        query = insert(values, self._table, self._on_conflict, self.model)
        return self.qb.build_query(query)

    def execute(self):
        return self.database.last_insert_id(self._execute())


cdef class DeleteQuery(Query):
    cdef:
        public Table _table
        public int _limit, _offset

    def __init__(self, database, model):
        super(DeleteQuery, self).__init__(database, model)
        self._table = None
        self._limit = 0
        self._offset = 0
        if model is not None:
            self._table = model._meta.table

    cpdef _clone_attributes(self, query):
        query = super(DeleteQuery, self)._clone_attributes(query)
        if self._table is not None:
            query._table = self._table
        query._limit = self._limit
        query._offset = self._offset
        return query

    @returns_clone
    def from_(self, table):
        self._table = table

    cpdef tuple sql(self, bint nested=False):
        cdef Clause query
        query = delete(
            self._table,
            self._where,
            self._limit,
            self._offset,
            self.model)
        return self.qb.build_query(query)

    def execute(self):
         return self.database.rows_affected(self._execute())


cdef int field_order = 0


cdef class Field(Node):
    cdef:
        basestring column_name
        object default
        readonly basestring name, sort_key
        readonly bint null, unique, primary_key
        readonly Entity column
        readonly field_order
        readonly list constraints
        readonly Model model

    field_type = ''
    node_type = 'field'

    def __init__(self, null=False, unique=False, default=None, column=None,
                 primary_key=False, constraints=None):
        global field_order
        self.null = null
        self.unique = unique
        self.default = default
        self.column_name = column
        self.primary_key = primary_key
        self.constraints = constraints

        self.model = None
        self.column = None
        self.name = ''
        field_order += 1
        self.field_order = field_order
        self.sort_key = '%s%s' % (primary_key and 1 or 2, field_order)

    cpdef bind(self, model, basestring name):
        self.model = <Model>model
        self.name = name
        self.column_name = self.column_name or name
        self.column = getattr(model._meta.table, self.column_name)
        setattr(model, name, self.get_descriptor(name))

    cdef get_descriptor(self, name):
        return FieldDescriptor(self, name)

    cpdef python_value(self, value):
        return value if value is None else self.coerce(value)

    cpdef db_value(self, value):
        return value if value is None else self.coerce(value)

    cdef coerce(self, value):
        return value

    def __richcmp__(self, rhs, operation):
        return Expression(self, comparison_map[operation], rhs)

    def __hash__(self):
        return hash('field.%s.%s' % (self.model._name, self.name))


cdef coerce_to_unicode(s):
    if isinstance(s, unicode):
        return s
    elif isinstance(s, str):
        return s.decode('utf-8')
    return unicode(s)


cdef class TextField(Field):
    field_type = 'TEXT'

    cdef coerce(self, value):
        return coerce_to_unicode(value)


cdef class IntegerField(Field):
    field_type = 'INTEGER'

    cdef coerce(self, value):
        return int(value)


cdef class FloatField(Field):
    field_type = 'REAL'

    cdef coerce(self, value):
        return float(value)


cdef class BooleanField(IntegerField):
    cpdef db_value(self, value):
        if value is None:
            return None
        return 1 if value else 0

    cpdef python_value(self, value):
        if value is None:
            return None
        return bool(value)


cdef class BlobField(Field):
    field_type = 'BLOB'

    cpdef db_value(self, value):
        if isinstance(value, basestring):
            return buffer(value)
        return value


cdef class DateTimeField(Field):
    field_type = 'DATETIME'
    formats = [
        '%Y-%m-%d %H:%M:%S.%f',
        '%Y-%m-%d %H:%M:%S',
        '%Y-%m-%d',
    ]

    cdef format_value(self, value):
        cdef str fmt

        for fmt in self.formats:
            try:
                return datetime.datetime.strptime(value, fmt)
            except ValueError:
                pass

    cpdef python_value(self, value):
        if value and isinstance(value, basestring):
            return self.format_value(value) or value
        return value

    cpdef db_value(self, value):
        return str(value) if value is not None else None


cdef class DateField(DateTimeField):
    field_type = 'DATE'
    formats = [
        '%Y-%m-%d',
        '%Y-%m-%d %H:%M:%S',
        '%Y-%m-%d %H:%M:%S.%f',
    ]

    cpdef python_value(self, value):
        if value and isinstance(value, basestring):
            value = self.format_value(value)
            if value:
                return value.date()
        return value


cdef class PrimaryKeyField(IntegerField):
    def __init__(self, *args, **kwargs):
        kwargs['primary_key'] = True
        super(PrimaryKeyField, self).__init__(*args, **kwargs)


cdef class Model  # Forward declaration.


cdef class ForeignKeyField(Field):
    cdef:
        readonly object rel_model
        readonly Field rel_field
        basestring backref

    def __init__(self, model, field=None, backref=None, *args, **kwargs):
        if model is not None:
            self.rel_model = model
        if field is not None:
            self.rel_field = field
        else:
            self.rel_field = model._meta.primary_key
        self.backref = backref or '%s_set' % (model.__name__.lower())
        super(ForeignKeyField, self).__init__(*args, **kwargs)

    cpdef python_value(self, value):
        return self.rel_field.python_value(value)

    cpdef db_value(self, value):
        if isinstance(value, Model):
            value = getattr(value, self.rel_field.name)
        return self.rel_field.db_value(value)

    cpdef bind(self, model, basestring name):
        self.column_name = self.column_name or name
        if self.column_name == name and not name.endswith('_id'):
            self.column_name = name + '_id'

        super(ForeignKeyField, self).bind(model, name)

        setattr(self.model, name + '_id', ObjectIdDescriptor(self))
        setattr(self.rel_model, self.backref, BackrefDescriptor(self, name))

        self.model._meta.refs[name] = self
        self.rel_model._meta.backrefs[self.backref] = self

    cdef get_descriptor(self, name):
        return RelatedFieldDescriptor(self, name)


cdef class RelatedFieldDescriptor(FieldDescriptor):
    cdef:
        object rel_model
        Field rel_field

    def __init__(self, field, name):
        super(RelatedFieldDescriptor, self).__init__(field, name)
        self.rel_model = field.rel_model
        self.rel_field = field.rel_field

    cpdef get_object(self, instance):
        rel_id = instance._data.get(self.att_name)
        if rel_id is not None or self.att_name in instance._obj_cache:
            if self.att_name not in instance._obj_cache:
                obj = self.rel_model.get(self.rel_field == rel_id)
                instance._obj_cache[self.att_name] = obj
            return instance._obj_cache[self.att_name]
        elif not self.field.null:
            raise DoesNotExist
        return rel_id

    def __get__(self, instance, instance_type):
        if instance is not None:
            return self.get_object(instance)
        return self.field

    def __set__(self, instance, value):
        if isinstance(value, self.rel_model):
            instance._data[self.att_name] = getattr(
                value, self.rel_field.name)
            instance._obj_cache[self.att_name] = value
        else:
            orig_value = instance._data.get(self.att_name)
            instance._data[self.att_name] = value
            if orig_value != value and self.att_name in instance._obj_cache:
                del instance._obj_cache[self.att_name]


cdef class ObjectIdDescriptor(object):
    cdef:
        basestring attr_name
        Field field

    def __init__(self, field):
        self.attr_name = field.name
        self.field = field

    def __get__(self, instance, instance_type):
        if instance is not None:
            return instance._data.get(self.attr_name)
        return self.field

    def __set__(self, instance, value):
        setattr(instance, self.attr_name, value)


cdef class BackrefDescriptor(object):
    cdef:
        basestring att_name
        Field field
        object rel_model

    def __init__(self, field, name):
        self.field = field
        self.att_name = name
        self.rel_model = field.model

    def __get__(self, instance, instance_type):
        if instance is not None:
            return self.rel_model.select().where(
                self.field == getattr(instance, self.field.rel_field.name))
        return self


cdef class ModelCursorWrapper(DictCursorWrapper):
    """
    CursorWrapper implementation that yields model instances. Additionally,
    when model fields are part of the SELECT clause, the corresponding
    ``python_value()`` functions will be used to coerce the values from the
    database into their Python equivalents.
    """
    cdef:
        dict converters
        list select
        object model

    def __init__(self, cursor, model, list select):
        super(ModelCursorWrapper, self).__init__(cursor)
        self.model = model
        self.select = select

    cdef initialize(self):
        cdef:
            basestring column
            tuple column_tuple
            tuple description = self.cursor.getdescription()
            dict column_to_field = self.model._meta.combined

        self.columns = []
        self.converters = {}
        for column_tuple in description:
            column = column_tuple[0][column_tuple[0].find('.') + 1:]
            if column in column_to_field:
                self.converters[column] = column_to_field[column].python_value
                self.columns.append(column_to_field[column].name)
            else:
                self.columns.append(column)

        self.ncols = len(description)

    cdef process_row(self, tuple row):
        cdef:
            basestring col_name
            dict result = {}
            int i = 0

        for i in range(self.ncols):
            col_name = self.columns[i]
            if col_name in self.converters:
                result[col_name] = self.converters[col_name](row[i])
            else:
                result[col_name] = row[i]

        return self.model(**result)


cdef class ModelSelectQuery(SelectQuery):
    """
    SelectQuery subclass that, by default, when executed returns a
    ModelCursorWrapper.
    """
    cdef CursorWrapper get_default_cursor_wrapper(self):
        return ModelCursorWrapper(self._execute(), self.model, self._select)


class DoesNotExist(Exception): pass


cdef class Metadata(object):
    cdef:
        readonly basestring name, table_name
        readonly dict backrefs, refs
        readonly dict columns, fields, combined
        readonly dict defaults, defaults_callables
        readonly Field primary_key
        readonly list sorted_fields
        readonly object database
        readonly Table table

    def __init__(self, database, table_name, field_list):
        self.database = database
        self.name = self.table_name = table_name

        self.backrefs = {}
        self.refs = {}
        self.columns = {}
        self.fields = {}
        self.combined = {}
        self.defaults = {}
        self.defaults_callables = {}
        self.table = Table((table_name,))

        self.initialize(field_list)

    cdef initialize(self, tuple field_list):
        cdef:
            basestring column_name, field_name
            Field field_obj

        for field_name, field_obj in field_list:
            if field_obj.primary_key:
                self.primary_key = field_obj

            # Populate the fields list.
            self.fields[field_name] = field_obj

            # Populate the column_name -> field obj mapping.
            column_name = field_obj.column_name or field_name
            self.columns[column_name] = field_obj

            # Populate the combined "fields and columns" dict.
            self.combined[field_name] = field_obj
            if column_name != field_name:
                self.combined[column_name] = field_obj

            # Populate the field_name -> default value mapping.
            if field_obj.default is not None:
                default = field_obj.default
                if callable(default):
                    self.defaults_callables[field_name] = default
                else:
                    self.defaults[field_name] = default

        self.sorted_fields = sorted(
            self.fields.values(),
            key=operator.attrgetter('sort_key'))


cdef class Model(Node):
    """
    Python representation of a database table and table metadata.
    """
    cdef:
        readonly dict _data
        readonly dict _obj_cache

    node_type = 'model'

    def __init__(self, **kwargs):
        super(Model, self).__init__()
        self._obj_cache = {}
        self._data = self._load_default_values()
        for key, value in kwargs.items():
            setattr(self, key, value)

    cdef dict _load_default_values(self):
        cdef:
            basestring name
            dict data = {}
            Metadata meta = self._meta

        for name in meta.defaults:
            data[name] = meta.defaults[name]
        for name in meta.defaults_callables:
            data[name] = meta.defaults_callables[name]()

        return data

    cpdef save(self, force_insert=False):
        cdef:
            basestring primary_key = self._meta.primary_key.name
            dict data = self._data.copy()

        if data.get(primary_key):
            pk = data.pop(primary_key)
            return (self
                    .update(**data)
                    .where(self._meta.primary_key == pk)
                    .execute())
        else:
            data.pop(primary_key, None)
            pk = self.insert(data).execute()
            setattr(self, primary_key, pk)
            return pk

    cdef pk_expr(self):
        cdef Field primary_key = self._meta.primary_key
        return primary_key == self._data[primary_key.name]

    cpdef dependencies(self, search_nullable=False):
        model = type(self)
        query = self.select().where(self.pk_expr())
        stack = [(model, query)]
        accum = []
        seen = set()
        while stack:
            klass, query = stack.pop()
            if klass in seen:
                continue
            seen.add(klass)
            for rel_name, fk in klass._meta.backrefs.items():
                rel_model = fk.model
                if fk.rel_model is model:
                    node = (fk == self._data[fk.rel_field.name])
                    subquery = rel_model.select().where(node)
                else:
                    node = fk << query
                    subquery = rel_model.select().where(node)
                if not fk.null or search_nullable:
                    stack.append((rel_model, subquery))
                accum.append((node, fk))

        return accum

    def delete_instance(self, recursive=False, delete_nullable=False):
        if recursive:
            dependencies = self.dependencies(delete_nullable)
            for query, fk in reversed(list(dependencies)):
                model = fk.model
                if fk.null and not delete_nullable:
                    model.update(**{fk.name: None}).where(query).execute()
                else:
                    model.delete().where(query).execute()
        return self.delete().where(self.pk_expr()).execute()

    @classmethod
    def select(cls, *selection):
        selection = selection or cls._meta.sorted_fields
        return (ModelSelectQuery(cls._meta.database, cls)
                .select(*selection))

    @classmethod
    def insert(cls, field_dict=None, rows=None, query=None):
        insert_query = InsertQuery(cls._meta.database, cls)
        if field_dict is not None or rows is not None or query is not None:
            return insert_query.values(field_dict, rows, query)
        return insert_query

    @classmethod
    def update(cls, **values):
        return UpdateQuery(cls._meta.database, cls).set(**values)

    @classmethod
    def delete(cls):
        return DeleteQuery(cls._meta.database, cls)

    @classmethod
    def create(cls, **kwargs):
        obj = cls(**kwargs)
        obj.save()
        return obj

    @classmethod
    def get(cls, *expressions):
        return cls.select().where(*expressions).get()

    cdef _get_pk_value(self):
        return getattr(self, self._meta.primary_key.name, None)

    def __richcmp__(self, other, op):
        pk = self._data.get(self._meta.primary_key.name)
        other_pk = other._data.get(other._meta.primary_key.name)
        if op == 2:
            return (
                other.__class__ == self.__class__ and
                pk is not None and
                pk == other_pk)
        elif op == 3:
            return not self == other


cdef class FieldDescriptor(object):
    cdef:
        Field field
        basestring att_name

    def __init__(self, field, name):
        self.field = field
        self.att_name = name

    def __get__(self, instance, instance_type):
        if instance is not None:
            return instance._data.get(self.att_name)
        return self.field

    def __set__(self, instance, value):
        instance._data[self.att_name] = value


cpdef create_model(database, basestring table_name, field_list):
    cdef:
        dict attrs = {}
        basestring column_name, field_name
        Field field_obj
        Field primary_key = None
        Metadata metadata
        object ModelClass

    for field_name, field_obj in field_list:
        if field_obj.primary_key:
            primary_key = field_obj

    if primary_key is None:
        primary_key = PrimaryKeyField()
        field_list = (('id', primary_key),) + field_list

    attrs['_meta'] = Metadata(database, table_name, field_list)

    ModelClass = type('%sModel' % table_name.title(), (Model,), attrs)

    for field_name, field_obj in field_list:
        field_obj.bind(ModelClass, field_name)

    return ModelClass
