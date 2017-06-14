.. _api:

Swee'pea's API
==============


.. py:class:: TableFunction()

    Implements a table-valued function (eponymous-only virtual table) in
    SQLite. In English, a table-valued function is a user-defined function that
    can return 0 or more rows of data. Normal user-defined functions must
    return a scalar value, and aggregate functions can accept multiple inputs
    but are still restricted to a single scalar output. These restrictions are
    lifted from table-valued functions. They are called table-valued because
    their output can be thought of as a table.

    Subclasses must define the ``columns`` (return values) and ``params``
    (input values) to their function. These are declared as class attributes.

    Subclasses must also implement two methods:

    * ``initialize(**query)``
    * ``iterate(idx)``

    .. code-block:: python

        # Example table-valued function that returns a range of integers.
        class Series(TableFunction):
            columns = ['value']
            params = ['start', 'stop', 'step']
            name = 'series'

            def initialize(self, start=0, stop=None, step=1):
                self.start = self.current = start
                self.stop = stop or float('inf')
                self.step = step

            def iterate(self, idx):
                if self.current > self.stop:
                    raise StopIteration

                return_val = self.current
                self.current += self.step
                return (return_val,)

        # Must register with a connection in order to use.
        conn = sqlite3.connect(':memory:')
        Series.register(conn)

        # Now we can call it.
        for num, in conn.execute('select * from series(0, 10, 2)'):
            print num

        # Prints 0, 2, 4, 6, 8, 10.

    .. py:attribute:: columns

        A list or tuple describing the rows returned by this function.

    .. py:attribute:: params

        A list or tuple describing the parameters this function accepts.

    .. py:attribute:: name

        The name of the table-valued function. If not provided, name will be
        inferred from the class name.

    .. py:method:: initialize(**query)

        This method is called once for each set of values the table-valued
        function is called with.

        :param query: The parameters the function was called with.
        :returns: return value is discarded.

    .. py:method:: iterate(idx)

        This method is called repeatedly until it raises ``StopIteration``. The
        return value should be a row-tuple matching the format described by the
        ``columns`` attribute.

        :param int idx: The row index being requested.
        :raises: StopIteration
        :returns: A row tuple

    .. py:method:: register(connection)

        Register the table-valued function with a SQLite database connection.
        You must register a table-valued function in order to use it.

        :param connection: a ``sqlite3.Connection`` instance.


.. py:class:: CursorWrapper(cursor)

    Wraps a SQLite3 cursor, providing additional functionality. This object
    should not be instantiated directly, but instead is returned when executing
    ``SELECT`` queries.

    When iterated over, the cursor wrapper will yield result rows as tuples.

    .. py:method:: iterator()

        Provides an iterator over the result-set that does not cache the result
        rows. Use this for iterating over large result sets, or result sets
        that only need to be iterated over once.

        Example:

        .. code-block:: python

            # Query will return a large number of rows.
            query = PageView.select(PageView.url, PageView.timestamp)
            for row in query.execute(db).iterator():
                report.write((row.url, row.timestamp))

    .. py:method:: first()

        Return the first row or ``None`` if no rows were returned.

    .. py:method:: get()

        Return the first row or raise a ``DoesNotExist`` exception if no rows
        were returned.

        :raises: DoesNotExist

    .. py:method:: scalar()

        Returns the first column of the first row, or raise a ``DoesNotExist``
        if no rows were returned. Useful for retrieving the value of a query
        that performed an aggregation, like a ``COUNT()`` or ``SUM()``.


.. py:class:: DictCursorWrapper

    A subclass of :py:class:`CursorWrapper` that yields result rows as
    dictionaries.


.. py:class:: NamedTupleCursorWrapper

    A subclass of :py:class:`CursorWrapper` that yields result rows as
    named tuples.


.. py:class:: ObjectCursorWrapper(cursor, constructor)

    A subclass of :py:class:`CursorWrapper` that accepts a constructor and for
    each result tuple, will call the constructor with the row and yield the
    return value.

    :param constructor: A callable which accepts a row of data and returns an
        arbitrary object.


Database
--------


.. py:class:: Database(database[, pragmas=None[, journal_mode=None[, rank_functions=False[, regex_function=True[, hash_functions=False[, **kwargs]]]]]])

    Wrapper for managing SQLite database connections. Handles connections in a
    thread-safe manner and provides Pythonic APIs for managing transactions,
    executing queries, and introspecting database internals.

    :param database: The filename of the SQLite database, or the string
        ``':memory:'`` for an in-memory database. To defer the initialization
        of the database, you can also specify ``None``.
    :param pragmas: A list of 2-tuples describing the pragma key and value to
        be applied when a connection is opened.
    :param journal_mode: Journaling mode to use with SQLite database.
    :param bool rank_functions: Whether to register user-defined functions for
        scoring search results. For use with full-text-search extension.
    :param bool regex_function: Whether to register a user-defined function to
        provide support for the ``REGEXP`` operator.
    :param bool hash_functions: Whether to register cryptographic hash
        functions.
    :param kwargs: Arbitrary keyword arguments passed to the ``sqlite3``
        connection constructor.

    .. py:method:: init(database, **connect_kwargs)

        This method is used to initialize a deferred database. A database is
        said to be deferred when it is instantiated with the database file as
        ``None``. Reasons you might do this are to declare the database in one
        place, and actually assign it to a given file elsewhere in the code
        (e.g. for running tests).

        :param database: The filename of the SQLite database, or the string
            ``':memory:'`` for an in-memory database.
        :param connect_kwargs: Arbitrary keyword arguments passed to the
            ``sqlite3`` connection constructor.

    .. py:method:: connect([reuse_if_open=False])

        Open a connection to the SQLite database. If a connection already
        exists for the current thread, an ``OperationalError`` will be raised.
        Alternatively, you can specify ``reuse_if_open`` to suppress the error
        in the event a connection is already open.

        :param bool reuse_if_open: If a connection already exists, re-use it
            rather than raising an exception.
        :raises OperationalError:
        :rtype bool:
        :returns: Boolean value indicating whether a connection was opened.
            Will always be ``True`` unless ``reuse_if_open`` was specified and
            a connection already existed.

    .. py:method:: close()

        Close the current thread's connection. If no connection is currently
        open, no exception will be raised.

        :rtype bool:
        :returns: Boolean indicating whether a connection was closed.

    .. py:method:: aggregate([name=None])

        Decorator for declaring and registering a user-defined aggregate
        function.

        Example:

        .. code-block:: python

            @db.aggregate('avg')
            class Average(object):
                def __init__(self):
                    self.vals = []

                def step(self, value):
                    self.vals.append(value)

                def finalize(self):
                    return sum(self.vals) / len(self.vals)

    .. py: method:: collation([name=None])

        Decorator for declaring and registering a user-defined collation.
        Collations define the ordering for a set of values.

        Example:

        .. code-block:: python

            @db.collation('numeric')
            def numeric(lhs, rhs):
                # Sort strings with numbers in them.
                l1 = [int(t) if t.isdigit() else t
                      for t in re.split('(\d+)', lhs)]
                l2 = [int(t) if t.isdigit() else t
                      for t in re.split('(\d+)', lhs)]
                return cmp(l1, l2)

    .. py:method:: func([name=None[, n=-1[, deterministic=True]]])

        Decorator for declaring and registering a user-defined function.
        User-defined functions accept up to ``n`` parameters and return a
        scalar value. If ``n`` is not fixed, you may specify ``-1``.

        :param str name: Name of the function.
        :param int n: Number of parameters function accepts, or ``-1``.
        :param bool deterministic: Function is deterministic.

        Example:

        .. code-block:: python

            @db.func('md5')
            def md5(s):
                return hashlib.md5(s).hexdigest()

    .. py:method:: table_function([name=None])

        Decorator for declaring and registering a table-valued function with
        the database. Table-valued functions are described in the section on
        :py:class:`TableFunction`, but briefly, a table-valued function accepts
        any number of parameters, and instead of returning a scalar value,
        returns any number of rows of tabular data.

        Example:

        .. code-block:: python

            @db.table_function('series')
            class Series(TableFunction):
                columns = ['value']
                params = ['start', 'stop']

                def initialize(self, start=0, stop=None):
                    self.start, self.stop = start, (stop or float('Inf'))
                    self.current = self.start

                def iterate(self, idx):
                    if self.current > self.stop:
                        raise StopIteration
                    ret = self.current
                    self.current += 1
                    return (ret,)

    .. py:method:: on_commit(func)

        Decorator for declaring and registering a post-commit hook. The
        handler's return value is ignored, but if a ``ValueError`` is raised,
        then the transaction will be rolled-back.

        The decorated function should not accept any parameters.

        Example:

        .. code-block:: python

            @db.on_commit
            def commit_handler():
                if datetime.date.today().weekday() == 6:
                    raise ValueError('no commits on sunday!')

    .. py:method:: on_rollback(func)

        Decorator for registering a rollback handler. The return value is
        ignored.

        The decorated function should not accept any parameters.

        Example:

        .. code-block:: python

            @db.on_rollback
            def rollback_handler():
                logger.info('rollback was issued.')

    .. py:method:: on_update(func)

        Decorator for registering an update hook. The decorated function is
        executed for each row that is inserted, updated or deleted. The return
        value is ignored.

        User-defined callback must accept the following parameters:

        * query type (INSERT, UPDATE or DELETE)
        * database name (typically 'main')
        * table name
        * rowid of affected row

        Example:

        .. code-block:: python

            @db.on_update
            def change_logger(query_type, db, table, rowid):
                logger.debug('%s query on %s.%s row %s', query_type, db,
                             table, rowid)

    .. py:method:: is_closed()

        Return a boolean indicating whether the database is closed.

    .. py:method:: connection()

        Get the currently open connection. If the database is closed, then a
        new connection will be opened and returned.

    .. py:method:: execute_sql(sql[, params=None[, commit=True]])

        Execute the given SQL query and returns the cursor. If no connection is
        currently open, one will be opened automatically.

        :param sql: SQL query
        :param params: A list or tuple of parameters for the query.
        :param bool commit: Whether a ``commit`` should be invoked after the
            query is executed.
        :returns: A ``sqlite3.Cursor`` instance.

    .. py:method:: execute(query)

        Execute the SQL query represented by the :py:class:`Query` object. The
        query will be parsed into a parameterized SQL query automatically.

        :param Query query: The :py:class:`Query` instance to execute.
        :returns: A ``sqlite3.Cursor`` instance.

    .. py:method:: pragma(key[, value=SENTINEL])

        Issue a PRAGMA query on the current connection. To query the status of
        a specific PRAGMA, typically only the ``key`` will be specified.

        For more information, see the `SQLite PRAGMA docs <http://sqlite.org/pragma.html>`_.

        .. note::
            Many ``PRAGMA`` settings are exposed as properties on the
            :py:class:`Database` object.

    .. py:method:: add_pragma(key, value)

        Apply the specified pragma query each time a new connection is opened.
        If a connection is currently open, the pragma will be executed.

    .. py:method:: remove_pragma(key)

        Remove the pragma operation specified by the given key from the list of
        pragma queries executed on each new connection.

    .. py:method:: begin([lock_type=None])

        Start a transaction using the specified lock type. If the lock type is
        unspecified, then a bare ``BEGIN`` statement is issued.

        Because swee'pea runs ``sqlite3`` in autocommit mode, it is necessary
        to explicitly begin transactions using this method.

        For an alternative API, see the :py:meth:`~Database.atomic` helper.

    .. py:method:: commit()

        Call ``commit()`` on the currently-open ``sqlite3.Connection`` object.

    .. py:method:: rollback()

        Call ``rollback()`` on the currently-open ``sqlite3.Connection`` object.

    .. py:method:: __getitem__(name)

        Factory method for creating :py:class:`BoundTable` instances.

        Example:

        .. code-block:: python

            UserTbl = db['users']
            query = UserTbl.select(UserTbl.c.username)
            for username, in query.execute():
                print username

    .. py:method:: __enter__()

        Use the database as a context-manager. When the context manager is
        entered, a connection is opened (if one is not already open) and a
        transaction begins. When the context manager exits, the transaction is
        either committed or rolled-back (depending on whether the context
        manager exits with an exception). Finally, the connection is closed.

        Example:

        .. code-block:: python

            with database:
                database.execute_sql('CREATE TABLE foo (data TEXT)')
                FooTbl = database['foo']
                for i in range(10):
                    FooTbl.insert({FooTbl.c.data: str(i)}).execute()

    .. py:method:: last_insert_rowid()

        Return the ``rowid`` of the most-recently-inserted row on the currently
        active connection.

    .. py:method:: changes()

        Return the number of rows changed by the most recent query.

    .. py:attribute:: autocommit

        A property which indicates whether the connection is in autocommit mode
        or not.

    .. py:method:: set_busy_handler([timeout=5000])

        Replace the default SQLite busy handler with one that introduces some
        *jitter* into the amount of time delayed between checks. This addresses
        an issue that frequently occurs when multiple threads are attempting to
        modify data at nearly the same time.

        :param timeout: Max number of milliseconds to attempt to execute query.

    .. py:method:: atomic()

        Context manager or decorator that executes the wrapped statements in
        either a transaction or a savepoint. The outer-most call to ``atomic``
        will use a transaction, and any subsequent nested calls will use
        savepoints.

        .. note::
            For most use-cases, it makes the most sense to always use
            ``atomic`` when you wish to execute queries in a transaction.
            The benefit of using ``atomic`` is that you do not need to
            manually keep track of the transaction stack depth, as this will
            be managed for you.

    .. py:method:: transaction()

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

    .. py:method:: savepoint()

        Execute statements in a savepoint using either a context manager or
        decorator. If an error is raised inside the wrapped block, the
        savepoint will be rolled back, otherwise statements are committed when
        exiting. Like :py:meth:`~Database.transaction`, a savepoint can also
        be explicitly rolled-back or committed by calling
        :py:meth:`~savepoint.rollback` or :py:meth:`~savepoint.commit`. If you
        manually commit or roll back, a new savepoint **will not** be created.

        Savepoints can be thought of as nested transactions.

        :param str sid: An optional string identifier for the savepoint.

    .. py:method:: get_tables()

        Return a sorted list of the tables in the database.

    .. py:method:: get_indexes(table)

        Returns a list of index metadata for the given table. The index
        metadata is returned as a 4-tuple consisting of:

        * Index name.
        * SQL used to create the index.
        * Names of columns being indexed.
        * Whether the index is unique.

    .. py:method:: get_columns(table)

        Returns a list of column metadata for the given table. Column
        metadata is returned as a 4-tuple consisting of:

        * Column name.
        * Data-type column was declared with.
        * Whether the column can be NULL.
        * Whether the column is the primary key.

    .. py:method:: get_primary_keys(table)

        Returns a list of column(s) that comprise the table's primary key.

    .. py:method:: get_foreign_keys(table)

        Returns a list of foreign key metadata for the given table. Foreign
        key metadata is returned as a 3-tuple consisting of:

        * Source column name, i.e. the column on the given table.
        * Destination table.
        * Destination column.

    .. py:method:: backup(dest_db)

        Backup the current database to the given destination
        :py:class:`Database` instance.

        :param Database dest_db: database to hold backup.

    .. py:method:: backup_to_file(filename)

        Backup the current database to the given filename.

    .. py:attribute:: cache_size

        Property that exposes ``PRAGMA cache_size``.

    .. py:attribute:: foreign_keys

        Property that exposes ``PRAGMA foreign_keys``.

    .. py:attribute:: journal_mode

        Property that exposes ``PRAGMA journal_mode``.

    .. py:attribute:: journal_size_limit

        Property that exposes ``PRAGMA journal_size_limit``.

    .. py:attribute:: mmap_size

        Property that exposes ``PRAGMA mmap_size``.

    .. py:attribute:: page_size

        Property that exposes ``PRAGMA page_size``

    .. py:attribute:: read_uncommited

        Property that exposes ``PRAGMA read_uncommited``

    .. py:attribute:: synchronous

        Property that exposes ``PRAGMA synchronous``

    .. py:attribute:: wal_autocheckpoint

        Property that exposes ``PRAGMA wal_autocheckpoint``

SQL Builder
-----------

.. py:class:: Table(name[, columns=None[, schema=None[, alias=None]]])

    Represents a table in a SQL query. Tables can be initialized with a static
    list of columns, or columns can be referenced dynamically using the ``c``
    attribute.

    Example:

    .. code-block:: python

        # Example using a static list of columns:
        UserTbl = Table('users', ('id', 'username'))
        query = (UserTbl
                 .select()  # "id" and "username" automatically selected.
                 .order_by(UserTbl.username))

        # Using dynamic columns:
        TweetTbl = Table('tweets')
        query = (TweetTbl
                 .select(TweetTbl.c.content, TweetTbl.c.timestamp)
                 .join(UserTbl, on=(TweetTbl.c.user_id == UserTbl.id))
                 .where(UserTbl.username == 'charlie')
                 .order_by(TweetTbl.c.timestamp.desc()))

    .. py:method:: bind(database)

        Create a table reference that is bound to the given database. Returns
        a :py:class:`BoundTable` instance.

    .. py:method:: select(*selection)

        Create a :py:class:`Select` query from the given table. If the
        ``selection`` is not provided, and the table defines a static list of
        columns, then the selection will default to all defined columns.

        :param selection: values to select.
        :returns: a :py:class:`Select` query instance.

    .. py:method:: insert([data=None[, columns=None[, on_conflict=None[, **kwargs]]]])

        Create a :py:class:`Insert` query into the given table. Data to be
        inserted can be provided in a number of different formats:

        * a dictionary of table columns to values
        * keyword arguments of column names to values
        * a list/tuple/iterable of dictionaries
        * a :py:class:`Select` query instance.

        .. note::
            When providing a :py:class:`Select` query, it is necessary to also
            provide a list of columns.

            It is also advisable to provide a list of columns when supplying a
            list or iterable of rows to insert.

        Simple insert example:

        .. code-block:: python

            User = Table('users', ('id', 'username', 'is_admin'))

            # Simple inserts.
            query = User.insert({User.username: 'huey', User.is_admin: False})

            # Equivalent to above.
            query = User.insert(username='huey', is_admin=False)

        Inserting multiple rows:

        .. code-block:: python

            # Inserting multiple rows of data.
            data = (
                {User.username: 'huey', User.is_admin: False},
                {User.username: 'mickey', User.is_admin: True})
            query = User.insert(data)

            # Equivalent to above
            query = User.insert(data, columns=(User.username, User.is_admin))

        Inserting using a SELECT query:

        .. code-block:: python

            Person = Table('person', ('id', 'name'))

            query = User.insert(
                Person.select(Person.name, False),
                columns=(User.username, User.is_admin))

            # Results in:
            # INSERT INTO "users" ("username", "is_admin")
            # SELECT "person"."name", false FROM "person";

    .. py:method:: update([data=None[, on_conflict=None[, **kwargs]]])

        Create a :py:class:`Update` query for the given table. Update can be
        provided as a dictionary keyed by column, or using keyword arguments.

        Examples:

        .. code-block:: python

            User = Table('users', ('id', 'username', 'is_admin'))

            query = (User
                     .update({User.is_admin: False})
                     .where(User.username == 'huey'))

            # Equivalent to above:
            query = User.update(is_admin=False).where(User.username == 'huey')

        Example of an atomic update:

        .. code-block:: python

            PageView = Table('pageviews', ('url', 'view_count'))

            query = (PageView
                     .update({PageView.view_count: PageView.view_count + 1})
                     .where(PageView.url == some_url))

    .. py:method:: delete()

        Create a :py:class:`Delete` query for the given table.

        Example:

        .. code-block:: python

            query = User.delete().where(User.c.account_expired == True)

    .. py:method:: filter(**kwargs)

        Perform a :py:class:`Select` query, filtering the result set using
        keyword arguments to represent filter expressions. All expressions are
        combined using ``AND``.

        This method is provided as a convenience API.

        Example:

        .. code-block:: python

            Person = Table('person', ('id', 'name', 'dob'))

            today = datetime.date.today()
            eighteen_years_ago = today - datetime.timedelta(years=18)
            adults = Person.filter(dob__gte=eighteen_years_ago)

    .. py:method:: rank()

        Convenience method for representing an expression which calculates the
        rank of search results.

        Example:

        .. code-block:: python

            NoteIdx = Table('note_idx', ('docid', 'content'))
            rank = NoteIdx.rank()
            query = (NoteIdx
                     .select(NoteIdx.docid, NoteIdx.content, rank.alias('score'))
                     .where(NoteIdx.match('search query'))
                     .order_by(rank)
                     .namedtuples())

            for search_result in query.execute(database):
                print search_result.score, search_result.content

    .. py:method:: bm25()

        Convenience method for representing an expression which calculates the
        rank of search results using the BM25 algorithm. Usage is identical to
        :py:meth:`~Table.rank`.

    .. py:method:: match(search_term)

        Convenience method for generating an expression that corresponds to a
        search on a full-text search virtual table. For an example of usage,
        see :py:meth:`~Table.rank`.


.. py:class:: BoundTable(database, name[, columns=None[, schema=None[, alias=None]]])

    Identical to :py:class:`Table` with the exception that any queries on the
    table will automatically be bound to the provided database.

    With an ordinary :py:class:`Table` object, you would write the following:

    .. code-block:: python

        User = Table('users', ('id', 'username'))
        query = User.select().namedtuples()
        for user in query.execute(database):
            print user.username

    With a bound table, you can instead write:

    .. code-block:: python

        BoundUser = User.bind(database)
        query = User.select().namedtuples()
        for user in query.execute():
            print user.username

        # Or, even simpler, since bound select queries implement __iter__:
        for user in query:
            print user.username
