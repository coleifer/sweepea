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

    .. py:attr:: columns

        A list or tuple describing the rows returned by this function.

    .. py:attr:: params

        A list or tuple describing the parameters this function accepts.

    .. py:attr:: name

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
