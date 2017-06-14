.. _usage:

Usage
=====

``swee'pea`` is designed to be simple-to-use, composable, consistent and
extensible. That's a lot of unhelpful adjectives, so let's get into it.

Database
--------

Everything begins with a database, which in our case is a `sqlite <http://www.sqlite.org>`_
database. The :py:class:`Database` object provides per-thread connection
management, and APIs for executing queries, managing transactions, and
introspecting the database.

Here's a fairly standard database declaration. We're instructing the database
to store data in a file named ``app.db``, and to additionally set two
`PRAGMAs <http://sqlite.org/pragma.html>`_ each time a connection is opened.
`WAL-mode <http://sqlite.org/wal.html>`_ is often desirable because it allows
multiple readers to coexist with a single writer.

.. code-block:: python

    database = Database('app.db', pragmas=(
        ('cache_size', 4000),
        ('journal_mode', 'wal')))

The database supports methods for opening and closing connections. These
methods are thread-safe.

.. code-block:: python

    database.connect()
    # Do some stuff.
    database.close()

To execute a SQL query, we can call the :py:meth:`~Database.execute_sql`
method.

.. code-block:: python

    # Here we use the database as a context manager. The enclosed block will be
    # executed in a transaction, and the connection closed afterwards.
    with database:
        database.execute_sql('CREATE TABLE users ('
                             'id INTEGER NOT NULL PRIMARY KEY, '
                             'username TEXT NOT NULL)')

Transaction management
^^^^^^^^^^^^^^^^^^^^^^

``swee'pea`` provides context-managers and decorators to manage transactions in
your application code. For most use-cases, you can simply use the
:py:meth:`~Database.atomic` method, which wraps the outer-most block in a
transaction, and uses save-points for any subsequent nested blocks.

Example:

.. code-block:: python

    with database.atomic() as txn:
        modify_some_data()
        with database.atomic() as savepoint:
            if modify_some_more_data() == -1:
                # Oh shit. Better back out the changes made by
                # modify_some_more_data().
                savepoint.rollback()

        # Only the data modified in the savepoint block has been rolled-back.
        # The changes made by modify_some_data() will still be intact.

    @database.atomic()
    def transfer_money(from_acct, to_acct, amount):
        # The changes made here will be executed in a transaction/savepoint.
        # If an exception occurs the transaction is automatically rolled-back.
        ...

Python extensions to SQLite
^^^^^^^^^^^^^^^^^^^^^^^^^^^

Since SQLite runs in the same address space as your application, we can write
extensions in Python and then call them from our SQL queries. ``swee'pea``
provides decorators to expose the hooks SQLite supports. Here is an example of
using user-defined functions:

.. code-block:: python

    import socket, struct

    @database.func()
    def ip_to_int(ip):
        return struct.unpack('!I', socket.inet_aton(ip))[0]

    @database.func()
    def int_to_ip(i):
        return socket.inet_ntoa(struct.pack('!I', i))

    # To conserve space, we want to store IP addresses as integers.
    database.execute_sql('INSERT INTO page_views (ip, url) VALUES '
                         '(ip_to_int(?), ?)',
                         ('1.2.3.4', '/blog/'))

    curs = database.execute_sql('SELECT int_to_ip(ip), url '
                                'FROM page_views '
                                'WHERE ip = ip_to_int(?)',
                                ('1.2.3.4',))
    for ip, url in curs.fetchall():
        print ip, url

For more information, check out the following APIs:

* :py:meth:`~Database.func` - scalar function
* :py:meth:`~Database.aggregate` - aggregate function (many rows, one output)
* :py:meth:`~Database.collation` - defines how to order two values
* :py:meth:`~Database.table_function` - produces tabular output (virtual table)
