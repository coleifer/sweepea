![](http://media.charlesleifer.com/blog/photos/sweepea-fast.png)

## swee'pea

Fast, lightweight Python database toolkit for SQLite, built with Cython.

Like it's cousin `peewee <http://docs.peewee-orm.com/>`_, ``swee'pea`` is
comprised of a database connection abstraction and query-building / execution
APIs. This project is a pet project of mine, so tailor expectations
accordingly.

Features:

* Implemented in Cython for performance and to expose advanced features of the
  SQLite database library.
* Composable and consistent APIs for building queries using Python.
* Layered APIs allow you to work as close to the database as you want.
* No magic.
* No bullshit.

Issue tracker and code are hosted on GitHub: https://github.com/coleifer/sweepea.

Documentation hosted on RT**F**D: https://sweepea.readthedocs.io/

### Dependencies

Cython.

This project is designed to work with the standard library `sqlite3` driver, or
alternatively, the latest version of `pysqlite2`.

### Installation

First install Cython and ensure that you have a SQLite library (standard
library ``sqlite3`` or `pysqlite <https://github.com/ghaering/pysqlite>`_).
Then:

```
$ pip install sweepea
```

Or

```
$ pip install -e git+https://github.com/coleifer/sweepea#egg=sweepea
```

-----------------------------------------------------------------

## Database Helper

## Dynamic Tables

SQLite makes it easy to define scalar and aggregate functions, but it is more
challenging to create functions that return multiple values. Scalar functions
accept zero or more parameters and return a single value. Aggregate functions
accept parameters from any number of input rows, and then generate a final
scalar value.

To create functions that return multiple values, it is necessary to create a
[virtual table](http://sqlite.org/vtab.html). SQLite has the concept of
"eponymous" virtual tables, which are virtual tables that can be called like a
function and do not require explicit creation using DDL statements.

The `vtfunc` module abstracts away the complexity of creating an eponymous
virtual table, allowing you to write your own multi-value SQLite functions in
Python.

### Example

Suppose we want to create a function that, given a regular expression and an
input string, returns all matching subgroups in the input string. For instance,
if our regex was `'[0-9]+'` and our input string was `'123 xxx 456 yyy
789 zzz 0'`, the function should return four rows:

* `123`
* `456`
* `789`
* `0`

With the `vtab` module it is very easy to implement this:

```python
import re

from vtfunc import TableFunction


class RegexSearch(TableFunction):
    params = ['regex', 'search_string']
    columns = ['match']
    name = 'regex_search'

    def initialize(self, regex=None, search_string=None):
        self._iter = re.finditer(regex, search_string)

    def iterate(self, idx):
        # We do not need `idx`, so just ignore it.
        return (next(self._iter).group(0),)
```

To use our function, we need to register the module with a SQLite connection,
then call it using a `SELECT` query:

```python

import sqlite3

conn = sqlite3.connect(':memory:')  # Create an in-memory database.

RegexSearch.register(conn)  # Register our module.

query_params = ('[0-9]+', '123 xxx 456 yyy 789 zzz 0')
cursor = conn.execute('SELECT * FROM regex_search(?, ?);', query_params)
print cursor.fetchall()
```

Let's say we have a table that contains a list of arbitrary messages and we
want to capture all the e-mail addresses from that table. This is also easy
using our table-valued function. We will query the `messages` table and pass
the message body into our table-valued function. Then, for each email address
we find, we'll return a row containing the message ID and the matching email
address:

```python

email_regex = '[\w]+@[\w]+\.[\w]{2,3}'  # Stupid simple email regex.
query = ('SELECT messages.id, regex_search.match '
         'FROM messages, regex_search(?, messages.body)')
cursor = conn.execute(query, (email_regex,))
```

The resulting rows will look something like:

```

message id |         email
-----------+-----------------------
     1     | charlie@example.com
     1     | huey@kitty.cat
     1     | zaizee@morekitties.cat
     3     | mickey@puppies.dog
     3     | huey@throwaway.cat
    ...    |         ...
```

#### Important note

In the above example you will note that the parameters for our query actually
change (because each row in the messages table has a different search string).
This means that for this particular query, the `RegexSearch.initialize()`
function will be called once for each row in the `messages` table.

### How it works

Behind-the-scenes, `vtfunc` is creating a [Virtual Table](http://sqlite.org/vtab.html)
and filling in the various callbacks with wrappers around your user-defined
function. There are two important methods that the wrapped virtual table
implements:

* xBestIndex
* xFilter

When SQLite attempts to execute a query, it will call the xBestIndex method of
the virtual table (possibly multiple times) trying to come up with the best
query plan. The `vtfunc` module optimizes for those query plans which include
values for all the parameters of the user-defined function. Since some
user-defined functions may have optional parameters, query plans with only a
subset of param values will be slightly penalized.

Since we have no visibility into what parameters the user *actually* passed in,
and we don't know ahead of time which query plan SQLite suggests will be
best, `vtfunc` just does its best to optimize for plans with the highest
number of usable parameter values.

If you encounter a situation where you pass your function multiple parameters,
but it doesn't receive all of them, it's the case that a less-than-optimal
plan was used.

After the plan is chosen by calling xBestIndex, the query will execute by
calling xFilter (possibly multiple times). xFilter has access to the actual
query parameters, and it's responsibility is to initialize the cursor and call
the user's initialize() callback with the parameters passed in.

## Query Builder

The query builder is designed to allow users to construct queries with reusable
Python objects. Instead of working with string query fragments, you can
construct queries using more Pythonic components which are then compiled into
SQL.

The query-builder is designed with consistency and composability as the primary
goals. Consistency enables one to learn once, then apply everywhere, while
composability ensures that large systems can be built one piece at a time, from
the bottom-up.

Example of constructing a simple query:

```python

db = Database('app.db')

Employee = Table('employees', ('id', 'name', 'start_date', 'manager_id'))

# Get list of employees and their manager's name, sorted by tenure.
Manager = Employee.alias('manager')
query = (Employee
         .select(
             Employee.name,
             Employee.start_date,
             Manager.name.alias('manager_name'))
         .join(Manager, JOIN.LEFT_OUTER, on=(Employee.manager_id == Manager.id))
         .order_by(Employee.start_date))

for row in query.execute(db):
    print row['name'], row['start_date'], (row['manager_name'] or 'no mgr')
```

Unlike an ORM, the query builder has no opinions on your data-model, nor does
it encourage inefficiency (e.g. the [n+1 problem](http://docs.peewee-orm.com/en/latest/peewee/querying.html#avoiding-n-1-queries)).
The query builder can be integrated into an already-running system with hardly
any code needing to be written.
