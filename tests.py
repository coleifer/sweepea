from collections import Counter
from collections import OrderedDict
from contextlib import contextmanager
import datetime
import logging
import os
import re
import sys
import unittest

import sqlite3
from sweepea import Context
from sweepea import Database
from sweepea import DoesNotExist
from sweepea import fn
from sweepea import SQL
from sweepea import Table
from sweepea import TableFunction


O = lambda *a: OrderedDict(a)


class Series(TableFunction):
    columns = ['value']
    params = ['start', 'stop', 'step']
    name = 'series'

    def initialize(self, start=0, stop=None, step=1):
        self.start = start
        self.stop = stop or float('inf')
        self.step = step
        self.curr = self.start

    def iterate(self, idx):
        if self.curr > self.stop:
            raise StopIteration

        ret = self.curr
        self.curr += self.step
        return (ret,)


class RegexSearch(TableFunction):
    columns = ['match']
    params = ['regex', 'search_string']
    name = 'regex_search'

    def initialize(self, regex=None, search_string=None):
        if regex and search_string:
            self._iter = re.finditer(regex, search_string)
        else:
            self._iter = None

    def iterate(self, idx):
        # We do not need `idx`, so just ignore it.
        if self._iter is None:
            raise StopIteration
        else:
            return (next(self._iter).group(0),)


class Split(TableFunction):
    params = ['data']
    columns = ['part']
    name = 'str_split'

    def initialize(self, data=None):
        self._parts = data.split()
        self._idx = 0

    def iterate(self, idx):
        if self._idx < len(self._parts):
            result = (self._parts[self._idx],)
            self._idx += 1
            return result
        raise StopIteration


logger = logging.getLogger('sweepea')


class QueryLogHandler(logging.Handler):
    def __init__(self, *args, **kwargs):
        self.queries = []
        logging.Handler.__init__(self, *args, **kwargs)

    def emit(self, record):
        self.queries.append(record)


class BaseTestCase(unittest.TestCase):
    def setUp(self):
        self._qh = QueryLogHandler()
        logger.setLevel(logging.DEBUG)
        logger.addHandler(self._qh)

    def tearDown(self):
        logger.removeHandler(self._qh)

    def assertSQL(self, query, sql, params=None):
        params = [] if params is None else params
        qsql, qparams = Context().parse(query)
        self.assertEqual(qsql, sql)
        if params is not None:
            self.assertEqual(qparams, params)

    @property
    def history(self):
        return self._qh.queries

    @contextmanager
    def assertQueryCount(self, num):
        qc = len(self.history)
        yield
        self.assertEqual(len(self.history) - qc, num)


class TestTableFunction(BaseTestCase):
    def setUp(self):
        super(TestTableFunction, self).setUp()
        self.conn = sqlite3.connect(':memory:')

    def tearDown(self):
        super(TestTableFunction, self).tearDown()
        self.conn.close()

    def test_split(self):
        Split.register(self.conn)
        curs = self.conn.execute('select part from str_split(?) order by part '
                                 'limit 3', ('well hello huey and zaizee',))
        self.assertEqual([row for row, in curs.fetchall()],
                         ['and', 'hello', 'huey'])

    def test_split_tbl(self):
        Split.register(self.conn)
        self.conn.execute('create table post (content TEXT);')
        self.conn.execute('insert into post (content) values (?), (?), (?)',
                          ('huey secret post',
                           'mickey message',
                           'zaizee diary'))
        curs = self.conn.execute('SELECT * FROM post, str_split(post.content)')
        results = curs.fetchall()
        self.assertEqual(results, [
            ('huey secret post', 'huey'),
            ('huey secret post', 'secret'),
            ('huey secret post', 'post'),
            ('mickey message', 'mickey'),
            ('mickey message', 'message'),
            ('zaizee diary', 'zaizee'),
            ('zaizee diary', 'diary'),
        ])

    def test_series(self):
        Series.register(self.conn)

        def assertSeries(params, values, extra_sql=''):
            param_sql = ', '.join('?' * len(params))
            sql = 'SELECT * FROM series(%s)' % param_sql
            if extra_sql:
                sql = ' '.join((sql, extra_sql))
            curs = self.conn.execute(sql, params)
            self.assertEqual([row for row, in curs.fetchall()], values)

        assertSeries((0, 10, 2), [0, 2, 4, 6, 8, 10])
        assertSeries((5, None, 20), [5, 25, 45, 65, 85], 'LIMIT 5')
        assertSeries((4, 0, -1), [4, 3, 2], 'LIMIT 3')
        assertSeries((3, 5, 3), [3])
        assertSeries((3, 3, 1), [3])

    def test_series_tbl(self):
        Series.register(self.conn)
        self.conn.execute('CREATE TABLE nums (id INTEGER PRIMARY KEY)')
        self.conn.execute('INSERT INTO nums DEFAULT VALUES;')
        self.conn.execute('INSERT INTO nums DEFAULT VALUES;')
        curs = self.conn.execute(
            'SELECT * FROM nums, series(nums.id, nums.id + 2)')
        results = curs.fetchall()
        self.assertEqual(results, [
            (1, 1), (1, 2), (1, 3),
            (2, 2), (2, 3), (2, 4)])

        curs = self.conn.execute(
            'SELECT * FROM nums, series(nums.id) LIMIT 3')
        results = curs.fetchall()
        self.assertEqual(results, [(1, 1), (1, 2), (1, 3)])

    def test_regex(self):
        RegexSearch.register(self.conn)

        def assertResults(regex, search_string, values):
            sql = 'SELECT * FROM regex_search(?, ?)'
            curs = self.conn.execute(sql, (regex, search_string))
            self.assertEqual([row for row, in curs.fetchall()], values)

        assertResults(
            '[0-9]+',
            'foo 123 45 bar 678 nuggie 9.0',
            ['123', '45', '678', '9', '0'])
        assertResults(
            '[\w]+@[\w]+\.[\w]{2,3}',
            ('Dear charlie@example.com, this is nug@baz.com. I am writing on '
             'behalf of zaizee@foo.io. He dislikes your blog.'),
            ['charlie@example.com', 'nug@baz.com', 'zaizee@foo.io'])
        assertResults(
            '[a-z]+',
            '123.pDDFeewXee',
            ['p', 'eew', 'ee'])
        assertResults(
            '[0-9]+',
            'hello',
            [])

    def test_regex_tbl(self):
        messages = (
            'hello foo@example.fap, this is nuggie@example.fap. How are you?',
            'baz@example.com wishes to let charlie@crappyblog.com know that '
            'huey@example.com hates his blog',
            'testing no emails.',
            '')
        RegexSearch.register(self.conn)

        self.conn.execute('create table posts (id integer primary key, msg)')
        self.conn.execute('insert into posts (msg) values (?), (?), (?), (?)',
                          messages)
        cur = self.conn.execute('select posts.id, regex_search.rowid, regex_search.match '
                                'FROM posts, regex_search(?, posts.msg)',
                                ('[\w]+@[\w]+\.\w{2,3}',))
        results = cur.fetchall()
        self.assertEqual(results, [
            (1, 1, 'foo@example.fap'),
            (1, 2, 'nuggie@example.fap'),
            (2, 3, 'baz@example.com'),
            (2, 4, 'charlie@crappyblog.com'),
            (2, 5, 'huey@example.com'),
        ])


class BaseDatabaseTestCase(BaseTestCase):
    __db_filename__ = ':memory:'

    def setUp(self):
        super(BaseDatabaseTestCase, self).setUp()
        self._db = Database(self.__db_filename__, rank_functions=True,
                            hash_functions=True)
        self._db.connect()

    def tearDown(self):
        super(BaseDatabaseTestCase, self).tearDown()
        self._db.close()
        if self.__db_filename__ != ':memory:' and \
           os.path.exists(self.__db_filename__):
            os.unlink(self.__db_filename__)

    def execute(self, sql, *params):
        return self._db.execute_sql(sql, params, commit=False)


class TestHelpers(BaseDatabaseTestCase):
    __db_filename__ = 'test.db'

    def test_autocommit(self):
        self.assertTrue(self._db.autocommit)
        self.execute('begin')
        self.assertFalse(self._db.autocommit)
        self.execute('rollback')

    def test_commit_hook(self):
        state = {}

        @self._db.on_commit
        def on_commit():
            state.setdefault('commits', 0)
            state['commits'] += 1

        self.execute('create table register (value text)')
        self.assertEqual(state['commits'], 1)

        # Check hook is preserved.
        self._db.close()
        self._db.connect()

        self.execute('insert into register (value) values (?), (?)',
                     'foo', 'bar')
        self.assertEqual(state['commits'], 2)

        curs = self.execute('select * from register order by value;')
        results = curs.fetchall()
        self.assertEqual([tuple(r) for r in results], [('bar',), ('foo',)])

        self.assertEqual(state['commits'], 2)

    def test_rollback_hook(self):
        state = {}

        @self._db.on_rollback
        def on_rollback():
            state.setdefault('rollbacks', 0)
            state['rollbacks'] += 1

        self.execute('create table register (value text);')
        self.assertEqual(state, {})

        # Check hook is preserved.
        self._db.close()
        self._db.connect()

        self.execute('begin;')
        self.execute('insert into register (value) values (?)', 'test')
        self.execute('rollback;')
        self.assertEqual(state, {'rollbacks': 1})

        curs = self.execute('select * from register;')
        self.assertEqual(curs.fetchall(), [])

    def test_update_hook(self):
        state = []

        @self._db.on_update
        def on_update(query, db, table, rowid):
            state.append((query, db, table, rowid))

        self.execute('create table register (value text)')
        self.execute('insert into register (value) values (?), (?)',
                     'foo', 'bar')

        self.assertEqual(state, [
            ('INSERT', 'main', 'register', 1),
            ('INSERT', 'main', 'register', 2)])

        # Check hook is preserved.
        self._db.close()
        self._db.connect()

        self.execute('update register set value = ? where rowid = ?', 'baz', 1)
        self.assertEqual(state, [
            ('INSERT', 'main', 'register', 1),
            ('INSERT', 'main', 'register', 2),
            ('UPDATE', 'main', 'register', 1)])

        self.execute('delete from register where rowid=?;', 2)
        self.assertEqual(state, [
            ('INSERT', 'main', 'register', 1),
            ('INSERT', 'main', 'register', 2),
            ('UPDATE', 'main', 'register', 1),
            ('DELETE', 'main', 'register', 2)])

    def test_udf(self):
        @self._db.func()
        def backwards(s):
            return s[::-1]

        @self._db.func()
        def titled(s):
            return s.title()

        query = self.execute('SELECT titled(backwards(?));', 'hello')
        result, = query.fetchone()
        self.assertEqual(result, 'Olleh')

        # Check hook is preserved.
        self._db.close()
        self._db.connect()

        query = self.execute('SELECT backwards(titled(?));', 'hello')
        result, = query.fetchone()
        self.assertEqual(result, 'olleH')

    def test_changes_rowid(self):
        self.execute('create table register (value)')
        self.assertEqual(self._db.changes(), 0)

        self.execute('insert into register (value) values (?), (?)', 'a', 'b')
        self.assertEqual(self._db.changes(), 2)
        self.assertEqual(self._db.last_insert_rowid(), 2)

        self._db.close()
        self._db.connect()

        self.execute('insert into register (value) values (?), (?)', 'c', 'd')
        self.assertEqual(self._db.changes(), 2)
        self.assertEqual(self._db.last_insert_rowid(), 4)

        self.execute('insert into register (value) values (?)', 'e')
        self.assertEqual(self._db.changes(), 1)
        self.assertEqual(self._db.last_insert_rowid(), 5)

        # Changes works with update queries.
        self.execute('update register set value=?', 'x')
        self.assertEqual(self._db.changes(), 5)
        self.assertEqual(self._db.last_insert_rowid(), 5)

        # Changes picks up rows deleted.
        self.execute('delete from register where rowid > ?', 2)
        self.assertEqual(self._db.changes(), 3)

    def test_aggregate(self):
        @self._db.aggregate()
        class Mode(object):
            def __init__(self):
                self.c = Counter()
            def step(self, value):
                self.c.update((value,))
            def finalize(self):
                return self.c.most_common(1)[0][0]

        self.execute('CREATE TABLE register (data INTEGER NOT NULL)')
        curs = self.execute('SELECT MODE(data) FROM register')
        self.assertEqual(curs.fetchone()[0], None)

        self.execute('INSERT INTO register (data) VALUES (?),(?),(?),(?),(?)',
                     3, 1, 3, 3, 7)
        curs = self.execute('SELECT MODE(data) FROM register')
        self.assertEqual(curs.fetchone()[0], 3)

    def test_collation(self):
        @self._db.collation()
        def numeric(lhs, rhs):
            l1 = [int(t) if t.isdigit() else t
                  for t in re.split('(\d+)', lhs)]
            l2 = [int(t) if t.isdigit() else t
                  for t in re.split('(\d+)', rhs)]
            if l1 < l2:
                return -1
            elif l2 < l1:
                return 1
            return 0

        self.execute('CREATE TABLE register (data TEXT)')
        self.execute('INSERT INTO register (data) VALUES (?), (?), (?), (?)',
                     'foo10', 'foo2', 'foo1', 'foo4')
        curs = self.execute('SELECT data FROM register ORDER BY data COLLATE '
                            'numeric')
        self.assertEqual([row[0] for row in curs.fetchall()],
                         ['foo1', 'foo2', 'foo4', 'foo10'])


    def test_properties(self):
        mem_used, mem_high = self._db.memory_used
        self.assertTrue(mem_high >= mem_used)
        self.assertTrue(self._db.cache_used is not None)

    def test_transactions(self):
        def assertRegister(vals):
            data = [row[0] for row in self.execute('SELECT data FROM register '
                                                   'ORDER BY data ASC')]
            self.assertEqual(data, vals)

        def save(*vals):
            for val in vals:
                self.execute('INSERT INTO register (data) VALUES (?)', val)

        self.assertTrue(self._db.autocommit)
        self.execute('CREATE TABLE register (data TEXT)')

        with self._db.atomic():
            self.assertFalse(self._db.autocommit)
            save('k1')

        assertRegister(['k1'])

        with self._db.atomic() as txn:
            save('k2')
            txn.rollback()
            save('k3')
            with self._db.atomic() as sp1:
                save('k4')
                with self._db.atomic() as sp2:
                    save('k5')
                    sp2.rollback()
                with self._db.atomic() as sp3:
                    save('k6')
                    with self._db.atomic() as sp4:
                        save('k7')
                        with self._db.atomic() as sp5:
                            save('k8')
                        assertRegister(['k1', 'k3', 'k4', 'k6', 'k7', 'k8'])
                        sp4.rollback()

                    assertRegister(['k1', 'k3', 'k4', 'k6'])

        assertRegister(['k1', 'k3', 'k4', 'k6'])
        self.execute('DELETE FROM register')
        assertRegister([])

        with self._db.transaction() as txn:
            save('k1')
            with self._db.transaction() as txn2:
                save('k2')
                txn2.rollback()  # Actually issues a rollback.
                assertRegister([])
            save('k3')
        assertRegister(['k3'])

    def test_pragmas(self):
        self.assertEqual(self._db.page_size, 4096)
        self._db.page_size = 1024
        self.assertEqual(self._db.page_size, 1024)

        self._db.foreign_keys = 'on'
        self.assertEqual(self._db.foreign_keys, 1)
        self._db.foreign_keys = 'off'
        self.assertEqual(self._db.foreign_keys, 0)

    def test_db_context(self):
        db = Database(':memory:')
        self.assertTrue(db.is_closed())

        db.connect()
        self.assertFalse(db.is_closed())
        self.assertRaises(sqlite3.OperationalError, db.connect)

        self.assertTrue(db.close())
        self.assertFalse(db.close())
        self.assertTrue(db.is_closed())

        with db:
            self.assertFalse(db.is_closed())
            self.assertFalse(db.autocommit)

        self.assertTrue(db.is_closed())

    def test_connection_initialization(self):
        state = {'count': 0}
        class TestDatabase(Database):
            def initialize_connection(self, conn):
                state['count'] += 1
        db = TestDatabase(':memory:')
        self.assertEqual(state['count'], 0)

        conn = db.connection()
        self.assertEqual(state['count'], 1)

        # Since already connected, nothing happens here.
        conn = db.connection()
        self.assertEqual(state['count'], 1)

    def test_connect_semantics(self):
        state = {'count': 0}
        class TestDatabase(Database):
            def initialize_connection(self, conn):
                state['count'] += 1
        db = TestDatabase(':memory:')

        db.connect()
        self.assertEqual(state['count'], 1)
        self.assertRaises(sqlite3.OperationalError, db.connect)
        self.assertEqual(state['count'], 1)

        self.assertFalse(db.connect(reuse_if_open=True))
        self.assertEqual(state['count'], 1)

        with db:
            self.assertEqual(state['count'], 1)
            self.assertFalse(db.is_closed())

        self.assertTrue(db.is_closed())
        with db:
            self.assertEqual(state['count'], 2)

    def test_execute_sql(self):
        db = Database(':memory:')
        with db:
            db.execute_sql('CREATE TABLE register (val INTEGER);')
            db.execute_sql('INSERT INTO register (val) VALUES (?), (?)',
                                      (1337, 31337))
            cursor = db.execute_sql(
                'SELECT val FROM register ORDER BY val')
            self.assertEqual(cursor.fetchall(), [(1337,), (31337,)])
            db.execute_sql('DROP TABLE register;')

    def test_sqlite_isolation(self):
        self._db.execute_sql('CREATE TABLE users ('
                             'id INTEGER NOT NULL PRIMARY KEY, '
                             'username TEXT NOT NULL)')
        for username in ('u1', 'u2', 'u3'):
            User.insert({User.c.username: username}).execute(self._db)

        new_db = Database(self.__db_filename__)
        curs = new_db.execute_sql('SELECT COUNT(*) FROM users')
        self.assertEqual(curs.fetchone()[0], 3)

        user_count = User.select(User.c.username).count(self._db)
        self.assertEqual(user_count, 3)
        self.assertEqual(User.delete().execute(self._db), 3)

        with self._db.atomic():
            User.insert(({User.c.username: 'u4'},
                         {User.c.username: 'u5'})).execute(self._db)

            # Second conn does not see the changes.
            curs = new_db.execute_sql('SELECT COUNT(*) FROM users')
            self.assertEqual(curs.fetchone()[0], 0)

            # Third conn does not see the changes.
            new_db2 = Database(self.__db_filename__)
            curs = new_db2.execute_sql('SELECT COUNT(*) FROM users')
            self.assertEqual(curs.fetchone()[0], 0)

            # Original connection sees its own changes.
            query = User.select(User.c.id)
            self.assertEqual(query.count(self._db), 2)

        curs = new_db.execute_sql('SELECT COUNT(*) FROM users')
        self.assertEqual(curs.fetchone()[0], 2)


class TestIntrospection(BaseDatabaseTestCase):
    ddl = """
        CREATE TABLE "person" (
            "id" INTEGER NOT NULL PRIMARY KEY,
            "first" TEXT NOT NULL,
            "last" TEXT NOT NULL,
            "dob" DATE NOT NULL);
        CREATE UNIQUE INDEX "person_name_dob" ON "person" (
            "first", "last", "dob");
        CREATE INDEX "person_name" ON "person" ("last", "first");
        CREATE INDEX "person_dob" ON "person" ("dob" DESC);

        CREATE TABLE "note" (
            "id" INTEGER NOT NULL PRIMARY KEY,
            "person_id" INTEGER NOT NULL,
            "content" TEXT NOT NULL,
            "timestamp" INTEGER NOT NULL,
            FOREIGN KEY ("person_id") REFERENCES "person" ("id"));
        CREATE INDEX "note_person_id" ON "note" ("person_id");

        CREATE TABLE "relationship" (
            "from_person_id" INTEGER NOT NULL,
            "to_person_id" INTEGER NOT NULL,
            "flags" INTEGER,
            PRIMARY KEY ("from_person_id", "to_person_id"),
            FOREIGN KEY ("from_person_id") REFERENCES "person" ("id"),
            FOREIGN KEY ("to_person_id") REFERENCES "person" ("id")
            ) WITHOUT ROWID;

        CREATE TABLE "note_tag" (
            "note_id" INTEGER NOT NULL,
            "tag" TEXT NOT NULL COLLATE NOCASE DEFAULT '',
            PRIMARY KEY ("note_id", "tag"),
            FOREIGN KEY ("note_id") REFERENCES "note" ("id")
            ) WITHOUT ROWID;
        """

    def setUp(self):
        super(TestIntrospection, self).setUp()
        self._db.foreign_keys = 'on'
        self._db.connection().executescript(self.ddl)

    def test_get_tables(self):
        self.assertEqual(self._db.get_tables(), [
            'note',
            'note_tag',
            'person',
            'relationship'])

        self._db.execute_sql('DROP TABLE "note";')
        self._db.execute_sql('DROP TABLE "note_tag";')
        self.assertEqual(self._db.get_tables(), [ 'person', 'relationship'])

    def test_get_indexes(self):
        indexes = self._db.get_indexes('person')
        without_sql = [(index[0], index[2], index[3]) for index in indexes]
        self.assertEqual(without_sql, [
            ('person_dob', ['dob'], False),
            ('person_name', ['last', 'first'], False),
            ('person_name_dob', ['first', 'last', 'dob'], True)])

        indexes = self._db.get_indexes('note')
        self.assertEqual(indexes, [
            ('note_person_id',
             'CREATE INDEX "note_person_id" ON "note" ("person_id")',
             ['person_id'],
             False)])

    def test_get_columns(self):
        self.assertEqual(self._db.get_columns('person'), [
            ('id', 'INTEGER', False, True),
            ('first', 'TEXT', False, False),
            ('last', 'TEXT', False, False),
            ('dob', 'DATE', False, False)])

        self.assertEqual(self._db.get_columns('note_tag'), [
            ('note_id', 'INTEGER', False, True),
            ('tag', 'TEXT', False, True)])

    def test_get_primary_keys(self):
        self.assertEqual(self._db.get_primary_keys('person'), ['id'])
        self.assertEqual(self._db.get_primary_keys('relationship'),
                         ['from_person_id', 'to_person_id'])
        self.assertEqual(self._db.get_primary_keys('note_tag'),
                         ['note_id', 'tag'])

    def test_get_foreign_keys(self):
        self.assertEqual(self._db.get_foreign_keys('person'), [])
        self.assertEqual(self._db.get_foreign_keys('note'), [
            ('person_id', 'person', 'id')])
        self.assertEqual(sorted(self._db.get_foreign_keys('relationship')), [
            ('from_person_id', 'person', 'id'),
            ('to_person_id', 'person', 'id')])
        self.assertEqual(sorted(self._db.get_foreign_keys('note_tag')), [
            ('note_id', 'note', 'id')])


#User = Table('users', ('id', 'username', 'is_admin'))
#Tweet = Table('tweet', ('id', 'user_id', 'content', 'timestamp'))
User = Table('users')
Tweet = Table('tweets')
Person = Table('person', ['id', 'name', 'dob'])
Note = Table('note', ['id', 'person_id', 'content'])

def __sql__(query):
    return Context().parse(query)


class TestQueryBuilder(BaseTestCase):
    def test_select(self):
        query = (User
                 .select(User.c.id, User.c.username)
                 .where(User.c.username == 'foo'))
        self.assertSQL(query, (
            'SELECT "users"."id", "users"."username" '
            'FROM "users" '
            'WHERE ("users"."username" = ?)'), ['foo'])

    def test_select_explicit_columns(self):
        query = (Person
                 .select()
                 .where(Person.dob < datetime.date(1980, 1, 1)))
        self.assertSQL(query, (
            'SELECT "person"."id", "person"."name", "person"."dob" '
            'FROM "person" '
            'WHERE ("person"."dob" < ?)'), [datetime.date(1980, 1, 1)])

    def test_from_clause(self):
        query = (Note
                 .select(Note.content, Person.name)
                 .from_(Note, Person)
                 .where(Note.person_id == Person.id)
                 .order_by(Note.id))
        self.assertSQL(query, (
            'SELECT "note"."content", "person"."name" '
            'FROM "note", "person" '
            'WHERE ("note"."person_id" = "person"."id") '
            'ORDER BY "note"."id"'), [])

    def test_from_query(self):
        inner = Person.select(Person.name).alias('i1')
        query = (Person
                 .select(inner.c.name)
                 .from_(inner))
        self.assertSQL(query, (
            'SELECT "i1"."name" '
            'FROM (SELECT "person"."name" FROM "person") AS "i1"'), [])

        PA = Person.alias('pa')
        inner = PA.select(PA.name).alias('i1')
        query = (Person
                 .select(inner.c.name)
                 .from_(inner)
                 .order_by(inner.c.name))
        self.assertSQL(query, (
            'SELECT "i1"."name" '
            'FROM (SELECT "pa"."name" FROM "person" AS "pa") AS "i1" '
            'ORDER BY "i1"."name"'), [])

    def test_join_explicit_columns(self):
        query = (Note
                 .select(Note.content)
                 .join(Person, on=(Note.person_id == Person.id))
                 .where(Person.name == 'charlie')
                 .order_by(Note.id.desc()))
        self.assertSQL(query, (
            'SELECT "note"."content" '
            'FROM "note" '
            'INNER JOIN "person" ON ("note"."person_id" = "person"."id") '
            'WHERE ("person"."name" = ?) '
            'ORDER BY "note"."id" DESC'), ['charlie'])

    def test_multi_join(self):
        Like = Table('likes')
        LikeUser = User.alias('lu')
        query = (Like
                 .select(Tweet.c.content, User.c.username, LikeUser.c.username)
                 .join(Tweet, on=(Like.c.tweet_id == Tweet.c.id))
                 .join(User, on=(Tweet.c.user_id == User.c.id))
                 .join(LikeUser, on=(Like.c.user_id == LikeUser.c.id))
                 .where(LikeUser.c.username == 'charlie')
                 .order_by(Tweet.c.timestamp))
        self.assertSQL(query, (
            'SELECT "tweets"."content", "users"."username", "lu"."username" '
            'FROM "likes" '
            'INNER JOIN "tweets" ON ("likes"."tweet_id" = "tweets"."id") '
            'INNER JOIN "users" ON ("tweets"."user_id" = "users"."id") '
            'INNER JOIN "users" AS "lu" ON ("likes"."user_id" = "lu"."id") '
            'WHERE ("lu"."username" = ?) '
            'ORDER BY "tweets"."timestamp"'), ['charlie'])

    def test_correlated_subquery(self):
        Employee = Table('employee', ['id', 'name', 'salary', 'dept'])
        EA = Employee.alias('e2')
        query = (Employee
                 .select(Employee.id, Employee.name)
                 .where(Employee.salary > (EA
                                           .select(fn.AVG(EA.salary))
                                           .where(EA.dept == Employee.dept))))
        self.assertSQL(query, (
            'SELECT "employee"."id", "employee"."name" '
            'FROM "employee" '
            'WHERE ("employee"."salary" > ('
            'SELECT AVG("e2"."salary") '
            'FROM "employee" AS "e2" '
            'WHERE ("e2"."dept" = "employee"."dept")))'), [])

    def test_multiple_where(self):
        query = (Person
                 .select(Person.name)
                 .where(Person.dob < datetime.date(1980, 1, 1))
                 .where(Person.dob > datetime.date(1950, 1, 1)))
        self.assertSQL(query, (
            'SELECT "person"."name" '
            'FROM "person" '
            'WHERE (("person"."dob" < ?) AND ("person"."dob" > ?))'),
            [datetime.date(1980, 1, 1), datetime.date(1950, 1, 1)])

    def test_simple_join(self):
        query = (User
                 .select(
                     User.c.id,
                     User.c.username,
                     fn.COUNT(Tweet.c.id).alias('ct'))
                 .join(Tweet, on=(Tweet.c.user_id == User.c.id))
                 .group_by(User.c.id, User.c.username))
        self.assertSQL(query, (
            'SELECT "users"."id", "users"."username", '
            'COUNT("tweets"."id") AS "ct" '
            'FROM "users" '
            'INNER JOIN "tweets" ON ("tweets"."user_id" = "users"."id") '
            'GROUP BY "users"."id", "users"."username"'), [])

    def test_subquery(self):
        inner = (Tweet
                 .select(fn.COUNT(Tweet.c.id).alias('ct'))
                 .where(Tweet.c.user == User.c.id))
        query = (User
                 .select(User.c.username, inner.alias('iq'))
                 .order_by(User.c.username))
        self.assertSQL(query, (
            'SELECT "users"."username", '
            '(SELECT COUNT("tweets"."id") AS "ct" '
            'FROM "tweets" '
            'WHERE ("tweets"."user" = "users"."id")) AS "iq" '
            'FROM "users" ORDER BY "users"."username"'), [])

    def test_filter(self):
        query = User.filter(username__in=('charlie', 'huey', 'zaizee'))
        self.assertSQL(query.select(User.c.id), (
            'SELECT "users"."id" FROM "users" '
            'WHERE ("users"."username" IN (?, ?, ?))'),
            ['charlie', 'huey', 'zaizee'])

    def test_string_concatenation(self):
        P = Table('person', ['first', 'last', 'dob'])
        query = (P
                 .select((P.last + ', ' + P.first).alias('name'))
                 .order_by(SQL('name').desc()))
        self.assertSQL(query, (
            'SELECT (("person"."last" || ?) || "person"."first") AS "name" '
            'FROM "person" ORDER BY name DESC'), [', '])

    def test_user_defined_alias(self):
        UA = User.alias('alt')
        query = (User
                 .select(User.c.id, User.c.username, UA.c.nuggz)
                 .join(UA, on=(User.c.id == UA.c.id))
                 .order_by(UA.c.nuggz))
        self.assertSQL(query, (
            'SELECT "users"."id", "users"."username", "alt"."nuggz" '
            'FROM "users" '
            'INNER JOIN "users" AS "alt" ON ("users"."id" = "alt"."id") '
            'ORDER BY "alt"."nuggz"'), [])

    def test_complex_select(self):
        Order = Table('orders', columns=(
            'region',
            'amount',
            'product',
            'quantity'))

        regional_sales = (Order
                          .select(
                              Order.region,
                              fn.SUM(Order.amount).alias('total_sales'))
                          .group_by(Order.region)
                          .cte('regional_sales'))

        top_regions = (regional_sales
                       .select(regional_sales.c.region)
                       .where(regional_sales.c.total_sales > (
                           regional_sales.select(
                               fn.SUM(regional_sales.c.total_sales) / 10)))
                       .cte('top_regions'))

        query = (Order
                 .select(
                     Order.region,
                     Order.product,
                     fn.SUM(Order.quantity).alias('product_units'),
                     fn.SUM(Order.amount).alias('product_sales'))
                 .where(
                     Order.region << top_regions.select(top_regions.c.region))
                 .group_by(Order.region, Order.product)
                 .with_cte(regional_sales, top_regions))

        self.assertSQL(query, (
            'WITH "regional_sales" AS ('
            'SELECT "orders"."region", SUM("orders"."amount") AS "total_sales"'
            ' FROM "orders" '
            'GROUP BY "orders"."region"'
            '), '
            '"top_regions" AS ('
            'SELECT "regional_sales"."region" '
            'FROM "regional_sales" '
            'WHERE ("regional_sales"."total_sales" > '
            '(SELECT (SUM("regional_sales"."total_sales") / ?) '
            'FROM "regional_sales"))'
            ') '
            'SELECT "orders"."region", "orders"."product", '
            'SUM("orders"."quantity") AS "product_units", '
            'SUM("orders"."amount") AS "product_sales" '
            'FROM "orders" '
            'WHERE ('
            '"orders"."region" IN ('
            'SELECT "top_regions"."region" '
            'FROM "top_regions")'
            ') GROUP BY "orders"."region", "orders"."product"'), [10])

    def test_compound_select(self):
        lhs = User.select(User.c.id).where(User.c.username == 'charlie')
        rhs = User.select(User.c.username).where(User.c.admin == True)
        q2 = (lhs | rhs)
        UA = User.alias('U2')
        q3 = q2 | UA.select(UA.c.id).where(UA.c.superuser == False)

        self.assertSQL(q3, (
            'SELECT "users"."id" '
            'FROM "users" '
            'WHERE ("users"."username" = ?) '
            'UNION '
            'SELECT "users"."username" '
            'FROM "users" '
            'WHERE ("users"."admin" = ?) '
            'UNION '
            'SELECT "U2"."id" '
            'FROM "users" AS "U2" '
            'WHERE ("U2"."superuser" = ?)'), ['charlie', True, False])

    def test_join_on_query(self):
        inner = User.select(User.c.id).alias('j1')
        query = (Tweet
                 .select(Tweet.c.content)
                 .join(inner, on=(Tweet.c.user_id == inner.c.id)))
        self.assertSQL(query, (
            'SELECT "tweets"."content" FROM "tweets" '
            'INNER JOIN (SELECT "users"."id" FROM "users") AS "j1" '
            'ON ("tweets"."user_id" = "j1"."id")'), [])

    def test_join_on_misc(self):
        cond = fn.Magic(Person.id, Note.id).alias('magic')
        query = Person.select(Person.id).join(Note, on=cond)
        self.assertSQL(query, (
            'SELECT "person"."id" FROM "person" '
            'INNER JOIN "note" '
            'ON Magic("person"."id", "note"."id") AS "magic"'), [])

    def test_select_integration(self):
        query = User.select(User.c.username).where(User.c.is_admin == True)
        self.assertSQL(query, (
            'SELECT "users"."username" FROM "users" '
            'WHERE ("users"."is_admin" = ?)'), [True])

        # Test wide range of SELECT query methods.
        query = (User
                 .select(User.c.username, fn.COUNT(Tweet.c.id).alias('ct'))
                 .left_outer_join(Tweet, on=(Tweet.c.user_id == User.c.id))
                 .where(User.c.is_admin == True)
                 .group_by(User.c.username)
                 .having(fn.COUNT(Tweet.c.id) > 1)
                 .order_by(SQL('ct').desc(), User.c.username.asc())
                 .limit(10))
        self.assertSQL(query, (
            'SELECT "users"."username", COUNT("tweets"."id") AS "ct" '
            'FROM "users" '
            'LEFT OUTER JOIN "tweets" ON ("tweets"."user_id" = "users"."id") '
            'WHERE ("users"."is_admin" = ?) '
            'GROUP BY "users"."username" '
            'HAVING (COUNT("tweets"."id") > ?) '
            'ORDER BY ct DESC, "users"."username" ASC '
            'LIMIT 10'), [True, 1])

    def test_joins_integration(self):
        Flag = Table('flag', ('id', 'tweet_id', 'flag_type'))
        Avatar = Table('avatar', ('id', 'user_id', 'url'))
        q = (User
             .select(User.c.username, Avatar.url, Tweet.c.content)
             .join(Tweet, on=(Tweet.c.user_id == User.c.id))
             .join(Flag, on=(Flag.tweet_id == Tweet.c.id))
             .join(Avatar, on=(Avatar.user_id == User.c.id))
             .where(Flag.flag_type != 'nuggie')
             .group_by(User.c.username, Avatar.url, Tweet.c.content)
             .having(fn.COUNT(Flag.id) == 0))
        self.assertSQL(q, (
            'SELECT "users"."username", "avatar"."url", "tweets"."content" '
            'FROM "users" '
            'INNER JOIN "tweets" ON ("tweets"."user_id" = "users"."id") '
            'INNER JOIN "flag" ON ("flag"."tweet_id" = "tweets"."id") '
            'INNER JOIN "avatar" ON ("avatar"."user_id" = "users"."id") '
            'WHERE ("flag"."flag_type" != ?) '
            'GROUP BY "users"."username", "avatar"."url", "tweets"."content" '
            'HAVING (COUNT("flag"."id") = ?)'), ['nuggie', 0])

    def test_compound_integration(self):
        q1 = (User
              .select(User.c.id, User.c.username)
              .where(User.c.is_admin == True))

        U2 = User.alias('u2')
        q2 = (U2
              .select(U2.c.id, U2.c.username)
              .where(U2.c.username << ('foo', 'bar', 'baz')))

        q = (q1 | q2).order_by(SQL('1')).limit(10)
        self.assertSQL(q, (
            'SELECT "users"."id", "users"."username" '
            'FROM "users" WHERE ("users"."is_admin" = ?) '
            'UNION '
            'SELECT "u2"."id", "u2"."username" '
            'FROM "users" AS "u2" '
            'WHERE ("u2"."username" IN (?, ?, ?)) '
            'ORDER BY 1 LIMIT 10'), [True, 'foo', 'bar', 'baz'])

        U3 = User.alias('u3')
        q3 = q & U3.select(U3.c.id, U3.c.username).where(U3.c.id > 100)
        self.assertSQL(q3, (
            'SELECT "users"."id", "users"."username" '
            'FROM "users" WHERE ("users"."is_admin" = ?) '
            'UNION '
            'SELECT "u2"."id", "u2"."username" '
            'FROM "users" AS "u2" '
            'WHERE ("u2"."username" IN (?, ?, ?)) '
            'ORDER BY 1 LIMIT 10 '
            'INTERSECT '
            'SELECT "u3"."id", "u3"."username" '
            'FROM "users" AS "u3" WHERE ("u3"."id" > ?)'),
            [True, 'foo', 'bar', 'baz', 100])


class TestInsertQuery(BaseTestCase):
    def test_insert(self):
        query = User.insert({
            User.c.username: 'charlie',
            User.c.superuser: False,
            User.c.admin: True})
        self.assertSQL(query, (
            'INSERT INTO "users" ("admin", "superuser", "username") '
            'VALUES (?, ?, ?)'), [True, False, 'charlie'])

        query = User.insert(username='charlie', superuser=False)
        self.assertSQL(query, (
            'INSERT INTO "users" ("superuser", "username") VALUES (?, ?)'),
            [False, 'charlie'])

    def test_insert_list(self):
        data = [
            {Person.name: 'charlie'},
            {Person.name: 'huey'},
            {Person.name: 'zaizee'}]
        query = Person.insert(data)
        self.assertSQL(query, (
            'INSERT INTO "person" ("name") VALUES (?), (?), (?)'),
            ['charlie', 'huey', 'zaizee'])

        query = Person.insert(data, columns=(Person.name,))
        self.assertSQL(query, (
            'INSERT INTO "person" ("name") VALUES (?), (?), (?)'),
            ['charlie', 'huey', 'zaizee'])

        data = [('charlie',), ('huey',), ('zaizee',)]
        query = User.insert(data, columns=[User.c.username])
        self.assertSQL(query, (
            'INSERT INTO "users" ("username") VALUES (?), (?), (?)'),
            ['charlie', 'huey', 'zaizee'])

    def test_insert_query(self):
        source = User.select(User.c.username).where(User.c.admin == False)
        query = Person.insert(source, columns=[Person.name])
        self.assertSQL(query, (
            'INSERT INTO "person" ("name") '
            'SELECT "users"."username" FROM "users" '
            'WHERE ("users"."admin" = ?)'), [False])

        source = Person.select(Person.name, False)
        query = User.insert(source, columns=(User.c.username, User.c.is_admin))
        self.assertSQL(query, (
            'INSERT INTO "users" ("username", "is_admin") '
            'SELECT "person"."name", ? FROM "person"'), [False])

    def test_insert_query_cte(self):
        cte = User.select(User.c.username).cte('foo')
        source = cte.select(cte.c.username)
        query = Person.insert(source, columns=[Person.name]).with_cte(cte)
        self.assertSQL(query, (
            'WITH "foo" AS (SELECT "users"."username" FROM "users") '
            'INSERT INTO "person" ("name") '
            'SELECT "foo"."username" FROM "foo"'), [])

    def test_empty(self):
        query = Table('empty').insert()
        self.assertSQL(query, 'INSERT INTO "empty" DEFAULT VALUES', [])


class TestUpdateQuery(BaseTestCase):
    def test_update(self):
        query = (User
                 .update({User.c.counter: User.c.counter + 1})
                 .where(User.c.username == 'nugz'))
        self.assertSQL(query, (
            'UPDATE "users" SET '
            '"counter" = ("counter" + ?) '
            'WHERE ("username" = ?)'), [1, 'nugz'])

        query = (User
                 .update(is_admin=False)
                 .where(User.c.is_admin == True))
        self.assertSQL(query, (
            'UPDATE "users" SET "is_admin" = ? WHERE ("is_admin" = ?)'),
            [False, True])

    def test_update_multi_set(self):
        Stat = Table('stat', ('id', 'key', 'counter'))
        query = (Stat
                 .update(O(
                     (Stat.key, 'k1'),
                     (Stat.counter, Stat.counter + 1)))
                 .where(Stat.key == 'k2'))
        self.assertSQL(query, (
            'UPDATE "stat" SET '
            '"key" = ?, '
            '"counter" = ("counter" + ?) '
            'WHERE ("key" = ?)'), ['k1', 1, 'k2'])

    def test_update_subquery(self):
        subquery = (User
                    .select(User.c.id, fn.COUNT(Tweet.c.id).alias('ct'))
                    .join(Tweet, on=(Tweet.c.user_id == User.c.id))
                    .group_by(User.c.id)
                    .having(SQL('ct') > 100))
        query = (User
                 .update({User.c.muted: True})
                 .where(User.c.id << subquery))
        self.assertSQL(query, (
            'UPDATE "users" SET '
            '"muted" = ? '
            'WHERE ("id" IN ('
            'SELECT "users"."id", COUNT("tweets"."id") AS "ct" '
            'FROM "users" '
            'INNER JOIN "tweets" '
            'ON ("tweets"."user_id" = "users"."id") '
            'GROUP BY "users"."id" '
            'HAVING (ct > ?)))'), [True, 100])


class TestDeleteQuery(BaseTestCase):
    def test_delete(self):
        query = (User
                 .delete()
                 .where(User.c.username != 'charlie')
                 .limit(3))
        self.assertSQL(
            query,
            'DELETE FROM "users" WHERE ("username" != ?) LIMIT 3',
            ['charlie'])

    def test_delete_subquery(self):
        subquery = (User
                    .select(User.c.id, fn.COUNT(Tweet.c.id).alias('ct'))
                    .join(Tweet, on=(Tweet.c.user_id == User.c.id))
                    .group_by(User.c.id)
                    .having(SQL('ct') > 100))
        query = (User
                 .delete()
                 .where(User.c.id << subquery))
        self.assertSQL(query, (
            'DELETE FROM "users" '
            'WHERE ("id" IN ('
            'SELECT "users"."id", COUNT("tweets"."id") AS "ct" '
            'FROM "users" '
            'INNER JOIN "tweets" ON ("tweets"."user_id" = "users"."id") '
            'GROUP BY "users"."id" '
            'HAVING (ct > ?)))'), [100])

    def test_delete_cte(self):
        cte = (User
               .select(User.c.id)
               .where(User.c.admin == True)
               .cte('u'))
        query = (User
                 .delete()
                 .where(User.c.id << cte.select(cte.c.id))
                 .with_cte(cte))
        self.assertSQL(query, (
            'WITH "u" AS '
            '(SELECT "users"."id" FROM "users" WHERE ("users"."admin" = ?)) '
            'DELETE FROM "users" '
            'WHERE ("id" IN (SELECT "u"."id" FROM "u"))'), [True])


Account = Table('account', ('id', 'name'))
Message = Table('message', ('id', 'account_id', 'content', 'timestamp'))


class TestQueryExecution(BaseDatabaseTestCase):
    account_tbl = ('CREATE TABLE "account" (id INTEGER NOT NULL PRIMARY KEY, '
                   'name TEXT NOT NULL)')
    message_tbl = ('CREATE TABLE "message" (id INTEGER NOT NULL PRIMARY KEY, '
                   'account_id INTEGER NOT NULL REFERENCES account(id), '
                   'content TEXT NOT NULL, timestamp DATETIME NOT NULL)')

    def setUp(self):
        super(TestQueryExecution, self).setUp()

        self.execute(self.account_tbl)
        self.execute(self.message_tbl)

    def test_integration(self):
        iq = Account.insert((
            {Account.name: 'charlie'},
            {Account.name: 'huey'},
            {Account.name: 'mickey'},
            {Account.name: 'zaizee'}))
        last_row_id = iq.execute(self._db)
        self.assertEqual(last_row_id, 4)

        dt = datetime.datetime.now()
        iq = Message.insert({
            Message.account_id: 2,
            Message.content: 'meow',
            Message.timestamp: dt})
        self.assertEqual(iq.execute(self._db), 1)

        q = (Account
             .select(Account.name, Message.content, Message.timestamp)
             .join(Message, on=(Account.id == Message.account_id))
             .order_by(Message.timestamp))
        rows = q.execute(self._db)
        self.assertEqual([tuple(r) for r in rows], [
            ('huey', 'meow', dt)])

        D = lambda day: datetime.datetime(2017, 1, day)
        data = (
            {Message.account_id: 2, Message.content: 'hiss', Message.timestamp: D(1)},
            {Message.account_id: 3, Message.content: 'woof', Message.timestamp: D(2)},
            {Message.account_id: 2, Message.content: 'purr', Message.timestamp: D(3)})
        Message.insert(data).execute(self._db)

        curs = (Message
                .select(Account.name, Message.content)
                .join(Account, on=(Message.account_id == Account.id))
                .order_by(Message.timestamp)
                .limit(3)
                .namedtuples()
                .execute(self._db))
        self.assertEqual([tuple(r) for r in curs], [
            ('huey', 'hiss'),
            ('mickey', 'woof'),
            ('huey', 'purr')])

    def _create_people(self):
        Account.insert((
            {Account.name: 'charlie'},
            {Account.name: 'huey'},
            {Account.name: 'mickey'},
            {Account.name: 'zaizee'})).execute(self._db)

    def test_cursor_wrappers(self):
        self._create_people()
        query = (Account
                 .select(
                     Account.name,
                     fn.SUBSTR(fn.MD5(Account.name), 1, 6).alias('hash'))
                 .order_by(Account.name))

        dicts = list(query.dicts().execute(self._db))
        self.assertEqual(dicts, [
            {'hash': 'bf779e', 'name': 'charlie'},
            {'hash': 'ef6307', 'name': 'huey'},
            {'hash': '4d5257', 'name': 'mickey'},
            {'hash': '7670f7', 'name': 'zaizee'},
        ])

        objs = list(query.execute(self._db))
        self.assertEqual(objs, [
            ('charlie', 'bf779e'),
            ('huey', 'ef6307'),
            ('mickey', '4d5257'),
            ('zaizee', '7670f7')])

    def test_multiple_iteration_result_caching(self):
        self._create_people()

        with self.assertQueryCount(1):
            query = Account.select(Account.name).order_by(Account.name)
            cw = query.execute(self._db)
            rows = [r for r in cw]

            self.assertEqual(cw[1], ('huey',))
            self.assertEqual(cw[2], ('mickey',))
            self.assertEqual(cw[:2], [('charlie',), ('huey',)])
            self.assertEqual(cw[-1], ('zaizee',))

            rows = [r for r, in cw]
            self.assertEqual(rows, ['charlie', 'huey', 'mickey', 'zaizee'])

    def test_indexing_query(self):
        self._create_people()
        with self.assertQueryCount(1):
            query = Account.select().order_by(Account.name).namedtuples()
            cursor_wrapper = query.execute(self._db)
            self.assertEqual(cursor_wrapper[0].name, 'charlie')
            self.assertEqual(cursor_wrapper[3].name, 'zaizee')
            self.assertEqual([x.name for x in cursor_wrapper[1:3]],
                             ['huey', 'mickey'])
            self.assertRaises(IndexError, lambda: cursor_wrapper[4])

    def test_builtin_functions(self):
        self._create_people()
        query = (Account
                 .select(Account.name, fn.MD5(Account.name).alias('md5'))
                 .order_by(Account.name))
        self.assertEqual([tuple(row) for row in query.execute(self._db)], [
            ('charlie', 'bf779e0933a882808585d19455cd7937'),
            ('huey', 'ef63073058e004fb5e7f2b1f081c416f'),
            ('mickey', '4d5257e5acc7fcac2f5dcd66c4e78f9a'),
            ('zaizee', '7670f7740a587d5eaa3f56196c3b36ed')])

        curs = (Account
                .select(Account.name)
                .where(fn.REGEXP(Account.name, '^.+ey$'))
                .order_by(Account.name)
                .namedtuples()
                .execute(self._db))
        self.assertEqual([row.name for row in curs],
                         ['huey', 'mickey'])

    def test_bound_insert_execution(self):
        BA = Account.bind(self._db)
        rowid = BA.insert({BA.name: 'charlie'}).execute()
        self.assertEqual(rowid, 1)

        rowid = BA.insert(name='huey').execute()
        self.assertEqual(rowid, 2)

        rowid = BA.insert([{Account.name: 'zaizee'}, ('mickey',)],
                          columns=[Account.name]).execute()
        self.assertEqual(rowid, 4)

        curs = BA.select(BA.name).order_by(BA.name).namedtuples()
        self.assertEqual([a.name for a in curs], ['charlie', 'huey', 'mickey',
                                                  'zaizee'])

    def test_bound_update_execution(self):
        BA = Account.bind(self._db)
        BA.insert([(name,) for name in ('charlie', 'huey', 'zaizee')],
                  columns=[BA.name]).execute()

        query = (BA
                 .update({BA.name: BA.name.concat('-x')})
                 .where(BA.name != 'huey'))
        self.assertEqual(query.execute(), 2)

        curs = BA.select(BA.name).order_by(BA.name).namedtuples()
        self.assertEqual([a.name for a in curs],
                         ['charlie-x', 'huey', 'zaizee-x'])

    def _create_accounts(self, *names):
        iq = Account.insert([{Account.name: name} for name in names])
        return iq.execute(self._db)

    def test_bound_select_execution(self):
        self._create_accounts('charlie', 'huey')
        BAccount = Account.bind(self._db)
        huey = (BAccount
                .select()
                .where(BAccount.name == 'huey')
                .dicts()
                .execute()
                .get())
        self.assertEqual(huey['name'], 'huey')

        huey = (BAccount
                .select()
                .where(BAccount.name == 'huey')
                .dicts()
                .get())
        self.assertEqual(huey['name'], 'huey')

        self.assertRaises(DoesNotExist,
                          BAccount.select().where(BAccount.name == 'x').get)

    def test_bound_select_direct_indexing(self):
        self._create_accounts('charlie', 'huey', 'mickey', 'zaizee')

        with self.assertQueryCount(1):
            BAccount = Account.bind(self._db)
            query = BAccount.select().order_by(Account.name).namedtuples()
            self.assertEqual(query[0].name, 'charlie')
            self.assertEqual(query[3].name, 'zaizee')
            self.assertEqual([x.name for x in query[1:3]],
                             ['huey', 'mickey'])
            self.assertRaises(IndexError, lambda: query[4])

    def test_bound_select_caching(self):
        self._create_accounts('charlie', 'huey', 'zaizee')
        BAccount = Account.bind(self._db)
        query = (BAccount
                 .select(BAccount.name.alias('account_name'))
                 .where(fn.LOWER(fn.SUBSTR(BAccount.name, 1, 1)) << ['c', 'z'])
                 .order_by(-BAccount.name)
                 .namedtuples())
        with self.assertQueryCount(1):
            for _ in range(3):
                self.assertEqual([row.account_name for row in query],
                                 ['zaizee', 'charlie'])

            self.assertEqual(query.get().account_name, 'zaizee')
            self.assertEqual(query.first().account_name, 'zaizee')

        query = query.where(BAccount.name.endswith('ie'))
        with self.assertQueryCount(1):
            for _ in range(3):
                self.assertEqual([row.account_name for row in query],
                                 ['charlie'])

    def test_bound_table(self):
        BAccount = Account.bind(self._db)
        self.assertEqual(BAccount.insert([
            {BAccount.name: 'charlie'},
            {BAccount.name: 'huey'},
            {BAccount.name: 'zaizee'}]).execute(), 3)

        query = (BAccount
                 .select(BAccount.name)
                 .where(BAccount.name.endswith('e'))
                 .order_by(BAccount.name))
        self.assertEqual([row for row, in query], ['charlie', 'zaizee'])

        nrows = BAccount.delete().where(BAccount.name == 'charlie').execute()
        self.assertEqual(nrows, 1)

        nrows = (BAccount
                 .update({BAccount.name: BAccount.name.concat('-cat')})
                 .where(BAccount.name << ('huey', 'zaizee'))
                 .execute())
        rows = BAccount.select(BAccount.name).order_by(BAccount.name).dicts()
        self.assertEqual(list(rows), [
            {'name': 'huey-cat'},
            {'name': 'zaizee-cat'}])


FNote = Table('note', ('id', 'content'))
FNoteIdx = Table('note_idx', ('docid', 'content'))


class TestRankFunctions(BaseDatabaseTestCase):
    data = (
        ('A faith is a necessity to a man. Woe to him who believes in '
         'nothing.'),
        ('All who call on God in true faith, earnestly from the heart, will '
         'certainly be heard, and will receive what they have asked and '
         'desired.'),
        ('Be faithful in small things because it is in them that your '
         'strength lies.'),
        ('Faith consists in believing when it is beyond the power of reason '
         'to believe.'),
        ('Faith has to do with things that are not seen and hope with things '
         'that are not at hand.'))

    def setUp(self):
        super(TestRankFunctions, self).setUp()

        self.execute('CREATE TABLE note (id INTEGER NOT NULL PRIMARY KEY, '
                     'content TEXT NOT NULL)')
        self.execute('CREATE VIRTUAL TABLE note_idx USING FTS4 '
                     '(content, tokenize=porter)')

        for content in self.data:
            note_id = FNote.insert({Note.content: content}).execute(self._db)
            FNoteIdx.insert({
                FNoteIdx.docid: note_id,
                FNoteIdx.content: content}).execute(self._db)

    def assertNotes(self, query, indexes):
        self.assertEqual([obj.content for obj in query],
                         [self.data[idx] for idx in indexes])

    def test_rank(self):
        query = (FNoteIdx
                 .select()
                 .where(FNoteIdx.match('believe'))
                 .order_by(FNoteIdx.docid)
                 .namedtuples()
                 .execute(self._db))
        self.assertNotes(query, [0, 3])

        rank = FNoteIdx.rank()
        query = (FNoteIdx
                 .select(FNoteIdx.content, rank.alias('score'))
                 .where(FNoteIdx.match('things'))
                 .order_by(rank)
                 .namedtuples()
                 .execute(self._db))
        self.assertEqual([(row.content, row.score) for row in query], [
            (self.data[4], -2. / 3),
            (self.data[2], -1. / 3)])

    def test_bm25(self):
        rank = FNoteIdx.bm25()
        query = (FNoteIdx
                 .select(FNoteIdx.content, rank.alias('score'))
                 .where(FNoteIdx.match('things'))
                 .order_by(rank)
                 .namedtuples()
                 .execute(self._db))
        self.assertEqual([(r.content, round(r.score, 2)) for r in query], [
            (self.data[4], -0.45),
            (self.data[2], -0.36)])

        # All phrases contain the phrase "Faith" so the results are
        # indeterminate.
        query = (FNoteIdx
                 .select(FNoteIdx.content, rank.alias('score'))
                 .where(FNoteIdx.match('faith'))
                 .order_by(rank)
                 .namedtuples()
                 .execute(self._db))
        self.assertEqual([round(row.score) for row in query],
                         [0., 0., 0., 0., 0.])


HUser = Table('users', ('id', 'username'))


class TestHashFunctions(BaseDatabaseTestCase):
    def setUp(self):
        super(TestHashFunctions, self).setUp()
        self.execute('create table users (id integer not null primary key, '
                     'username text not null)')

    def test_md5(self):
        for username in ('charlie', 'huey', 'zaizee'):
            HUser.insert({HUser.username: username}).execute(self._db)

        query = (HUser
                 .select(HUser.username,
                         fn.SUBSTR(fn.SHA1(HUser.username), 1, 6).alias('sha'))
                 .order_by(HUser.username)
                 .execute(self._db))

        self.assertEqual(query[:], [
            ('charlie', 'd8cd10'),
            ('huey', '89b31a'),
            ('zaizee', 'b4dcf9')])


if __name__ == '__main__':
    unittest.main(argv=sys.argv)
