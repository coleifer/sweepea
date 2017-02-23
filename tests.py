from collections import Counter
from collections import OrderedDict
import datetime
import os
import re
import sys
import unittest

import sqlite3
from sweepea import Context
from sweepea import Database
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


class BaseTestCase(unittest.TestCase):
    pass


class TestTableFunction(BaseTestCase):
    def setUp(self):
        self.conn = sqlite3.connect(':memory:')

    def tearDown(self):
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


class TestHelpers(BaseTestCase):
    __db_filename__ = 'test.db'

    def setUp(self):
        super(TestHelpers, self).setUp()
        self._db = Database(self.__db_filename__)
        self._db.connect()

    def tearDown(self):
        super(TestHelpers, self).tearDown()
        self._db.close()
        if os.path.exists(self.__db_filename__):
            os.unlink(self.__db_filename__)

    def execute(self, sql, *params):
        return self._db.execute_sql(sql, params, commit=False)

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
        self.assertEqual(self._db.rowcount, 0)

        self.execute('insert into register (value) values (?), (?)', 'a', 'b')
        self.assertEqual(self._db.rowcount, 2)
        self.assertEqual(self._db.last_insert_id, 2)

        self._db.close()
        self._db.connect()

        self.execute('insert into register (value) values (?), (?)', 'c', 'd')
        self.assertEqual(self._db.rowcount, 2)
        self.assertEqual(self._db.last_insert_id, 4)

        self.execute('insert into register (value) values (?)', 'e')
        self.assertEqual(self._db.rowcount, 1)
        self.assertEqual(self._db.last_insert_id, 5)

        # Changes works with update queries.
        self.execute('update register set value=?', 'x')
        self.assertEqual(self._db.rowcount, 5)
        self.assertEqual(self._db.last_insert_id, 5)

        # Changes picks up rows deleted.
        self.execute('delete from register where rowid > ?', 2)
        self.assertEqual(self._db.rowcount, 3)

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
            return cmp(l1, l2)

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
        self.assertFalse(mem_high == 0)

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
        self.assertRaises(sqlite3.InterfaceError, db.connect)

        self.assertTrue(db.close())
        self.assertFalse(db.close())
        self.assertTrue(db.is_closed())

        with db:
            self.assertFalse(db.is_closed())
            self.assertFalse(db.autocommit)

        self.assertTrue(db.is_closed())


User = Table('users', ('id', 'username', 'is_admin'))
Tweet = Table('tweet', ('id', 'user_id', 'content', 'timestamp'))

def __sql__(query):
    return Context().parse(query)


class TestQueryBuilder(BaseTestCase):
    def test_select(self):
        q = User.select().where(User.is_admin == True)
        sql, params = __sql__(q)
        self.assertEqual(sql, ('SELECT "users"."id", "users"."username", '
                               '"users"."is_admin" '
                               'FROM "users" '
                               'WHERE ("users"."is_admin" = ?)'))
        self.assertEqual(params, [True])

        # Test wide range of SELECT query methods.
        q = (User
             .select(User.username, fn.COUNT(Tweet.id).alias('ct'))
             .left_outer_join(Tweet, on=(Tweet.user_id == User.id))
             .where(User.is_admin == True)
             .group_by(User.username)
             .having(fn.COUNT(Tweet.id) > 1)
             .order_by(SQL('ct').desc(), User.username.asc())
             .limit(10))
        sql, params = __sql__(q)
        self.assertEqual(sql, (
            'SELECT "users"."username", COUNT("tweet"."id") AS "ct" '
            'FROM "users" '
            'LEFT OUTER JOIN "tweet" ON ("tweet"."user_id" = "users"."id") '
            'WHERE ("users"."is_admin" = ?) '
            'GROUP BY "users"."username" '
            'HAVING (COUNT("tweet"."id") > ?) '
            'ORDER BY ct DESC, "users"."username" ASC '
            'LIMIT 10'))
        self.assertEqual(params, [True, 1])

    def test_subquery(self):
        inner = (Tweet
                 .select(fn.COUNT(Tweet.id).alias('ct'))
                 .where(Tweet.user_id == User.id))
        query = (User
                 .select(User.username, inner.alias('iq'))
                 .order_by(User.username))
        sql, params = __sql__(query)
        self.assertEqual(sql, (
            'SELECT "users"."username", '
            '(SELECT COUNT("tweet"."id") AS "ct" '
            'FROM "tweet" '
            'WHERE ("tweet"."user_id" = "users"."id")) AS "iq" '
            'FROM "users" ORDER BY "users"."username"'))
        self.assertEqual(params, [])

    def test_user_defined_alias(self):
        UA = User.alias('alt')
        query = (User
                 .select(User.id, User.username, UA.is_admin)
                 .join(UA, on=(User.id == UA.id))
                 .order_by(UA.is_admin))
        sql, params = __sql__(query)
        self.assertEqual(sql, (
            'SELECT "users"."id", "users"."username", "alt"."is_admin" '
            'FROM "users" '
            'INNER JOIN "users" AS "alt" ON ("users"."id" = "alt"."id") '
            'ORDER BY "alt"."is_admin"'))

    def test_multi_join_select(self):
        Flag = Table('flag', ('id', 'tweet_id', 'flag_type'))
        Avatar = Table('avatar', ('id', 'user_id', 'url'))
        q = (User
             .select(User.username, Avatar.url, Tweet.content)
             .join(Tweet, on=(Tweet.user_id == User.id))
             .join(Flag, on=(Flag.tweet_id == Tweet.id))
             .join(Avatar, on=(Avatar.user_id == User.id))
             .where(Flag.flag_type != 'nuggie')
             .group_by(User.username, Avatar.url, Tweet.content)
             .having(fn.COUNT(Flag.id) == 0))
        sql, params = __sql__(q)
        self.assertEqual(sql, (
            'SELECT "users"."username", "avatar"."url", "tweet"."content" '
            'FROM "users" '
            'INNER JOIN "tweet" ON ("tweet"."user_id" = "users"."id") '
            'INNER JOIN "flag" ON ("flag"."tweet_id" = "tweet"."id") '
            'INNER JOIN "avatar" ON ("avatar"."user_id" = "users"."id") '
            'WHERE ("flag"."flag_type" != ?) '
            'GROUP BY "users"."username", "avatar"."url", "tweet"."content" '
            'HAVING (COUNT("flag"."id") = ?)'))
        self.assertEqual(params, ['nuggie', 0])

    def test_compound_query(self):
        U2 = User.alias('u2')
        q1 = User.select().where(User.is_admin == True)
        q2 = U2.select().where(U2.username << ('foo', 'bar', 'baz'))
        q = (q1 | q2).order_by(SQL('1')).limit(10)
        sql, params = __sql__(q)
        self.assertEqual(sql, (
            'SELECT "users"."id", "users"."username", "users"."is_admin" '
            'FROM "users" WHERE ("users"."is_admin" = ?) '
            'UNION '
            'SELECT "u2"."id", "u2"."username", "u2"."is_admin" '
            'FROM "users" AS "u2" '
            'WHERE ("u2"."username" IN (?, ?, ?)) '
            'ORDER BY 1 LIMIT 10'))
        self.assertEqual(params, [True, 'foo', 'bar', 'baz'])

        U3 = User.alias('u3')
        q3 = q & U3.select().where(U3.id > 100)
        sql, params = __sql__(q3)
        self.assertEqual(sql, (
            'SELECT "users"."id", "users"."username", "users"."is_admin" '
            'FROM "users" WHERE ("users"."is_admin" = ?) '
            'UNION '
            'SELECT "u2"."id", "u2"."username", "u2"."is_admin" '
            'FROM "users" AS "u2" '
            'WHERE ("u2"."username" IN (?, ?, ?)) '
            'ORDER BY 1 LIMIT 10 '
            'INTERSECT '
            'SELECT "u3"."id", "u3"."username", "u3"."is_admin" '
            'FROM "users" AS "u3" WHERE ("u3"."id" > ?)'))
        self.assertEqual(params, [True, 'foo', 'bar', 'baz', 100])

    def test_cte(self):
        Order = Table('orders', columns=(
            'region',
            'amount',
            'product',
            'quantity'))

        regional_sales = (Order
                          .select(Order.region,
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
                 .with_(regional_sales, top_regions))
        sql, params = __sql__(query)
        self.assertEqual(sql, (
            'WITH "regional_sales" AS ('
            'SELECT "orders"."region", SUM("orders"."amount") AS "total_sales" '
            'FROM "orders" '
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
            ') GROUP BY "orders"."region", "orders"."product"'))
        self.assertEqual(params, [10])

    def test_insert_simple(self):
        query = User.insert(O(
            (User.username, 'charlie'),
            (User.is_admin, False)))
        sql, params = __sql__(query)
        self.assertEqual(sql, (
            'INSERT INTO "users" ("username", "is_admin") '
            'VALUES (?, ?)'))
        self.assertEqual(params, ['charlie', False])

    def test_insert_multi(self):
        data = [
            O((User.username, 'foo'), (User.is_admin, True)),
            O((User.username, 'bar'), (User.is_admin, False)),
            O((User.username, 'baz'), (User.is_admin, False))]
        query = User.insert(data)
        sql, params = __sql__(query)
        self.assertEqual(sql, (
            'INSERT INTO "users" ("username", "is_admin") '
            'VALUES (?, ?), (?, ?), (?, ?)'))
        self.assertEqual(params, ['foo', True, 'bar', False, 'baz', False])

    def test_insert_query(self):
        Employee = Table('employee')
        query = User.insert(
            Employee.select(Employee.c.name, SQL('0')),
            columns=[User.username, User.is_admin])
        sql, params = __sql__(query)
        self.assertEqual(sql, ('INSERT INTO "users" ("username", "is_admin") '
                               'SELECT "employee"."name", 0 FROM "employee"'))

    def test_update(self):
        Stat = Table('stat', ('id', 'key', 'counter'))
        query = (Stat
                 .update(O(
                     (Stat.key, 'k1'),
                     (Stat.counter, Stat.counter + 1)))
                 .where(Stat.key == 'k2'))
        sql, params = __sql__(query)
        self.assertEqual(sql, (
            'UPDATE "stat" SET '
            '"key" = ?, '
            '"counter" = ("counter" + ?) '
            'WHERE ("key" = ?)'))
        self.assertEqual(params, ['k1', 1, 'k2'])

    def test_update_subquery(self):
        subquery = (User
                    .select(User.id, fn.COUNT(Tweet.id).alias('ct'))
                    .join(Tweet, on=(Tweet.user_id == User.id))
                    .group_by(User.id)
                    .having(SQL('ct') > 100))
        query = (User
                 .update({User.is_admin: True})
                 .where(User.id << subquery))
        sql, params = __sql__(query)
        self.assertEqual(sql, (
            'UPDATE "users" SET '
            '"is_admin" = ? '
            'WHERE ("id" IN ('
            'SELECT "users"."id", COUNT("tweet"."id") AS "ct" '
            'FROM "users" '
            'INNER JOIN "tweet" '
            'ON ("tweet"."user_id" = "users"."id") '
            'GROUP BY "users"."id" '
            'HAVING (ct > ?)))'))
        self.assertEqual(params, [True, 100])

    def test_delete_query(self):
        query = (User
                 .delete()
                 .where(User.username != 'charlie')
                 .limit(3))
        sql, params = __sql__(query)
        self.assertEqual(sql, (
            'DELETE FROM "users" WHERE ("username" != ?) LIMIT 3'))
        self.assertEqual(params, ['charlie'])

    def test_delete_subquery(self):
        subquery = (User
                    .select(User.id, fn.COUNT(Tweet.id).alias('ct'))
                    .join(Tweet, on=(Tweet.user_id == User.id))
                    .group_by(User.id)
                    .having(SQL('ct') > 100))
        query = (User
                 .delete()
                 .where(User.id << subquery))
        sql, params = __sql__(query)
        self.assertEqual(sql, (
            'DELETE FROM "users" '
            'WHERE ("id" IN ('
            'SELECT "users"."id", COUNT("tweet"."id") AS "ct" '
            'FROM "users" '
            'INNER JOIN "tweet" ON ("tweet"."user_id" = "users"."id") '
            'GROUP BY "users"."id" '
            'HAVING (ct > ?)))'))
        self.assertEqual(params, [100])


database = Database(':memory:')

Person = Table('person', ('id', 'name'))
Note = Table('notes', ('id', 'person_id', 'content', 'timestamp'))


class TestQueryExecution(BaseTestCase):
    person_tbl = ('CREATE TABLE "person" (id INTEGER NOT NULL PRIMARY KEY, '
                  'name TEXT NOT NULL)')
    notes_tbl = ('CREATE TABLE "notes" (id INTEGER NOT NULL PRIMARY KEY, '
                 'person_id INTEGER NOT NULL REFERENCES person(id), '
                 'content TEXT NOT NULL, timestamp DATETIME NOT NULL)')


    def setUp(self):
        super(TestQueryExecution, self).setUp()
        database.connect()

        database.execute_sql(self.person_tbl)
        database.execute_sql(self.notes_tbl)

    def tearDown(self):
        database.close()
        super(TestQueryExecution, self).tearDown()

    def test_integration(self):
        iq = Person.insert((
            {Person.name: 'charlie'},
            {Person.name: 'huey'},
            {Person.name: 'mickey'},
            {Person.name: 'zaizee'}))
        last_row_id = iq.execute(database)
        self.assertEqual(last_row_id, 4)

        dt = datetime.datetime.now()
        iq = Note.insert({
            Note.person_id: 2,
            Note.content: 'meow',
            Note.timestamp: dt})
        self.assertEqual(iq.execute(database), 1)

        q = (Person
             .select(Person.name, Note.content, Note.timestamp)
             .join(Note, on=(Person.id == Note.person_id))
             .order_by(Note.timestamp))
        rows = q.execute(database)
        self.assertEqual([tuple(r) for r in rows], [
            ('huey', 'meow', dt)])

        D = lambda day: datetime.datetime(2017, 1, day)
        (Note
         .insert((
             {Note.person_id: 2, Note.content: 'hiss', Note.timestamp: D(1)},
             {Note.person_id: 3, Note.content: 'woof', Note.timestamp: D(2)},
             {Note.person_id: 2, Note.content: 'purr', Note.timestamp: D(3)}))
         .execute(database))

        curs = (Note
                .select(Person.name, Note.content)
                .join(Person, on=(Note.person_id == Person.id))
                .order_by(-Note.timestamp)
                .limit(3)
                .execute(database))
        self.assertEqual([tuple(r) for r in curs], [
            ('huey', 'meow'),
            ('huey', 'purr'),
            ('mickey', 'woof')])

    def _create_people(self):
        Person.insert((
            {Person.name: 'charlie'},
            {Person.name: 'huey'},
            {Person.name: 'mickey'},
            {Person.name: 'zaizee'})).execute(database)

    def test_cursor_wrappers(self):
        self._create_people()
        query = (Person
                 .select(
                     Person.name,
                     fn.SUBSTR(fn.MD5(Person.name), 1, 6).alias('hash'))
                 .order_by(Person.name))

        dicts = list(query.dicts().execute(database))
        self.assertEqual(dicts, [
            {'hash': 'bf779e', 'name': 'charlie'},
            {'hash': 'ef6307', 'name': 'huey'},
            {'hash': '4d5257', 'name': 'mickey'},
            {'hash': '7670f7', 'name': 'zaizee'},
        ])

        objs = list(query.tuples().execute(database))
        self.assertEqual(objs, [
            ('charlie', 'bf779e'),
            ('huey', 'ef6307'),
            ('mickey', '4d5257'),
            ('zaizee', '7670f7')])

    def test_cursor_iteration(self):
        self._create_people()
        query = Person.select(Person.name).order_by(Person.name)
        cw = query.execute(database)
        rows = [r for r in cw]

        self.assertEqual(cw[1], ('huey',))
        self.assertEqual(cw[2], ('mickey',))
        self.assertEqual(cw[:2], [('charlie',), ('huey',)])
        self.assertEqual(cw[-1], ('zaizee',))

        rows = [r for r, in cw]
        self.assertEqual(rows, ['charlie', 'huey', 'mickey', 'zaizee'])

    def test_builtin_functions(self):
        self._create_people()
        query = (Person
                 .select(Person.name, fn.MD5(Person.name).alias('md5'))
                 .order_by(Person.name))
        self.assertEqual([tuple(row) for row in query.execute(database)], [
            ('charlie', 'bf779e0933a882808585d19455cd7937'),
            ('huey', 'ef63073058e004fb5e7f2b1f081c416f'),
            ('mickey', '4d5257e5acc7fcac2f5dcd66c4e78f9a'),
            ('zaizee', '7670f7740a587d5eaa3f56196c3b36ed')])

        curs = (Person
                .select(Person.name)
                .where(fn.REGEXP(Person.name, '^.+ey$'))
                .order_by(Person.name)
                .dicts()
                .execute(database))
        self.assertEqual([row['name'] for row in curs],
                         ['huey', 'mickey'])


if __name__ == '__main__':
    unittest.main(argv=sys.argv)
