#!/usr/bin/env python

import datetime
import logging
import os
import sys
import tempfile
import threading
import unittest
from contextlib import contextmanager

from sweepea import *


if sys.version_info[:2] <= (2, 6):
    raise RuntimeError('Get a newer version of Python to run the tests.')


logger = logging.getLogger('test-sweepea')

class QueryHandler(logging.Handler):
    def __init__(self, *args, **kwargs):
        self.queries = []
        logging.Handler.__init__(self, *args, **kwargs)

    def emit(self, record):
        self.queries.append(record)


test_db_file = os.environ.get('SWEEPEA_TEST_DATABASE') or tempfile.mktemp()


class TestDatabase(Database):
    def execute_sql(self, sql, params=None):
        logger.info((sql, params))
        return super(TestDatabase, self).execute_sql(sql, params)


def make_database():
    return TestDatabase(test_db_file)

test_db = make_database()

@test_db.func('TITLE')
def title(s):
    return s.title()


class Base(Declarative):
    class Meta:
        database = test_db

class User(Base):
    username = TextField()

class Tweet(Base):
    user = ForeignKeyField(User, column='user_id', backref='tweets')
    content = TextField(default='')
    timestamp = DateTimeField(default=datetime.datetime.now)

class Comment(Base):
    tweet = ForeignKeyField(Tweet, backref='comments')
    comment = TextField(default='')
    timestamp = DateTimeField(default=datetime.datetime.now)

class Favorite(Base):
    user = ForeignKeyField(User, column='user_id', backref='favorites')
    tweet = ForeignKeyField(Tweet, column='tweet_id')
    timestamp = DateTimeField(default=datetime.datetime.now)

class Category(Base):
    name = TextField(index=True)
    parent = ForeignKeyField('self', null=True, backref='children')

class _UpperField(TextField):
    def python_value(self, value):
        return value.upper() if value else value

class UpperUser(Base):
    username = _UpperField()
    class Meta:
        table_name = User._meta.table_name


UserTbl = User._meta.table
TweetTbl = Tweet._meta.table
CommentTbl = Comment._meta.table
FavoriteTbl = Favorite._meta.table


def create_fts_model(tbl_name, fields, **updates):
    options = {'tokenize': 'porter'}
    options.update(updates)
    return create_model(
        test_db,
        tbl_name,
        fields,
        include_primary_key=False,
        virtual=True,
        extension='fts4',
        options=options,
        base_class=FTSModel)

Post = create_fts_model('posts', (
    ('content', TextField()),
    ('idx', IntegerField()),
))

AutoFTSUser = create_fts_model('auto_fts_user', (
    ('username', TextField()),
), content='"user"')

MultiColumn = create_fts_model('multicolumn_fts', (
    ('c1', TextField(default='')),
    ('c2', TextField(default='')),
    ('c3', TextField(default='')),
    ('c4', IntegerField()),
))


class BaseTestCase(unittest.TestCase):
    def setUp(self):
        self.qh = QueryHandler()
        logger.setLevel(logging.INFO)
        logger.addHandler(self.qh)

    def tearDown(self):
        logger.removeHandler(self.qh)

    def assertIsNone(self, value):  # Py2.6.
        self.assertTrue(value is None)

    def get_queries(self, ignore_txn=False):
        queries = [q.msg for q in self.qh.queries]
        if ignore_txn:
            skip = ('BEGIN', 'COMMIT', 'ROLLBACK', 'SAVEPOINT', 'RELEASE')
            queries = [q for q in queries if not q[0].startswith(skip)]
        return queries

    @contextmanager
    def assertQueryCount(self, ct, ignore_txn=False):
        qc = len(self.get_queries(ignore_txn=ignore_txn))
        yield
        self.assertEqual(len(self.get_queries(ignore_txn=ignore_txn)) - qc, ct)

    def refresh(self, obj):
        return type(obj).get(
            obj._meta.primary_key == getattr(obj, obj._meta.primary_key.name))

    def list_of(self, model, field):
        return list(model.select(field).order_by(field).dicts())


class ModelTestCase(BaseTestCase):
    MODELS = [User, Tweet, Comment, Favorite, Category]

    def setUp(self):
        test_db.drop_tables(self.MODELS, True)
        test_db.create_tables(self.MODELS)
        super(ModelTestCase, self).setUp()

    def tearDown(self):
        test_db.close()
        os.unlink(test_db_file)
        super(ModelTestCase, self).tearDown()


class TestDatabaseClass(ModelTestCase):
    def test_connect_close(self):
        db = Database()
        conn_ids = []
        can_close = threading.Event()
        have_connected = [threading.Event() for _ in range(3)]

        def thread(connected_evt):
            with db:
                conn_ids.append(id(db.connection()))
                self.assertFalse(db.is_closed())
                connected_evt.set()
                can_close.wait()
            self.assertTrue(db.is_closed())

        threads = [threading.Thread(target=thread, args=(evt,))
                   for evt in have_connected]
        for thread in threads:
            thread.start()

        for evt in have_connected:
            evt.wait()

        can_close.set()

        for thread in threads:
            thread.join()

        self.assertEqual(len(set(conn_ids)), 3)

    def test_execute_sql(self):
        self.assertEqual(
            test_db.execute_sql('SELECT 1, 2').fetchone(),
            (1, 2))

    def test_pragmas(self):
        db = TestDatabase(pragmas=(
            ('cache_size', 1337),
        ))
        self.assertEqual(
            db.execute_sql('PRAGMA cache_size;').fetchone(),
            (1337,))

    def test_create_read_update_delete(self):
        for username in ['charlie', 'huey']:
            query = test_db.insert({'username': username}).into(UserTbl)
            query.execute()

        read = (test_db
                .select(UserTbl.username)
                .from_(UserTbl)
                .order_by(UserTbl.username))
        self.assertEqual([row for row in read], [('charlie',), ('huey',)])

        query = (test_db
                 .update(username=UserTbl.username.concat('-zai'))
                 .table(UserTbl))
        self.assertEqual(query.execute(), 2)
        self.assertEqual([row for row in read.dicts()], [
            {'username': 'charlie-zai'},
            {'username': 'huey-zai'},
        ])

        delete_q = (test_db
                    .delete()
                    .from_(UserTbl)
                    .where(UserTbl.username.endswith('zai')))
        self.assertEqual(delete_q.execute(), 2)
        self.assertEqual(User.select().count(), 0)

    def test_introspection(self):
        tables = test_db.get_tables()
        self.assertEqual(tables,
                         ['category', 'comment', 'favorite', 'tweet', 'user'])

        user_indexes = test_db.get_indexes('user')
        tweet_indexes = test_db.get_indexes('tweet')
        self.assertEqual(user_indexes, [])
        self.assertEqual(tweet_indexes, [(
            'tweet_user',
            'CREATE INDEX "tweet_user" ON "tweet" ("user_id")',
            ['user_id'],
            False,
        )])

        user_cols = test_db.get_columns('user')
        tweet_cols = test_db.get_columns('tweet')
        self.assertEqual(user_cols, [
            ('id', 'INTEGER', False, True),
            ('username', 'TEXT', False, False),
        ])
        self.assertEqual(tweet_cols, [
            ('id', 'INTEGER', False, True),
            ('user_id', 'INTEGER', False, False),
            ('content', 'TEXT', False, False),
            ('timestamp', 'DATETIME', False, False),
        ])

        user_pk = test_db.get_primary_keys('user')
        tweet_pk = test_db.get_primary_keys('tweet')
        self.assertEqual(user_pk, ['id'])
        self.assertEqual(tweet_pk, ['id'])

        user_fk = test_db.get_foreign_keys('user')
        tweet_fk = test_db.get_foreign_keys('tweet')
        self.assertEqual(user_fk, [])
        self.assertEqual(tweet_fk, [
            ('user_id', 'user', 'id'),
        ])

    def test_metadata(self):
        self.assertEqual(
            [field.name for field in User._meta.sorted_fields],
            ['id', 'username'])

        self.assertEqual(
            [field.name for field in Tweet._meta.sorted_fields],
            ['id', 'user', 'content', 'timestamp'])


        self.assertEqual(Tweet._meta.columns, {
            'id': Tweet.id,
            'user_id': Tweet.user,
            'content': Tweet.content,
            'timestamp': Tweet.timestamp})
        self.assertEqual(Tweet._meta.defaults, {'content': ''})
        self.assertEqual(Tweet._meta.defaults_callables,
                         {'timestamp': datetime.datetime.now})
        self.assertEqual(Tweet._meta.fields, {
            'id': Tweet.id,
            'user': Tweet.user,
            'content': Tweet.content,
            'timestamp': Tweet.timestamp})
        self.assertEqual(Tweet._meta.name, 'tweet')
        self.assertEqual(Tweet._meta.primary_key, Tweet.id)
        self.assertEqual(Tweet._meta.table_name, 'tweet')

    def test_metadata_references(self):
        self.assertEqual(User._meta.refs, {})
        self.assertEqual(
            User._meta.backrefs,
            {'favorites': User.favorites, 'tweets': User.tweets})

        self.assertEqual(Tweet._meta.refs, {'user': Tweet.user})
        self.assertEqual(
            Tweet._meta.backrefs,
            {'comments': Tweet.comments, 'favorite_set': Tweet.favorite_set})

        self.assertEqual(Category._meta.refs, {'parent': Category.parent})
        self.assertEqual(Category._meta.backrefs,
                         {'children': Category.children})

    def test_udf(self):
        curs = test_db.execute_sql('SELECT TITLE(?) AS foo', ('heLLo huey',))
        self.assertEqual(curs.fetchone(), ('Hello Huey',))

    def test_date_functions(self):
        dt = datetime.datetime
        datetimes = [
            dt(2015, 1, 2, 3, 4, 5),
            dt(2015, 2, 20, 19, 18, 17),
            dt(2015, 12, 25, 0, 0, 0),
            dt(2014, 12, 31, 23, 59, 59, 999999)]

        user = User.create(username='u1')
        for value in datetimes:
            Tweet.create(user=user, content=str(value), timestamp=value)

        ts = Tweet.timestamp
        query = (Tweet
                 .select(
                     Tweet.id, ts.year, ts.month, ts.day, ts.hour, ts.minute,
                     ts.second)
                 .order_by(Tweet.id))
        self.assertEqual(
            [(t.id, t.year, t.month, t.day, t.hour, t.minute, t.second)
             for t in query], [
                 (1, 2015, 1, 2, 3, 4, 5),
                 (2, 2015, 2, 20, 19, 18, 17),
                 (3, 2015, 12, 25, 0, 0, 0),
                 (4, 2014, 12, 31, 23, 59, 59)])


class TestDDL(BaseTestCase):
    def assertDDL(self, clause, sql):
        csql, _ = test_db.query_builder.build_query(clause)
        self.assertEqual(csql, sql)

    def test_create_table(self):
        clause = ModelSchemaManager(User).create_table(True, None)
        self.assertDDL(clause, (
            'CREATE TABLE IF NOT EXISTS "user" ('
            '"id" INTEGER NOT NULL PRIMARY KEY, '
            '"username" TEXT NOT NULL)'))

        clause = ModelSchemaManager(Tweet).create_table(False, None)
        self.assertDDL(clause, (
            'CREATE TABLE "tweet" ('
            '"id" INTEGER NOT NULL PRIMARY KEY, '
            '"user_id" INTEGER NOT NULL, '
            '"content" TEXT NOT NULL, '
            '"timestamp" DATETIME NOT NULL, '
            'FOREIGN KEY ("user_id") REFERENCES "user" ("id"))'))

    def test_self_referential_foreign_key(self):
        clause = ModelSchemaManager(Category).create_table(True, None)
        self.assertDDL(clause, (
            'CREATE TABLE IF NOT EXISTS "category" ('
            '"id" INTEGER NOT NULL PRIMARY KEY, '
            '"name" TEXT NOT NULL, '
            '"parent_id" INTEGER, '
            'FOREIGN KEY ("parent_id") REFERENCES "category" ("id"))'))

        self.assertTrue(Category.parent.rel_model is Category)
        self.assertTrue(Category.parent.rel_field is Category.id)

    def test_virtual_table(self):
        FTSContent = create_model(test_db, 'fts_content', (
            ('tweet_id', IntegerField()),
            ('content', TextField()),
        ), True, True, 'fts4', {'stem': 'porter'})
        clause = ModelSchemaManager(FTSContent).create_table(True, None)
        self.assertDDL(clause, (
            'CREATE VIRTUAL TABLE IF NOT EXISTS "fts_content" USING fts4 ('
            '"id" INTEGER NOT NULL PRIMARY KEY, '
            '"tweet_id" INTEGER NOT NULL, '
            '"content" TEXT NOT NULL, '
            'stem=porter)'))

        clause = ModelSchemaManager(FTSContent).create_table(
            True, {'foo': 'bar'})
        self.assertDDL(clause, (
            'CREATE VIRTUAL TABLE IF NOT EXISTS "fts_content" USING fts4 ('
            '"id" INTEGER NOT NULL PRIMARY KEY, '
            '"tweet_id" INTEGER NOT NULL, '
            '"content" TEXT NOT NULL, '
            'foo=bar, stem=porter)'))


class TestSQL(BaseTestCase):
    def assertSQL(self, query, sql, params=None):
        csql, cparams = query.sql()
        self.assertEqual(csql, sql)
        if params is not None:
            self.assertEqual(cparams, params)

    def test_select_query(self):
        query = (SelectQuery(test_db)
                 .select(UserTbl.username, fn.COUNT(TweetTbl.id).alias('ct'))
                 .from_(UserTbl)
                 .join(
                     UserTbl,
                     TweetTbl,
                     (UserTbl.id == TweetTbl.user_id),
                     'LEFT OUTER')
                 .where(UserTbl.username.startswith('charlie'))
                 .group_by(UserTbl.username)
                 .order_by(fn.COUNT(TweetTbl.id).desc())
                 .having(SQL('ct') > 3)
                 .limit(4))
        self.assertSQL(query, (
            'SELECT "user"."username", COUNT("tweet"."id") AS ct '
            'FROM "user" '
            'LEFT OUTER JOIN "tweet" ON ("user"."id" = "tweet"."user_id") '
            'WHERE ("user"."username" LIKE ?) '
            'GROUP BY "user"."username" '
            'HAVING (ct > ?) '
            'ORDER BY COUNT("tweet"."id") DESC '
            'LIMIT ?'),
            ('charlie%', 3, 4))

    def test_select_table_alias(self):
        UserAlt = UserTbl.alias('user_alt')
        query = (SelectQuery(test_db)
                 .select(UserTbl.username, fn.COUNT(UserAlt.id))
                 .from_(UserTbl)
                 .join(
                     UserTbl,
                     UserAlt,
                     UserTbl.username == UserAlt.username)
                 .group_by(UserTbl.username))
        self.assertSQL(query, (
            'SELECT "user"."username", COUNT("user_alt"."id") '
            'FROM "user" '
            'INNER JOIN "user" AS user_alt '
            'ON ("user"."username" = "user_alt"."username") '
            'GROUP BY "user"."username"'))

    def test_select_join_query(self):
        TweetAlias = Tweet.alias('t2')
        subquery = (SelectQuery(test_db)
                    .select(
                        TweetAlias.user,
                        fn.MAX(TweetAlias.timestamp).alias('max_ts'))
                    .from_(TweetAlias)
                    .group_by(TweetAlias.user)
                    .alias('tweet_max_subquery'))

        query = (Tweet
                 .select(Tweet, User)
                 .join(Tweet.user)
                 .join(
                     Tweet,
                     subquery,
                     ((Tweet.timestamp == subquery.c.max_ts) &
                      (Tweet.user == subquery.c.user_id))))

        self.assertSQL(query, (
            'SELECT "tweet"."id", "tweet"."user_id", "tweet"."content", '
            '"tweet"."timestamp", "user"."id", "user"."username" '
            'FROM "tweet" '
            'INNER JOIN "user" ON ("tweet"."user_id" = "user"."id") '
            'INNER JOIN ('
            'SELECT "t2"."user", MAX("t2"."timestamp") AS max_ts '
            'FROM "tweet" AS t2 '
            'GROUP BY "t2"."user") '
            'AS tweet_max_subquery ON ('
            '("tweet"."timestamp" = "tweet_max_subquery"."max_ts") AND '
            '("tweet"."user_id" = "tweet_max_subquery"."user_id"))'))

    def test_self_join(self):
        TweetAlias = Tweet.alias('t2')
        query = (Tweet
                 .select(Tweet, User)
                 .join(Tweet.user)
                 .join(
                     Tweet,
                     TweetAlias,
                     ((TweetAlias.user == User.id) &
                      (Tweet.timestamp < TweetAlias.timestamp)),
                     'LEFT OUTER')
                 .where(TweetAlias.id.is_null()))

        self.assertSQL(query, (
            'SELECT "tweet"."id", "tweet"."user_id", "tweet"."content", '
            '"tweet"."timestamp", "user"."id", "user"."username" '
            'FROM "tweet" '
            'INNER JOIN "user" ON ("tweet"."user_id" = "user"."id") '
            'LEFT OUTER JOIN "tweet" AS t2 ON ('
            '("t2"."user" = "user"."id") AND '
            '("tweet"."timestamp" < "t2"."timestamp")) '
            'WHERE ("t2"."id" IS ?)'
        ))

        Parent = Category.alias('parent')
        query = (Category
                 .select(Category, Parent.name)
                 .join(Category, Parent, (Category.parent == Parent.id)))
        self.assertSQL(query, (
            'SELECT "category"."id", "category"."name", "category"."parent_id"'
            ', "parent"."name" '
            'FROM "category" '
            'INNER JOIN "category" AS parent '
            'ON ("category"."parent_id" = "parent"."id")'))

    def test_update(self):
        query = (UpdateQuery(test_db, table=UserTbl)
                 .set(
                     username=UserTbl.username.concat('-zai'),
                     id=(UserTbl.id * 3) + 1337)
                 .where(UserTbl.username << ['foo', 'bar', 'baz'])
                 .limit(2))
        self.assertSQL(query, (
            'UPDATE "user" '
            'SET "username" = ("user"."username" || ?), '
            '"id" = (("user"."id" * ?) + ?) '
            'WHERE ("user"."username" IN (?, ?, ?)) LIMIT ?'),
            ('-zai', 3, 1337, 'foo', 'bar', 'baz', 2))

        query = (User
                 .update(username=User.select(fn.MAX(User.id).alias('max_id')))
                 .where(User.id < 100))
        self.assertSQL(query, (
            'UPDATE "user" SET "username" = ('
            'SELECT MAX("user"."id") AS max_id FROM "user") '
            'WHERE ("user"."id" < ?)'))

    def test_insert_dict(self):
        query = (InsertQuery(test_db)
                 .into(UserTbl)
                 .values({
                     'username': 'charlie',  # Either works here.
                     UserTbl.id: 100,
                 }))
        self.assertSQL(query, (
            'INSERT INTO "user" ("username", "id") VALUES (?, ?)'),
            ('charlie', 100))

        query = User.insert({User.username: 'baz'})
        self.assertSQL(query, (
            'INSERT INTO "user" ("username") VALUES (?)'),
            ('baz',))

    def test_insert_list(self):
        def generator():
            for i in range(3):
                yield {UserTbl.id: i, 'username': 'foo-%s' % i}

        query = (InsertQuery(test_db)
                 .into(UserTbl)
                 .values(rows=list(generator())))
        self.assertSQL(query, (
            'INSERT INTO "user" ("username", "id") '
            'VALUES (?, ?), (?, ?), (?, ?)'),
            ('foo-0', 0, 'foo-1', 1, 'foo-2', 2))

        query = User.insert(rows=[
            {User.username: 'u%s' % i} for i in range(3)])
        self.assertSQL(query, (
            'INSERT INTO "user" ("username") '
            'VALUES (?), (?), (?)'),
            ('u0', 'u1', 'u2'))

    def test_insert_select(self):
        query = (InsertQuery(test_db)
                 .into(UserTbl)
                 .values(query=(
                     SelectQuery(test_db)
                     .select(UserTbl.username.concat('xx'))
                     .from_(UserTbl))))
        self.assertSQL(query, (
            'INSERT INTO "user" ('
            'SELECT ("user"."username" || ?) FROM "user")'),
            ('xx',))

        query = query.columns(UserTbl.username)
        self.assertSQL(query, (
            'INSERT INTO "user" ("username") ('
            'SELECT ("user"."username" || ?) FROM "user")'),
            ('xx',))

    def test_delete(self):
        query = (DeleteQuery(test_db)
                 .from_(UserTbl)
                 .where(
                     (UserTbl.username == 'foo') |
                     ~(UserTbl.id < 100))
                 .offset(2))
        self.assertSQL(query, (
            'DELETE FROM "user" '
            'WHERE (("user"."username" = ?) OR NOT '
            '("user"."id" < ?)) '
            'OFFSET ?'),
            ('foo', 100, 2))


class TestModelAPIs(ModelTestCase):
    def test_create_read(self):
        for username in ['charlie', 'huey']:
            User.create(username=username)

        query = User.select().order_by(User.username.desc())
        self.assertEqual(
            [user.username for user in query],
            ['huey', 'charlie'])

        charlie = User.get(User.username == 'charlie')
        self.assertEqual(charlie.id, 1)
        self.assertEqual(charlie.username, 'charlie')

        huey = User.get(User.username == 'huey')
        self.assertEqual(huey.id, 2)
        self.assertEqual(huey.username, 'huey')

        charlie.username = 'charles'
        huey.username = 'huey-zai'

        with test_db.atomic():
            charlie.save()
            self.assertEqual(charlie.id, 1)

        self.assertEqual(
            [user.username for user in query.clone()],
            ['huey', 'charles'])

        huey.save()
        self.assertEqual(
            [user.username for user in query.clone()],
            ['huey-zai', 'charles'])

    def test_read_missing(self):
        query = User.select()
        self.assertEqual(list(query), [])

        self.assertRaises(DoesNotExist, User.get, User.id == 0)

        # Looking up a non-null foreign key.
        tweet = Tweet()
        self.assertRaises(DoesNotExist, lambda: tweet.user)

    def test_type_coercion(self):
        # The integer value is converted to a string by `insert()`.
        user = User.create(username=100)
        user_db = User.get(User.id == user.id)
        self.assertEqual(user_db.username, '100')

        # Similarly, the datetime will be converted to a string when
        # saving it to the database.
        test_timestamp = datetime.datetime(2015, 1, 2, 3, 4, 5, 6)
        tweet = Tweet.create(
            user=user,
            content='foo',
            timestamp=test_timestamp)
        tweet_db = Tweet.get(Tweet.id == tweet.id)
        self.assertEqual(tweet_db.user.id, user.id)
        self.assertEqual(tweet_db.user.username, '100')
        self.assertEqual(tweet_db.content, 'foo')

        # In this case, the value is de-serialized from a string to a
        # datetime when reading it from the database.
        self.assertEqual(tweet_db.timestamp, test_timestamp)

        # Even if we alias our field, it will still be correctly coerced.
        tweet = (Tweet
                 .select(Tweet, Tweet.timestamp.alias('foo'))
                 .where(Tweet.content == 'foo')
                 .get())
        self.assertEqual(tweet.timestamp, test_timestamp)
        self.assertEqual(tweet.foo, test_timestamp)

        # By using an aliased entity we get the raw value.
        tweet = (Tweet
                 .select(Tweet, TweetTbl.timestamp.alias('foo'))
                 .where(Tweet.content == 'foo')
                 .get())
        self.assertEqual(tweet.timestamp, test_timestamp)
        self.assertEqual(tweet.foo, '2015-01-02 03:04:05.000006')

    def test_model_shorthand(self):
        query = (Tweet
                 .select(Tweet, User)
                 .from_(Tweet, User)
                 .where(Tweet.user == User.id))
        sql, params = query.sql()
        self.assertEqual(sql, (
            'SELECT "tweet"."id", "tweet"."user_id", "tweet"."content", '
            '"tweet"."timestamp", "user"."id", "user"."username" '
            'FROM "tweet", "user" '
            'WHERE ("tweet"."user_id" = "user"."id")'))
        self.assertEqual(params, ())


class TestForeignKey(ModelTestCase):
    def setUp(self):
        super(TestForeignKey, self).setUp()
        self.huey = User.create(username='huey')
        self.mickey = User.create(username='mickey')
        for huey_tweet in ['meow', 'purr']:
            Tweet.create(user=self.huey, content=huey_tweet)
        woof = Tweet.create(user=self.mickey, content='woof')
        Favorite.create(user=self.huey, tweet=woof)
        Favorite.create(user=self.mickey, tweet=woof)

    def test_descriptors(self):
        """Ensure the descriptors read and write the data correctly."""
        with self.assertQueryCount(2):
            meow = Tweet.get(Tweet.content == 'meow')

            # Check that the related object is correct.
            self.assertEqual(meow.user, self.huey)
            self.assertNotEqual(meow.user, self.mickey)

            # Check that the underlying column value is correct.
            self.assertEqual(meow.user_id, self.huey.id)
            self.assertNotEqual(meow.user_id, self.mickey.id)

            # Update the foreign key by setting the model attribute.
            meow.user = self.mickey
            self.assertEqual(meow.user, self.mickey)
            self.assertEqual(meow.user_id, self.mickey.id)

        meow.save()

        with self.assertQueryCount(2):
            # Verify the changes were saved.
            meow = Tweet.get(Tweet.content == 'meow')
            self.assertEqual(meow.user, self.mickey)
            self.assertEqual(meow.user_id, self.mickey.id)

        with self.assertQueryCount(1):
            # Update the foreign key by setting the column value attribute.
            meow.user_id = self.huey.id
            self.assertEqual(meow.user_id, self.huey.id)
            self.assertEqual(meow.user, self.huey)

        # We can assign a dictionary to the foreign key descriptor, and it
        # is treated like a list of related model attributes.
        with self.assertQueryCount(0):
            meow.user = {
                'id': self.mickey.id,
                'test_attr': 'test',
                'username': 'mickey-xx'}
            self.assertEqual(meow.user_id, self.mickey.id)
            self.assertEqual(meow.user.id, self.mickey.id)
            self.assertEqual(meow.user.test_attr, 'test')
            self.assertEqual(meow.user.username, 'mickey-xx')

        # Sanity check.
        self.assertEqual(Tweet.select().count(), 3)

    def test_backrefs(self):
        tweets = self.huey.tweets
        self.assertEqual(tweets.count(), 2)
        self.assertEqual(
            [tweet.content for tweet in tweets.order_by(Tweet.id)],
            ['meow', 'purr'])

        zaizee = User.create(username='zaizee')
        self.assertEqual(zaizee.tweets.count(), 0)
        self.assertEqual(list(zaizee.tweets), [])

    def test_foreign_key_model_coerce(self):
        query = (Tweet
                 .select()
                 .where(Tweet.user == self.huey)
                 .order_by(Tweet.id))
        self.assertEqual([tweet.content for tweet in query], ['meow', 'purr'])
        self.assertEqual(
            [tweet.user.username for tweet in query],
            ['huey', 'huey'])

        query = (Tweet
                 .select()
                 .where(Tweet.user_id == self.mickey.id))
        self.assertEqual([tweet.content for tweet in query], ['woof'])
        self.assertEqual(
            [tweet.user.username for tweet in query],
            ['mickey'])

    def test_foreign_key_querying(self):
        mickey_tweets = Tweet.select().where(Tweet.user == self.mickey)
        mickey_favorites = (Favorite
                            .select()
                            .where(Favorite.tweet.in_(mickey_tweets))
                            .order_by(Favorite.id))

        # Query is lazily evaluated.
        alt = Tweet.create(user=self.mickey, content='alt')
        Favorite.create(user=self.huey, tweet=alt)

        self.assertEqual(
            [(fav.user.username, fav.tweet.user.username, fav.tweet.content)
             for fav in mickey_favorites],
            [('huey', 'mickey', 'woof'),
             ('mickey', 'mickey', 'woof'),
             ('huey', 'mickey', 'alt')])

    def test_foreign_key_aggregation(self):
        zaizee = User.create(username='zaizee')
        query = (User
                 .select(User, fn.COUNT(Tweet.id).alias('count'))
                 .join(User.tweets)
                 .group_by(User)
                 .order_by(SQL('count DESC')))
        data = [(user.username, user.count) for user in query]
        self.assertEqual(data, [
            ('huey', 2),
            ('mickey', 1),
            ('zaizee', 0)])

        subquery = (Tweet
                    .select(fn.COUNT(Tweet.id).alias('ct'))
                    .where(Tweet.user == User.id)
                    .alias('tweet_ct'))
        query = (User
                 .select(
                     User,
                     subquery)
                 .order_by(SQL('tweet_ct DESC')))
        data = [(user.username, user.tweet_ct) for user in query]
        self.assertEqual(data, [
            ('huey', 2),
            ('mickey', 1),
            ('zaizee', 0)])

    def test_dependency_resolution(self):
        accum = []
        for query, fk in reversed(list(self.huey.dependencies())):
            accum.append(fk.model.select().where(query).sql())

        self.assertEqual(accum, [
            # Comments.
            ('SELECT "comment"."id", "comment"."tweet_id", '
             '"comment"."comment", "comment"."timestamp" '
             'FROM "comment" '
             'WHERE ("comment"."tweet_id" IN ('
             'SELECT "tweet"."id" '
             'FROM "tweet" '
             'WHERE ("tweet"."user_id" = ?)))', (1,)),

            # Favorites by tweet by user (most complex).
            ('SELECT "favorite"."id", "favorite"."user_id", '
             '"favorite"."tweet_id", "favorite"."timestamp" '
             'FROM "favorite" '
             'WHERE ("favorite"."tweet_id" IN ('
             'SELECT "tweet"."id" '
             'FROM "tweet" '
             'WHERE ("tweet"."user_id" = ?)))', (1,)),

            # Favorites by user.
            ('SELECT "favorite"."id", "favorite"."user_id", '
             '"favorite"."tweet_id", "favorite"."timestamp" '
             'FROM "favorite" WHERE ("favorite"."user_id" = ?)', (1,)),

            # Tweets by user.
            ('SELECT "tweet"."id", "tweet"."user_id", "tweet"."content", '
             '"tweet"."timestamp" '
             'FROM "tweet" WHERE ("tweet"."user_id" = ?)', (1,)),
        ])

    def test_delete_recursive(self):
        huey_deps = self.huey.dependencies()
        mickey_deps = self.mickey.dependencies()

        alt = Tweet.create(user=self.huey, content='alt')
        Favorite.create(user=self.huey, tweet=alt)
        Favorite.create(user=self.mickey, tweet=alt)

        self.assertEqual(User.select().count(), 2)
        self.assertEqual(Tweet.select().count(), 4)
        self.assertEqual(Favorite.select().count(), 4)

        self.huey.delete_instance(recursive=True)

        self.assertEqual(User.select().count(), 1)
        self.assertEqual(Tweet.select().count(), 1)
        self.assertEqual(Favorite.select().count(), 1)

        self.assertEqual(
            self.list_of(User, User.username),
            [{'username': 'mickey'}])
        self.assertEqual(
            self.list_of(Tweet, Tweet.content),
            [{'content': 'woof'}])
        self.assertEqual(
            self.list_of(Favorite, Favorite.user),
            [{'user_id': self.mickey.id}])

        self.mickey.delete_instance(recursive=True)
        self.assertEqual(User.select().count(), 0)
        self.assertEqual(Tweet.select().count(), 0)
        self.assertEqual(Favorite.select().count(), 0)

    def test_select_related(self):
        with self.assertQueryCount(1):
            query = (Tweet
                     .select(Tweet.content, User.username)
                     .from_(Tweet, User)
                     .where(Tweet.user == User.id)
                     .order_by(User.username.desc(), Tweet.content.asc()))
            results = [d for d in query.dicts()]
            self.assertEqual(results, [
                {'content': 'woof', 'username': 'mickey'},
                {'content': 'meow', 'username': 'huey'},
                {'content': 'purr', 'username': 'huey'},
            ])

        with self.assertQueryCount(1):
            results = [(tweet.content, tweet.username) for tweet in query]
            self.assertEqual(results, [
                ('woof', 'mickey'),
                ('meow', 'huey'),
                ('purr', 'huey')])


class TestForeignKeySelf(ModelTestCase):
    def setUp(self):
        super(TestForeignKeySelf, self).setUp()
        self.parent = Category.create(name='p')
        for i in range(2):
            c = Category.create(name='c-%s' % i, parent=self.parent)
            for j in range(2):
                Category.create(name='gc-%s-%s' % (i, j), parent=c)

    def assertCategories(self, query, names):
        query = query.order_by(Category.id)
        self.assertEqual([category.name for category in query], names)

    def test_parent_where(self):
        query = Category.select().where(Category.parent.is_null(True))
        self.assertCategories(query, ['p'])

        query = Category.select().where(Category.parent == self.parent)
        self.assertCategories(query, ['c-0', 'c-1'])

    def test_backrefs(self):
        # Test backrefs.
        self.assertCategories(self.parent.children, ['c-0', 'c-1'])
        self.assertEqual(self.parent.children.count(), 2)

        child0 = Category.get(Category.name == 'c-0')
        self.assertCategories(child0.children, ['gc-0-0', 'gc-0-1'])

    def test_join_filter(self):
        # Test joining.
        Parent = Category.alias('parent')
        query = (Category
                 .select()
                 .join(Category, Parent, (Category.parent == Parent.id))
                 .where(Parent.parent_id == self.parent.id))
        self.assertCategories(query, ['gc-0-0', 'gc-0-1', 'gc-1-0', 'gc-1-1'])

    def test_select_related(self):
        # Test select related.
        Parent = Category.alias('parent')
        with self.assertQueryCount(1):
            query = (Category
                     .select(Category, Parent.name, Parent.parent_id)
                     .join(
                         Category,
                         Parent,
                         (Category.parent == Parent.id),
                         'LEFT OUTER')
                     .order_by(Category.id))

            results = [(cat.parent.name, cat.name) for cat in query]
            self.assertEqual(results, [
                (None, 'p'),
                ('p', 'c-0'),
                ('c-0', 'gc-0-0'),
                ('c-0', 'gc-0-1'),
                ('p', 'c-1'),
                ('c-1', 'gc-1-0'),
                ('c-1', 'gc-1-1'),
            ])


class TestJoins(ModelTestCase):
    def setUp(self):
        super(TestJoins, self).setUp()
        self.huey = User.create(username='huey')
        self.mickey = User.create(username='mickey')
        for huey_tweet in ['meow', 'purr']:
            Tweet.create(user=self.huey, content=huey_tweet)
        woof = Tweet.create(user=self.mickey, content='woof')
        Favorite.create(user=self.huey, tweet=woof)
        Favorite.create(user=self.mickey, tweet=woof)

    def test_simple_join(self):
        # Model graph is reconstructed, foreign key resolutions are free!
        with self.assertQueryCount(1):
            query = (Tweet
                     .select(Tweet, User)
                     .join(Tweet.user)
                     .order_by(Tweet.id))
            self.assertEqual([
                (tweet.user.username, tweet.content)
                 for tweet in query],
                [('huey', 'meow'), ('huey', 'purr'), ('mickey', 'woof')])

    def test_backward_join(self):
        result = [('huey', 'meow'), ('huey', 'purr'), ('mickey', 'woof')]

        with self.assertQueryCount(1):
            query = (User
                     .select(User, Tweet)
                     .join(User.tweets)
                     .order_by(Tweet.id))
            self.assertEqual([(user.username, user.tweet.content)
                              for user in query],
                             result)

        with self.assertQueryCount(1):
            query = (User
                     .select(User, Tweet)
                     .join(
                         User,
                         Tweet,
                         (User.id == Tweet.user).alias('tweezle'))
                     .order_by(Tweet.id))
            self.assertEqual([(user.username, user.tweezle.content)
                              for user in query],
                             result)

    def test_join_tables(self):
        with self.assertQueryCount(1):
            query = (Tweet
                     .select(Tweet, UserTbl.username)
                     .join(
                         Tweet,
                         UserTbl,
                         (Tweet.user == UserTbl.id))
                     .order_by(Tweet.id))
            self.assertEqual(
                [(row.user.username, row.content) for row in query],
                [('huey', 'meow'), ('huey', 'purr'), ('mickey', 'woof')])

        with self.assertQueryCount(1):
            query = (test_db
                     .select(UserTbl.username, TweetTbl.content)
                     .from_(UserTbl)
                     .join(
                         UserTbl,
                         TweetTbl,
                         (UserTbl.id == TweetTbl.user_id))
                     .order_by(TweetTbl.id.desc()))
            results = [row for row in query]
            self.assertEqual(results, [
                ('mickey', 'woof'), ('huey', 'purr'), ('huey', 'meow')])

    def test_multiple_join(self):
        with self.assertQueryCount(1):
            FavUser = User.alias('fav_user')
            query = (Favorite
                     .select(Favorite, Tweet, User, FavUser.username)
                     .join(Favorite.tweet)
                     .join(
                         Favorite,
                         FavUser,
                         (Favorite.user == FavUser.id).alias('fav_user'))
                     .join(Tweet.user)
                     .order_by(Favorite.id))

            results = []
            for fav in query:
                results.append((
                    fav.tweet.content,
                    fav.fav_user['username'],
                    fav.tweet.user.username))

            expected = [
                ('woof', 'huey', 'mickey'),
                ('woof', 'mickey', 'mickey')]
            self.assertEqual(results, expected)

    def test_self_join(self):
        root = Category.create(name='root')
        c1 = Category.create(name='c1', parent=root)
        c2 = Category.create(name='c2', parent=root)
        gc11 = Category.create(name='gc1-1', parent=c1)
        gc12 = Category.create(name='gc1-2', parent=c1)

        with self.assertQueryCount(1):
            Parent = Category.alias('parent')
            query = (Category
                     .select(Category, Parent.id, Parent.name)
                     .join(
                         Category,
                         Parent,
                         (Category.parent == Parent.id).alias('parent'),
                         'LEFT OUTER')
                     .order_by(Category.id))
            results = []
            for category in query:
                results.append((category.name, category.parent.name))

            self.assertEqual(results, [
                ('root', None),
                ('c1', 'root'),
                ('c2', 'root'),
                ('gc1-1', 'c1'),
                ('gc1-2', 'c1')])


class TestFullTextSearch(BaseTestCase):
    messages = [
        # 0.
        ('A faith is a necessity to a man. Woe to him who believes in '
         'nothing.'),

        # 1.
        ('All who call on God in true faith, earnestly from the heart, will '
         'certainly be heard, and will receive what they have asked and '
         'desired.'),

        # 2.
        ('Be faithful in small things because it is in them that your '
         'strength lies.'),

        # 3.
        ('Faith consists in believing when it is beyond the power of reason '
         'to believe.'),

        # 4.
        ('Faith has to do with things that are not seen and hope with things '
         'that are not at hand.'),
    ]

    values = [
        ('aaaaa bbbbb ccccc ddddd', 'aaaaa ccccc', 'zzzzz zzzzz', 1),
        ('bbbbb ccccc ddddd eeeee', 'bbbbb', 'zzzzz', 2),
        ('ccccc ccccc ddddd fffff', 'ccccc', 'yyyyy', 3),
        ('ddddd', 'ccccc', 'xxxxx', 4)]

    def setUp(self):
        super(TestFullTextSearch, self).setUp()
        test_db.drop_tables([Post, User, AutoFTSUser, MultiColumn], safe=True)
        test_db.create_tables([Post, User, AutoFTSUser, MultiColumn])
        for idx, message in enumerate(self.messages):
            Post.create(idx=idx, content=message)

    def tearDown(self):
        test_db.drop_tables([Post, User, AutoFTSUser, MultiColumn])
        super(TestFullTextSearch, self).tearDown()

    def assertMessages(self, query, indices):
        self.assertEqual(
            [row.content for row in query],
            [self.messages[index] for index in indices])

    def test_search(self):
        query = (Post
                 .select()
                 .where(Post.match('believe'))
                 .order_by(Post.idx))
        self.assertMessages(query, [0, 3])

        query = Post.search('believe')
        self.assertMessages(query, [3, 0])

        query = Post.search('things')
        about = lambda n: round(n, 3)
        self.assertEqual(
            [(post.content, about(post.score)) for post in query],
            [(self.messages[4], about(2.0 / 3)),
             (self.messages[2], about(1.0 / 3))])

    def test_external_content(self):
        users = []
        for message in self.messages:
            users.append(User.create(username=message))

        # Nothing matches, index is not built.
        pq = AutoFTSUser.select().where(AutoFTSUser.match('faith'))
        self.assertEqual(list(pq), [])

        AutoFTSUser.rebuild()
        AutoFTSUser.optimize()

        # it will stem faithful -> faith b/c we use the porter tokenizer
        def assertResults(query, expected):
            results = [' '.join(result.username.split()[:2]).lower()
                       for result in query]
            self.assertEqual(results, expected)

        assertResults(AutoFTSUser.search('faith'), [
            'a faith',
            'all who',
            'be faithful',
            'faith consists',
            'faith has',
        ])

        assertResults(AutoFTSUser.search('believe'), [
            'faith consists',
            'a faith',
        ])

        assertResults(AutoFTSUser.search('thin*'), [
            'faith has',
            'be faithful',
        ])

        assertResults(AutoFTSUser.search('"it is"'), [
            'be faithful',
            'faith consists',
        ])

        query = AutoFTSUser.search('things')
        about = lambda n: round(n, 3)
        self.assertEqual(
            [(result.username, about(result.score)) for result in query],
            [(self.messages[4], about(2.0 / 3)),
             (self.messages[2], about(1.0 / 3))])

        pq = (AutoFTSUser
              .select(AutoFTSUser.rank())
              .where(AutoFTSUser.match('faithful'))
              .tuples())
        self.assertEqual([about(x[0]) for x in pq], [about(.2)] * 5)

        pq = (AutoFTSUser
              .search('faithful')
              .dicts())
        self.assertEqual([about(x['score']) for x in pq], [about(.2)] * 5)

    def test_external_content_deletion(self):
        for message in self.messages:
            User.create(username=message)

        AutoFTSUser.rebuild()
        query = AutoFTSUser.search('believe')
        self.assertEqual(
            [self.messages.index(user.username) for user in query],
            [3, 0])

        self.assertEqual(AutoFTSUser.delete().execute(), 5)
        self.assertEqual(AutoFTSUser.select().count(), 5)  # !!!

        user = User.get(User.username == self.messages[0])
        self.assertEqual(user.delete_instance(), 1)
        self.assertEqual(AutoFTSUser.select().count(), 4)  # !!!

        AutoFTSUser.rebuild()
        query = AutoFTSUser.search('believe')
        self.assertEqual(
            [self.messages.index(user.username) for user in query],
            [3])

    def test_multi_content(self):
        for c1, c2, c3, c4 in self.values:
            MultiColumn.create(c1=c1, c2=c2, c3=c3, c4=c4)

        def assertResults(term, expected):
            results = [
                (x.c4, round(x.score, 2))
                for x in MultiColumn.search(term)]
            self.assertEqual(results, expected)

        # `bbbbb` appears two times in `c1`, one time in `c2`.
        assertResults('bbbbb', [
            (2, 1.5),  # 1/2 + 1/1
            (1, 0.5),  # 1/2
        ])

        # `ccccc` appears four times in `c1`, three times in `c2`.
        assertResults('ccccc', [
            (3, .83),  # 2/4 + 1/3
            (1, .58), # 1/4 + 1/3
            (4, .33), # 1/3
            (2, .25), # 1/4
        ])

        # `zzzzz` appears three times in c3.
        assertResults('zzzzz', [
            (1, .67),
            (2, .33),
        ])

        self.assertEqual(
            [x.score for x in MultiColumn.search('ddddd')],
            [.25, .25, .25, .25])

    def test_bm25(self):
        for c1, c2, c3, c4 in self.values:
            MultiColumn.create(c1=c1, c2=c2, c3=c3, c4=c4)

        def assertResults(term, col_idx, expected):
            query = MultiColumn.search_bm25(term, MultiColumn.c1)
            self.assertEqual(
                [(mc.c4, round(mc.score, 2)) for mc in query],
                expected)

        MultiColumn.create(c1='aaaaa fffff', c4=5)

        assertResults('aaaaa', 1, [
            (5, 0.39),
            (1, 0.3),
        ])
        assertResults('fffff', 1, [
            (5, 0.39),
            (3, 0.3),
        ])
        assertResults('eeeee', 1, [
            (2, 0.97),
        ])

        # No column specified, use the first text field.
        query = MultiColumn.search_bm25('fffff', MultiColumn.c1)
        self.assertEqual([(mc.c4, round(mc.score, 2)) for mc in query], [
            (5, 0.39),
            (3, 0.3),
        ])

        # Use helpers.
        query = (MultiColumn
                 .select(
                     MultiColumn.c4,
                     MultiColumn.bm25(MultiColumn.c1).alias('score'))
                 .where(MultiColumn.match('aaaaa'))
                 .order_by(SQL('score').desc()))
        self.assertEqual([(mc.c4, round(mc.score, 2)) for mc in query], [
            (5, 0.39),
            (1, 0.3),
        ])

    def test_bm25_alt_corpus(self):
        def assertResults(term, expected):
            query = Post.search_bm25(term, Post.content)
            cleaned = [
                (round(doc.score, 2), ' '.join(doc.content.split()[:2]))
                for doc in query]
            self.assertEqual(cleaned, expected)

        assertResults('things', [
            (0.45, 'Faith has'),
            (0.36, 'Be faithful'),
        ])

        # Indeterminate order since all are 0.0. All phrases contain the word
        # faith, so there is no meaningful score.
        results = [x.score for x in Post.search_bm25('faith', Post.content)]
        self.assertEqual(results, [0., 0., 0., 0., 0.])


class TestDeclarative(BaseTestCase):
    def test_basics(self):
        class BaseModel(Declarative):
            class Meta:
                database = test_db

            def foo(self):
                return 'foo!'

        class Person(BaseModel):
            name = TextField()
            dob = DateField()

            class Meta:
                table_name = 'people'

        self.assertEqual(Person._meta.database, test_db)
        self.assertEqual(Person._meta.extension, '')
        self.assertEqual(Person._meta.options, None)
        self.assertEqual(Person._meta.table_name, 'people')
        self.assertEqual(
            [field.name for field in Person._meta.sorted_fields],
            ['id', 'name', 'dob'])

        p = Person(name='huey', dob=datetime.date(2008, 1, 2))
        self.assertEqual(p.name, 'huey')
        self.assertEqual(p.dob, datetime.date(2008, 1, 2))
        self.assertIsNone(p.id)
        self.assertEqual(p.foo(), 'foo!')

    def test_complex_inheritance(self):
        class BaseModel(Declarative):
            class Meta:
                database = test_db

            def foo(self):
                return 'foo1'

        class Timestamped(BaseModel):
            timestamp = DateTimeField(default=datetime.datetime.now)

            def foo(self):
                return 'foo2'

        class Searchable(BaseModel):
            class Meta:
                extension = 'fts4'
                options = {'tokenize': 'porter'}

        class ContentMixin(object):
            content = TextField()

            def foo(self):
                return 'foo3'

        class Note(ContentMixin, Timestamped):
            class Meta:
                table_name = 'notes'

        class FTSNote(ContentMixin, Searchable):
            class Meta:
                primary_key = False

        self.assertEqual(Note._meta.database, test_db)
        self.assertEqual(Note._meta.extension, '')
        self.assertEqual(Note._meta.options, None)
        self.assertEqual(sorted(Note._meta.fields.keys()),
                         ['content', 'id', 'timestamp'])
        self.assertEqual(
            [field.name for field in Note._meta.sorted_fields],
            ['id', 'content', 'timestamp'])
        self.assertEqual(Note().foo(), 'foo3')

        self.assertEqual(FTSNote._meta.database, test_db)
        self.assertEqual(FTSNote._meta.extension, 'fts4')
        self.assertEqual(FTSNote._meta.options, {'tokenize': 'porter'})
        self.assertEqual(
            [field.name for field in FTSNote._meta.sorted_fields],
            ['content'])
        self.assertEqual(FTSNote().foo(), 'foo3')

    def test_sorting_models(self):
        class Base(Declarative):
            class Meta:
                database = test_db

        FKF = ForeignKeyField
        class A(Base): pass
        class B(Base): a = FKF(A)
        class C(Base): a, b = FKF(A), FKF(B)
        class D(Base): c = FKF(C)
        class E(Base): d = FKF(D)
        class F(Base): a, d = FKF(A), FKF(D)
        class Excluded(Base): e = FKF(E)

        models = [F, A, B, C, D, E]
        accum = test_db.sort_models(models)
        self.assertEqual(accum, [A, B, C, D, E, F])


class TestCursorWrappers(ModelTestCase):
    MODELS = [User, Tweet, Comment, Category]

    def setUp(self):
        super(TestCursorWrappers, self).setUp()
        data = (
            ('huey', 3),
            ('mickey', 1),
            ('zaizee', 0))
        for username, ntweets in data:
            user = User.create(username=username)
            for i in range(ntweets):
                Tweet.create(content='%s-%s' % (username, i), user=user)

        self.basic = User.select().order_by(User.username)
        self.tweet_join = (Tweet
                           .select(Tweet.content, User.username)
                           .join(Tweet.user)
                           .order_by(User.username, Tweet.id))
        self.aggregate = (User
                          .select(User, fn.COUNT(Tweet.id).alias('count'))
                          .join(User.tweets)
                          .group_by(User)
                          .order_by(fn.COUNT(Tweet.id)))
        self.tables = (Tweet
                       .select(Tweet.content, UserTbl.username)
                       .join(Tweet, UserTbl, (Tweet.user == UserTbl.id))
                       .order_by(UserTbl.username, Tweet.id))

        self.basic_data = [
            (1, 'huey'),
            (2, 'mickey'),
            (3, 'zaizee')]
        self.tweet_join_data = [
            ('huey-0', 'huey'),
            ('huey-1', 'huey'),
            ('huey-2', 'huey'),
            ('mickey-0', 'mickey')]
        self.aggregate_data = [
            (3, 'zaizee', 0),
            (2, 'mickey', 1),
            (1, 'huey', 3)]
        self.tables_data = [
            ('huey-0', 'huey'),
            ('huey-1', 'huey'),
            ('huey-2', 'huey'),
            ('mickey-0', 'mickey')]

    def assertResults(self, query, tuple_data, row_transform=None):
        row_transform = row_transform or (lambda x: x)
        with self.assertQueryCount(1):
            data = [row_transform(row) for row in query]
            self.assertEqual(data, tuple_data)

    def _test_wrapper(self, qwrap=None, transform=None, transform_args=None):
        identity = lambda x: x
        qwrap = qwrap or identity
        transform = transform or (lambda *args: identity)
        transform_args = transform_args or [(), (), (), ()]

        self.assertResults(
            qwrap(self.basic),
            self.basic_data,
            transform(*transform_args[0]))
        self.assertResults(
            qwrap(self.tweet_join),
            self.tweet_join_data,
            transform(*transform_args[1]))
        self.assertResults(
            qwrap(self.aggregate),
            self.aggregate_data,
            transform(*transform_args[2]))
        self.assertResults(
            qwrap(self.tables),
            self.tables_data,
            transform(*transform_args[3]))

    def test_tuples(self):
        self._test_wrapper(lambda query: query.tuples())

    def test_dicts(self):
        def dict_to_tuple(*keys):
            def transform(row):
                return tuple([row[key] for key in keys])
            return transform
        self._test_wrapper(
            lambda query: query.dicts(),
            dict_to_tuple,
            [('id', 'username'),
             ('content', 'username'),
             ('id', 'username', 'count'),
             ('content', 'username')])

    def test_namedtuples(self):
        self._test_wrapper(lambda query: query.namedtuples())

    def test_constructor(self):
        def obj_to_tuple(*attrs):
            def transform(row_obj):
                return tuple([getattr(row_obj, attr) for attr in attrs])
            return transform

        class FakeModel(Declarative):
            pass

        self._test_wrapper(
            lambda query: query.constructor(FakeModel),
            obj_to_tuple,
            [('id', 'username'),
             ('content', 'username'),
             ('id', 'username', 'count'),
             ('content', 'username')])

    def test_models(self):
        def model_to_tuple(ModelClass, *paths):
            def resolve_path(obj, path):
                if not path:
                    return obj
                parts = path.split('.', 1)
                obj = getattr(obj, parts[0])
                return resolve_path(obj, '.'.join(parts[1:]))

            def transform(row_obj):
                self.assertTrue(isinstance(row_obj, ModelClass))
                accum = []
                return tuple([resolve_path(row_obj, path) for path in paths])
            return transform

        self._test_wrapper(
            None,
            model_to_tuple,
            [(User, 'id', 'username'),
             (Tweet, 'content', 'user.username'),
             (User, 'id', 'username', 'count'),
             (Tweet, 'content', 'user.username')])

    def test_row_cache(self):
        huey, mickey, zaizee = User.select().order_by(User.id)

        with self.assertQueryCount(1):
            query = User.select().order_by(User.id)
            wrapper = query.execute()

            self.assertFalse(wrapper.populated)
            self.assertEqual(wrapper.row_cache, [])

            wrapper.fill_cache(1)
            self.assertFalse(wrapper.populated)
            self.assertEqual(wrapper.row_cache, [huey])

            wrapper.fill_cache(3)
            self.assertFalse(wrapper.populated)
            self.assertEqual(wrapper.row_cache, [huey, mickey, zaizee])

            wrapper.fill_cache(4)
            self.assertTrue(wrapper.populated)
            self.assertEqual(wrapper.row_cache, [huey, mickey, zaizee])

        with self.assertQueryCount(1):
            query = User.select().order_by(User.id)
            wrapper = query.execute()

            self.assertEqual(wrapper[0], huey)
            self.assertEqual(wrapper[-1], zaizee)
            self.assertEqual(wrapper[0:3], [huey, mickey, zaizee])
            self.assertEqual(wrapper[0:3:2], [huey, zaizee])
            self.assertEqual(wrapper[:2], [huey, mickey])
            self.assertEqual(wrapper[1:], [mickey, zaizee])
            self.assertEqual(wrapper[4:], [])
            self.assertRaises(IndexError, lambda: wrapper[4])

    def test_iteration(self):
        nums = range(1, 11)
        for i in nums:
            Category.create(name='c%s' % i)

        with self.assertQueryCount(1):
            query = Category.select()
            wrapper = query.execute()

            first_five = []
            for i, c in enumerate(wrapper):
                first_five.append(c.name)
                if i == 4:
                    break

            self.assertEqual(first_five, ['c1', 'c2', 'c3', 'c4', 'c5'])

            # Performing another iteration instantiates a new ResultIterator
            # object pointing at index 0. The iterator will yield from the
            # result cache until it runs out, then pull from the cursor.
            another_iter = [c.name for c in wrapper]
            self.assertEqual(another_iter, ['c%s' % i for i in nums])

            another_iter = [c.name for c in wrapper]
            self.assertEqual(another_iter, ['c%s' % i for i in nums])

    def test_iteration_protocol(self):
        huey, mickey, zaizee = User.select().order_by(User.id)

        with self.assertQueryCount(1):
            query = User.select().order_by(User.id)
            wrapper = query.execute()
            wrapper_id = id(wrapper)

            it = iter(wrapper)
            for user in it:
                pass

            self.assertEqual(wrapper.row_cache, [huey, mickey, zaizee])

            self.assertRaises(StopIteration, next, it)
            self.assertEqual(list(it), [])

            it = iter(wrapper)
            self.assertEqual(
                [user.username for user in it],
                ['huey', 'mickey', 'zaizee'])
            self.assertRaises(StopIteration, next, it)

            wrapper_2 = query.execute()
            self.assertEqual(wrapper_id, id(wrapper_2))

            self.assertEqual(query[0].username, 'huey')
            self.assertEqual(query[2].username, 'zaizee')
            self.assertEqual(query[::2], [huey, zaizee])

    def test_one_pass_iterators(self):
        with self.assertQueryCount(1):
            query = User.select().order_by(User.id)
            it = query.iterator()
            wrapper = query.execute()

            usernames = [user.username for user in it]
            self.assertEqual(usernames, ['huey', 'mickey', 'zaizee'])
            self.assertEqual(wrapper.row_cache, [])
            self.assertTrue(wrapper.populated)
            self.assertEqual(list(it), [])
            self.assertEqual(list(wrapper.iterator()), [])

        with self.assertQueryCount(0):
            again = [u.username for u in query.iterator()]
            self.assertEqual(again, [])

    def test_select_multiple(self):
        huey, mickey, zaizee = User.select().order_by(User.id)

        # Huey has one comment on his first and third tweet.
        for i, tweet in enumerate(huey.tweets):
            if i % 2 == 0:
                Comment.create(tweet=tweet, comment='c-%s' % tweet.content)

        # Mickey has 4 comments on his single tweet.
        for tweet in mickey.tweets:
            for i in range(4):
                Comment.create(tweet=tweet, comment='c-%s' % tweet.content)

        comments = (Comment
                    .select(Comment.comment, Tweet.id, Tweet.content)
                    .join(Comment.tweet)
                    .where(Tweet.user == mickey)
                    .order_by(Comment.id))
        expected = ['mickey-0'] * 4
        with self.assertQueryCount(1):
            self.assertEqual([c.tweet.content for c in comments], expected)

        comments = (Comment
                    .select(Comment.comment, Tweet.content)
                    .join(Comment.tweet)
                    .where(Tweet.user == mickey)
                    .order_by(Comment.id))
        with self.assertQueryCount(1):
            self.assertEqual([c.tweet.content for c in comments], expected)

        comments = (Comment
                    .select(Comment, Tweet, User)
                    .join(Comment.tweet)
                    .join(Tweet.user)
                    .where(User.username == 'huey')
                    .order_by(Comment.id))
        with self.assertQueryCount(1):
            self.assertEqual(
                [c.comment for c in comments],
                ['c-huey-0', 'c-huey-2'])
            self.assertEqual(
                [c.tweet.content for c in comments],
                ['huey-0', 'huey-2'])
            self.assertEqual(
                [c.tweet.user.username for c in comments],
                ['huey', 'huey'])

        with self.assertQueryCount(1):
            comments = comments.select(Comment.id, Tweet.id, User.username)
            self.assertEqual(
                [c.tweet.user.username for c in comments],
                ['huey', 'huey'])

        comments = (Comment
                    .select(Comment, Tweet, User)
                    .join(
                        Comment,
                        Tweet,
                        (Comment.tweet == Tweet.id).alias('tx'))
                    .join(
                        Tweet,
                        User,
                        (Tweet.user == User.id).alias('ux'))
                    .where(User.username == 'huey')
                    .order_by(Comment.id))
        with self.assertQueryCount(1):
            self.assertEqual(
                [c.comment for c in comments],
                ['c-huey-0', 'c-huey-2'])
            self.assertEqual(
                [c.tx.content for c in comments],
                ['huey-0', 'huey-2'])
            self.assertEqual(
                [c.tx.ux.username for c in comments],
                ['huey', 'huey'])

        with self.assertQueryCount(1):
            comments = comments.constructor(Comment)
            self.assertEqual(
                [c.comment for c in comments],
                ['c-huey-0', 'c-huey-2'])
            self.assertEqual(
                [c.content for c in comments],
                ['huey-0', 'huey-2'])
            self.assertEqual(
                [c.username for c in comments],
                ['huey', 'huey'])

    def test_type_conversion(self):
        query = UpperUser.select().order_by(UpperUser.id)
        self.assertEqual(
            [user.username for user in query],
            ['HUEY', 'MICKEY', 'ZAIZEE'])

        query = query.select(UpperUser.username.alias('foo'))
        self.assertEqual(
            [user.foo for user in query],
            ['HUEY', 'MICKEY', 'ZAIZEE'])

    def test_type_conversion_aggregate(self):
        base = UpperUser.select(fn.MAX(UpperUser.username))

        max_username = base.scalar(convert=True)
        self.assertEqual(max_username, 'ZAIZEE')

        # Detect the column name of the function call matches a field.
        Tbl = UpperUser._meta.table
        max_username = (base
                        .select(fn.MAX(Tbl.username))
                        .scalar(convert=True))
        self.assertEqual(max_username, 'ZAIZEE')

        # Indicate the function should not allow the value to be altered.
        max_username = (base
                        .select(fn.MAX(UpperUser.username).coerce(False))
                        .scalar(convert=True))
        self.assertEqual(max_username, 'zaizee')

        # Call `scalar()` without specifying value conversion.
        max_username = base.scalar()
        self.assertEqual(max_username, 'zaizee')

    def test_type_conversion_function(self):
        substr = fn.SUBSTR(UpperUser.username, 1, 3)

        def assertNames(query, expected, attr='username'):
            return self.assertEqual(
                [getattr(obj, attr) for obj in query.order_by(UpperUser.id)],
                expected)

        assertNames(
            UpperUser.select(substr.alias('foo')),
            ['HUE', 'MIC', 'ZAI'],
            'foo')

        # Because it's named `username`, we get the default converter.
        assertNames(
            UpperUser.select(substr.coerce(False).alias('username')),
            ['HUE', 'MIC', 'ZAI'])

        # Because it's named `username`, we get the default converter.
        assertNames(
            UpperUser.select(substr.coerce(False).alias('bar')),
            ['hue', 'mic', 'zai'],
            'bar')


if __name__ == '__main__':
    unittest.main(argv=sys.argv)
