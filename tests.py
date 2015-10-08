#!/usr/bin/env python

import datetime
import os
import sys
import tempfile
import unittest

from sweepea import *


if sys.version_info[:2] <= (2, 6):
    raise RuntimeError('Get a newer version of Python to run the tests.')


test_db_file = os.environ.get('SWEEPEA_TEST_DATABASE') or tempfile.mktemp()

def make_database():
    return Database(test_db_file)

test_db = make_database()


User = create_model(test_db, 'user', (
    ('username', TextField()),
))

Tweet = create_model(test_db, 'tweet', (
    ('user', ForeignKeyField(User, column='user_id', backref='tweets')),
    ('content', TextField(default='')),
    ('timestamp', DateTimeField(default=datetime.datetime.now)),
))

Favorite = create_model(test_db, 'favorite', (
    ('user', ForeignKeyField(User, column='user_id', backref='favorites')),
    ('tweet', ForeignKeyField(Tweet, column='tweet_id')),
    ('timestamp', DateTimeField(default=datetime.datetime.now)),
))


class BaseTestCase(unittest.TestCase):
    def setUp(self):
        Favorite.drop_table(True)
        Tweet.drop_table(True)
        User.drop_table(True)
        User.create_table()
        Tweet.create_table()
        Favorite.create_table()

    def tearDown(self):
        test_db.close()
        os.unlink(test_db_file)

    def refresh(self, obj):
        return type(obj).get(
            obj._meta.primary_key == getattr(obj, obj._meta.primary_key.name))

    def list_of(self, model, field):
        return list(model.select(field).order_by(field).dicts())


class TestModelAPIs(BaseTestCase):
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


class TestForeignKey(BaseTestCase):
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

        # Verify the changes were saved.
        meow = Tweet.get(Tweet.content == 'meow')
        self.assertEqual(meow.user, self.mickey)
        self.assertEqual(meow.user_id, self.mickey.id)

        # Update the foreign key by setting the column value attribute.
        meow.user_id = self.huey.id
        self.assertEqual(meow.user_id, self.huey.id)
        self.assertEqual(meow.user, self.huey)

        # Sanity check.
        self.assertEqual(Tweet.select().count(), 3)

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

    def test_dependency_resolution(self):
        accum = []
        for query, fk in reversed(list(self.huey.dependencies())):
            accum.append(fk.model.select().where(query).sql())

        self.assertEqual(accum, [
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


if __name__ == '__main__':
    unittest.main(argv=sys.argv)
