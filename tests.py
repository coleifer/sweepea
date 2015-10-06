import datetime
import os
import sys
import tempfile
import unittest

from sweepea import *


test_db_file = os.environ.get('SWEEPEA_TEST_DATABASE') or tempfile.mktemp()

def make_database():
    return Database(test_db_file)

test_db = make_database()


User = create_model(test_db, 'user', (
    ('username', TextField()),
))
Tweet = create_model(test_db, 'tweet', (
    ('user_id', ForeignKeyField(User)),
    ('content', TextField(default='')),
    ('timestamp', DateTimeField(default=datetime.datetime.now)),
))


class BaseTestCase(unittest.TestCase):
    def setUp(self):
        test_db.execute_sql(
            'CREATE TABLE user ('
            'id INTEGER NOT NULL PRIMARY KEY, '
            'username TEXT NOT NULL)')
        test_db.execute_sql(
            'CREATE TABLE tweet ('
            'id INTEGER NOT NULL PRIMARY KEY, '
            'user_id INTEGER NOT NULL REFERENCES user (id), '
            'content TEXT NOT NULL, '
            'timestamp DATETIME NOT NULL)')

    def tearDown(self):
        test_db.execute_sql('DROP TABLE tweet;')
        test_db.execute_sql('DROP TABLE user;')


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

    def test_type_coercion(self):
        user = User.create(username=100)
        user_db = User.get(User.id == user.id)
        self.assertEqual(user_db.username, '100')

        test_timestamp = datetime.datetime(2015, 1, 2, 3, 4, 5, 6)
        tweet = Tweet.create(
            user_id=user.id,
            content='foo',
            timestamp=test_timestamp)
        tweet_db = Tweet.get(Tweet.id == tweet.id)
        self.assertEqual(tweet_db.user_id, user.id)
        self.assertEqual(tweet_db.content, 'foo')
        self.assertEqual(tweet_db.timestamp, test_timestamp)


if __name__ == '__main__':
    unittest.main(argv=sys.argv)
