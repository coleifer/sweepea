import datetime
import time
import peewee as pw
import sweepea as sw
from playhouse import apsw_ext as aw


peewee_db = pw.SqliteDatabase(':memory:')
peewee_apsw_db = aw.APSWDatabase(':memory:')
sweepea_db = sw.Database()


class PNote(pw.Model):
    content = pw.TextField()
    timestamp = pw.DateTimeField(default=datetime.datetime.now)
    tags = pw.CharField()
    deleted = pw.BooleanField(default=False)

    class Meta:
        database = peewee_db


class ANote(aw.Model):
    content = aw.TextField()
    timestamp = aw.DateTimeField(default=datetime.datetime.now)
    tags = aw.CharField()
    deleted = aw.BooleanField(default=False)

    class Meta:
        database = peewee_apsw_db


SNote = sw.create_model(sweepea_db, 'pnote', (
    ('content', sw.TextField()),
    ('timestamp', sw.DateTimeField(default=datetime.datetime.now)),
    ('tags', sw.TextField()),
    ('deleted', sw.BooleanField(default=False)),
))


def create():
    ANote.create_table()
    PNote.create_table()
    psql = PNote.sqlall()
    for query in psql:
        sweepea_db.execute_sql(query)

def drop():
    ANote.drop_table()
    PNote.drop_table()
    sweepea_db.execute_sql('DROP TABLE pnote;')


NOTES = (
    ('peewee', PNote, peewee_db),
    ('peewee+apsw', ANote, peewee_apsw_db),
    ('sweepea', SNote, sweepea_db),
)


def timed():
    def _timed(fn):
        def inner(*args, **kwargs):
            for label, Note, db in NOTES:
                start = time.time()
                fn(Note, db, *args, **kwargs)
                duration = time.time() - start
                print '%24s %12s : %0.3f' % (fn.__name__, label, duration)
            print
        return inner
    return _timed


@timed()
def test_write(Note, db):
    for i in range(20000):
        Note.create(content='note-%s' % i, tags=str(i), deleted=i % 5 == 0)


@timed()
def test_write_transactions(Note, db):
    for i in range(0, 20000, 1000):
        with db.atomic():
            for j in range(i, i + 1000):
                Note.create(content='note-%s' % i, tags=str(i), deleted=i % 5 == 0)


@timed()
def test_read(Note, db):
    for i in range(2):
        for note in Note.select():
            pass


@timed()
def test_read_dicts(Note, db):
    for i in range(2):
        for note in Note.select().dicts():
            pass


@timed()
def test_read_update(Note, db):
    with db.atomic():
        for note in Note.select():
            note.content='xx'
            note.save()


create()
test_write()
drop()
create()
test_write_transactions()

test_read()
test_read_dicts()
test_read_update()
