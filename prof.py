import datetime
import time
import peewee as pw
import sweepea as sw
from playhouse import apsw_ext as aw


peewee_db = pw.SqliteDatabase(':memory:')
peewee_apsw_db = aw.APSWDatabase(':memory:')
sweepea_db = sw.Database()


class PAuthor(pw.Model):
    name = pw.CharField()

    class Meta:
        database = peewee_db

class PNote(pw.Model):
    content = pw.TextField()
    timestamp = pw.DateTimeField(default=datetime.datetime.now)
    tags = pw.CharField()
    deleted = pw.BooleanField(default=False)
    author = pw.ForeignKeyField(PAuthor)

    class Meta:
        database = peewee_db

class AAuthor(aw.Model):
    name = pw.CharField()

    class Meta:
        database = peewee_apsw_db

class ANote(aw.Model):
    content = aw.TextField()
    timestamp = aw.DateTimeField(default=datetime.datetime.now)
    tags = aw.CharField()
    deleted = aw.BooleanField(default=False)
    author = aw.ForeignKeyField(AAuthor)

    class Meta:
        database = peewee_apsw_db

SAuthor = sw.create_model(sweepea_db, 'sauthor', (
    ('name', sw.TextField()),
))

SNote = sw.create_model(sweepea_db, 'snote', (
    ('content', sw.TextField()),
    ('timestamp', sw.DateTimeField(default=datetime.datetime.now)),
    ('tags', sw.TextField()),
    ('deleted', sw.BooleanField(default=False)),
    ('author', sw.ForeignKeyField(SAuthor)),
))


def create():
    AAuthor.create_table()
    ANote.create_table()
    PAuthor.create_table()
    PNote.create_table()
    SAuthor.create_table()
    SNote.create_table()

def drop():
    ANote.drop_table()
    AAuthor.drop_table()
    PNote.drop_table()
    PAuthor.drop_table()
    SNote.drop_table()
    SAuthor.drop_table()


MODELS = (
    ('peewee', PAuthor, PNote, peewee_db),
    ('peewee+apsw', AAuthor, ANote, peewee_apsw_db),
    ('sweepea', SAuthor, SNote, sweepea_db),
)


def timed():
    def _timed(fn):
        def inner(*args, **kwargs):
            for label, Author, Note, db in MODELS:
                start = time.time()
                fn(Author, Note, db, *args, **kwargs)
                duration = time.time() - start
                print '%24s %12s : %0.3f' % (fn.__name__, label, duration)
            print
        return inner
    return _timed


@timed()
def test_write(Author, Note, db):
    author = Author.create(name='a')
    for i in range(20000):
        Note.create(
            content='note-%s' % i,
            tags=str(i),
            deleted=i % 5 == 0,
            author=author)


@timed()
def test_write_tbl(Author, Note, db):
    author = Author.create(name='a')
    NoteTbl = Note._meta.table
    if db == sweepea_db:
        for i in range(20000):
            db.insert({
                NoteTbl.content: 'note-%s' % i,
                NoteTbl.tags: str(i),
                NoteTbl.deleted: i % 5 == 0,
                NoteTbl.author_id: author.id})
    else:
        for i in range(20000):
            Note.create(
                content='note-%s' % i,
                tags=str(i),
                deleted=i % 5 == 0,
                author=author)


@timed()
def test_read(Author, Note, db):
    for i in range(2):
        for note in Note.select():
            pass


@timed()
def test_read_dicts(Author, Note, db):
    for i in range(2):
        for note in Note.select().dicts():
            pass


@timed()
def test_read_dicts_tbl(Author, Note, db):
    if db == sweepea_db:
        NoteTbl = Note._meta.table
        for i in range(2):
            query = (db
                     .select(NoteTbl.id, NoteTbl.content, NoteTbl.tags,
                             NoteTbl.deleted, NoteTbl.author_id)
                     .from_(NoteTbl))
            for data in query.dicts():
                pass
    else:
        for i in range(2):
            for note in Note.select().dicts():
                pass


@timed()
def test_read_tuples(Author, Note, db):
    for i in range(2):
        for note in Note.select().tuples():
            pass


@timed()
def test_read_join(Author, Note, db):
    for i in range(2):
        if db == sweepea_db:
            query = (Note
                     .select(Note, Author.name)
                     .join(Note.author))
        else:
            query = (Note
                     .select(Note, Author.name)
                     .join(Author))
        for note in query:
            note.author


@timed()
def test_filter(Author, Note, db):
    for i in range(20):
        query = (Note
                 .select()
                 .where(
                     (Note.deleted == True) &
                     (Note.content.contains('te-10')))
                 .limit(100))
        items = list(query)

        query = (Note
                 .select()
                 .where(Note.tags << ['1', '10', '100', '1000', '10000']))
        items = list(query)


@timed()
def test_read_update(Author, Note, db):
    with db.atomic():
        for note in Note.select():
            note.content='xx'
            note.save()


if __name__ == '__main__':
    create()
    test_write()
    drop()
    create()
    test_write_tbl()

    test_read()
    test_read_dicts()
    test_read_dicts_tbl()
    test_read_tuples()
    test_read_join()
    test_filter()
    test_read_update()
