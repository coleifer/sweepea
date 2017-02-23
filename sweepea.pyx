from cpython cimport datetime
from cpython.mem cimport PyMem_Free
from cpython.object cimport PyObject
from cpython.ref cimport Py_INCREF, Py_DECREF
from libc.float cimport DBL_MAX
from libc.math cimport log
from libc.stdlib cimport free, malloc, rand
from libc.string cimport memcpy, memset

from collections import namedtuple
from contextlib import contextmanager
import decimal
import hashlib
import itertools
import operator
import re
import sqlite3 as pysqlite
import struct
import threading
import uuid
from sqlite3 import DatabaseError
from sqlite3 import InterfaceError
from sqlite3 import OperationalError


cdef struct sqlite3_index_constraint:
    int iColumn
    unsigned char op
    unsigned char usable
    int iTermOffset


cdef struct sqlite3_index_orderby:
    int iColumn
    unsigned char desc


cdef struct sqlite3_index_constraint_usage:
    int argvIndex
    unsigned char omit


cdef extern from "sqlite3.h":
    ctypedef struct sqlite3:
        int busyTimeout
    ctypedef struct sqlite3_context
    ctypedef struct sqlite3_value
    ctypedef long long sqlite3_int64

    # Virtual tables.
    ctypedef struct sqlite3_module  # Forward reference.
    ctypedef struct sqlite3_vtab:
        const sqlite3_module *pModule
        int nRef
        char *zErrMsg
    ctypedef struct sqlite3_vtab_cursor:
        sqlite3_vtab *pVtab

    ctypedef struct sqlite3_index_info:
        int nConstraint
        sqlite3_index_constraint *aConstraint
        int nOrderBy
        sqlite3_index_orderby *aOrderBy
        sqlite3_index_constraint_usage *aConstraintUsage
        int idxNum
        char *idxStr
        int needToFreeIdxStr
        int orderByConsumed
        double estimatedCost
        sqlite3_int64 estimatedRows
        int idxFlags

    ctypedef struct sqlite3_module:
        int iVersion
        int (*xCreate)(sqlite3*, void *pAux, int argc, char **argv,
                       sqlite3_vtab **ppVTab, char**)
        int (*xConnect)(sqlite3*, void *pAux, int argc, char **argv,
                        sqlite3_vtab **ppVTab, char**)
        int (*xBestIndex)(sqlite3_vtab *pVTab, sqlite3_index_info*)
        int (*xDisconnect)(sqlite3_vtab *pVTab)
        int (*xDestroy)(sqlite3_vtab *pVTab)
        int (*xOpen)(sqlite3_vtab *pVTab, sqlite3_vtab_cursor **ppCursor)
        int (*xClose)(sqlite3_vtab_cursor*)
        int (*xFilter)(sqlite3_vtab_cursor*, int idxNum, const char *idxStr,
                       int argc, sqlite3_value **argv)
        int (*xNext)(sqlite3_vtab_cursor*)
        int (*xEof)(sqlite3_vtab_cursor*)
        int (*xColumn)(sqlite3_vtab_cursor*, sqlite3_context *, int)
        int (*xRowid)(sqlite3_vtab_cursor*, sqlite3_int64 *pRowid)
        int (*xUpdate)(sqlite3_vtab *pVTab, int, sqlite3_value **,
                       sqlite3_int64 **)
        int (*xBegin)(sqlite3_vtab *pVTab)
        int (*xSync)(sqlite3_vtab *pVTab)
        int (*xCommit)(sqlite3_vtab *pVTab)
        int (*xRollback)(sqlite3_vtab *pVTab)
        int (*xFindFunction)(sqlite3_vtab *pVTab, int nArg, const char *zName,
                             void (**pxFunc)(sqlite3_context *, int,
                                             sqlite3_value **),
                             void **ppArg)
        int (*xRename)(sqlite3_vtab *pVTab, const char *zNew)
        int (*xSavepoint)(sqlite3_vtab *pVTab, int)
        int (*xRelease)(sqlite3_vtab *pVTab, int)
        int (*xRollbackTo)(sqlite3_vtab *pVTab, int)

    cdef int sqlite3_declare_vtab(sqlite3 *db, const char *zSQL)
    cdef int sqlite3_create_module(sqlite3 *db, const char *zName,
                                   const sqlite3_module *p, void *pClientData)

    # Encoding.
    cdef int SQLITE_UTF8 = 1

    # Return values.
    cdef int SQLITE_OK = 0
    cdef int SQLITE_ERROR = 1
    cdef int SQLITE_NOMEM = 7

    # Function type.
    cdef int SQLITE_DETERMINISTIC = 0x800

    # Types of filtering operations.
    cdef int SQLITE_INDEX_CONSTRAINT_EQ = 2
    cdef int SQLITE_INDEX_CONSTRAINT_GT = 4
    cdef int SQLITE_INDEX_CONSTRAINT_LE = 8
    cdef int SQLITE_INDEX_CONSTRAINT_LT = 16
    cdef int SQLITE_INDEX_CONSTRAINT_GE = 32
    cdef int SQLITE_INDEX_CONSTRAINT_MATCH = 64

    # sqlite_value_type.
    cdef int SQLITE_INTEGER = 1
    cdef int SQLITE_FLOAT   = 2
    cdef int SQLITE3_TEXT   = 3
    cdef int SQLITE_TEXT    = 3
    cdef int SQLITE_BLOB    = 4
    cdef int SQLITE_NULL    = 5

    ctypedef void (*sqlite3_destructor_type)(void*)

    # Converting from Sqlite -> Python.
    cdef const void *sqlite3_value_blob(sqlite3_value*)
    cdef int sqlite3_value_bytes(sqlite3_value*)
    cdef double sqlite3_value_double(sqlite3_value*)
    cdef int sqlite3_value_int(sqlite3_value*)
    cdef sqlite3_int64 sqlite3_value_int64(sqlite3_value*)
    cdef const unsigned char *sqlite3_value_text(sqlite3_value*)
    cdef int sqlite3_value_type(sqlite3_value*)
    cdef int sqlite3_value_numeric_type(sqlite3_value*)

    # Converting from Python -> Sqlite.
    cdef void sqlite3_result_double(sqlite3_context*, double)
    cdef void sqlite3_result_error(sqlite3_context*, const char*, int)
    cdef void sqlite3_result_error_toobig(sqlite3_context*)
    cdef void sqlite3_result_error_nomem(sqlite3_context*)
    cdef void sqlite3_result_error_code(sqlite3_context*, int)
    cdef void sqlite3_result_int(sqlite3_context*, int)
    cdef void sqlite3_result_int64(sqlite3_context*, sqlite3_int64)
    cdef void sqlite3_result_null(sqlite3_context*)
    cdef void sqlite3_result_text(sqlite3_context*, const char*, int,
                                  void(*)(void*))
    cdef void sqlite3_result_value(sqlite3_context*, sqlite3_value*)

    # Memory management.
    cdef void* sqlite3_malloc(int)
    cdef void sqlite3_free(void *)

    cdef int sqlite3_changes(sqlite3 *db)
    cdef int sqlite3_get_autocommit(sqlite3 *db)
    cdef sqlite3_int64 sqlite3_last_insert_rowid(sqlite3 *db)

    cdef int sqlite3_create_function(
        sqlite3 *db,
        const char *zFunctionName,
        int nArg,
        int eEncoding,
        void *pApp,
        void (*xFunc)(sqlite3_context *, int, sqlite3_value **),
        void (*xStep)(sqlite3_context *, int, sqlite3_value **),
        void (*xFinal)(sqlite3_context *))
    cdef void *sqlite3_user_data(sqlite3_context *)

    cdef void *sqlite3_get_auxdata(sqlite3_context *, int N)
    cdef void sqlite3_set_auxdata(sqlite3_context *, int N, void *, void(*)(void *))

    cdef void *sqlite3_commit_hook(sqlite3 *, int(*)(void *), void *)
    cdef void *sqlite3_rollback_hook(sqlite3 *, void(*)(void *), void *)
    cdef void *sqlite3_update_hook(
        sqlite3 *,
        void(*)(void *, int, char *, char *, sqlite3_int64),
        void *)

    cdef int SQLITE_STATUS_MEMORY_USED = 0
    cdef int SQLITE_STATUS_PAGECACHE_USED = 1
    cdef int SQLITE_STATUS_PAGECACHE_OVERFLOW = 2
    cdef int SQLITE_STATUS_SCRATCH_USED = 3
    cdef int SQLITE_STATUS_SCRATCH_OVERFLOW = 4
    cdef int SQLITE_STATUS_MALLOC_SIZE = 5
    cdef int SQLITE_STATUS_PARSER_STACK = 6
    cdef int SQLITE_STATUS_PAGECACHE_SIZE = 7
    cdef int SQLITE_STATUS_SCRATCH_SIZE = 8
    cdef int SQLITE_STATUS_MALLOC_COUNT = 9
    cdef int sqlite3_status(int op, int *pCurrent, int *pHighwater, int resetFlag)

    cdef int SQLITE_DBSTATUS_LOOKASIDE_USED = 0
    cdef int SQLITE_DBSTATUS_CACHE_USED = 1
    cdef int SQLITE_DBSTATUS_SCHEMA_USED = 2
    cdef int SQLITE_DBSTATUS_STMT_USED = 3
    cdef int SQLITE_DBSTATUS_LOOKASIDE_HIT = 4
    cdef int SQLITE_DBSTATUS_LOOKASIDE_MISS_SIZE = 5
    cdef int SQLITE_DBSTATUS_LOOKASIDE_MISS_FULL = 6
    cdef int SQLITE_DBSTATUS_CACHE_HIT = 7
    cdef int SQLITE_DBSTATUS_CACHE_MISS = 8
    cdef int SQLITE_DBSTATUS_CACHE_WRITE = 9
    cdef int SQLITE_DBSTATUS_DEFERRED_FKS = 10
    cdef int SQLITE_DBSTATUS_CACHE_USED_SHARED = 11
    cdef int sqlite3_db_status(sqlite3 *, int op, int *pCur, int *pHigh, int reset)

    cdef int SQLITE_DELETE = 9
    cdef int SQLITE_INSERT = 18
    cdef int SQLITE_UPDATE = 23

    # Misc.
    cdef int sqlite3_busy_handler(sqlite3 *db, int(*)(void *, int), void *)
    cdef int sqlite3_sleep(int ms)


cdef extern from "_pysqlite/connection.h":
    ctypedef struct pysqlite_Connection:
        sqlite3* db
        double timeout
        int initialized
        PyObject* isolation_level
        char* begin_statement


# The peewee_vtab struct embeds the base sqlite3_vtab struct, and adds a field
# to store a reference to the Python implementation.
ctypedef struct peewee_vtab:
    sqlite3_vtab base
    void *table_func_cls


# Like peewee_vtab, the peewee_cursor embeds the base sqlite3_vtab_cursor and
# adds fields to store references to the current index, the Python
# implementation, the current rows' data, and a flag for whether the cursor has
# been exhausted.
ctypedef struct peewee_cursor:
    sqlite3_vtab_cursor base
    long long idx
    void *table_func
    void *row_data
    bint stopped


# We define an xConnect function, but leave xCreate NULL so that the
# table-function can be called eponymously.
cdef int pwConnect(sqlite3 *db, void *pAux, int argc, char **argv,
                   sqlite3_vtab **ppVtab, char **pzErr) with gil:
    cdef:
        int rc
        object table_func_cls = <object>pAux
        peewee_vtab *pNew

    rc = sqlite3_declare_vtab(
        db,
        'CREATE TABLE x(%s);' % table_func_cls.get_table_columns_declaration())
    if rc == SQLITE_OK:
        pNew = <peewee_vtab *>sqlite3_malloc(sizeof(pNew[0]))
        memset(<char *>pNew, 0, sizeof(pNew[0]))
        ppVtab[0] = &(pNew.base)

        pNew.table_func_cls = <void *>table_func_cls
        Py_INCREF(table_func_cls)

    return rc


cdef int pwDisconnect(sqlite3_vtab *pBase) with gil:
    cdef:
        peewee_vtab *pVtab = <peewee_vtab *>pBase
        object table_func_cls = <object>(pVtab.table_func_cls)

    Py_DECREF(table_func_cls)
    sqlite3_free(pVtab)
    return SQLITE_OK


# The xOpen method is used to initialize a cursor. In this method we
# instantiate the TableFunction class and zero out a new cursor for iteration.
cdef int pwOpen(sqlite3_vtab *pBase, sqlite3_vtab_cursor **ppCursor) with gil:
    cdef:
        peewee_vtab *pVtab = <peewee_vtab *>pBase
        peewee_cursor *pCur
        object table_func_cls = <object>pVtab.table_func_cls

    pCur = <peewee_cursor *>sqlite3_malloc(sizeof(pCur[0]))
    memset(<char *>pCur, 0, sizeof(pCur[0]))
    ppCursor[0] = &(pCur.base)
    pCur.idx = 0
    table_func = table_func_cls()
    Py_INCREF(table_func)
    pCur.table_func = <void *>table_func
    pCur.stopped = False
    return SQLITE_OK


cdef int pwClose(sqlite3_vtab_cursor *pBase) with gil:
    cdef:
        peewee_cursor *pCur = <peewee_cursor *>pBase
        object table_func = <object>pCur.table_func
    Py_DECREF(table_func)
    sqlite3_free(pCur)
    return SQLITE_OK


# Iterate once, advancing the cursor's index and assigning the row data to the
# `row_data` field on the peewee_cursor struct.
cdef int pwNext(sqlite3_vtab_cursor *pBase) with gil:
    cdef:
        peewee_cursor *pCur = <peewee_cursor *>pBase
        object table_func = <object>pCur.table_func
        tuple result

    if pCur.row_data:
        Py_DECREF(<tuple>pCur.row_data)

    try:
        result = table_func.iterate(pCur.idx)
    except StopIteration:
        pCur.stopped = True
    except:
        return SQLITE_ERROR
    else:
        Py_INCREF(result)
        pCur.row_data = <void *>result
        pCur.idx += 1
        pCur.stopped = False

    return SQLITE_OK


# Return the requested column from the current row.
cdef int pwColumn(sqlite3_vtab_cursor *pBase, sqlite3_context *ctx,
                  int iCol) with gil:
    cdef:
        peewee_cursor *pCur = <peewee_cursor *>pBase
        sqlite3_int64 x = 0
        tuple row_data

    if iCol == -1:
        sqlite3_result_int64(ctx, <sqlite3_int64>pCur.idx)
        return SQLITE_OK

    row_data = <tuple>pCur.row_data
    value = row_data[iCol]
    if value is None:
        sqlite3_result_null(ctx)
    elif isinstance(value, (int, long)):
        sqlite3_result_int64(ctx, <sqlite3_int64>value)
    elif isinstance(value, float):
        sqlite3_result_double(ctx, <double>value)
    elif isinstance(value, basestring):
        sqlite3_result_text(
            ctx,
            <const char *>value,
            -1,
            <sqlite3_destructor_type>-1)
    elif isinstance(value, bool):
        sqlite3_result_int(ctx, int(value))
    else:
        sqlite3_result_error(ctx, 'Unsupported type %s' % type(value), -1)
        return SQLITE_ERROR

    return SQLITE_OK


cdef int pwRowid(sqlite3_vtab_cursor *pBase, sqlite3_int64 *pRowid):
    cdef:
        peewee_cursor *pCur = <peewee_cursor *>pBase
    pRowid[0] = <sqlite3_int64>pCur.idx
    return SQLITE_OK


# Return a boolean indicating whether the cursor has been consumed.
cdef int pwEof(sqlite3_vtab_cursor *pBase):
    cdef:
        peewee_cursor *pCur = <peewee_cursor *>pBase
    if pCur.stopped:
        return 1
    return 0


# The filter method is called on the first iteration. This method is where we
# get access to the parameters that the function was called with, and call the
# TableFunction's `initialize()` function.
cdef int pwFilter(sqlite3_vtab_cursor *pBase, int idxNum,
                  const char *idxStr, int argc, sqlite3_value **argv) with gil:
    cdef:
        peewee_cursor *pCur = <peewee_cursor *>pBase
        object table_func = <object>pCur.table_func
        dict query = {}
        int idx
        int value_type
        tuple row_data
        void *row_data_raw

    if not idxStr or argc == 0 and len(table_func.params):
        return SQLITE_ERROR
    elif idxStr:
        params = str(idxStr).split(',')
    else:
        params = []

    for idx, param in enumerate(params):
        value = argv[idx]
        if not value:
            query[param] = None
            continue

        value_type = sqlite3_value_type(value)
        if value_type == SQLITE_INTEGER:
            query[param] = sqlite3_value_int(value)
        elif value_type == SQLITE_FLOAT:
            query[param] = sqlite3_value_double(value)
        elif value_type == SQLITE_TEXT:
            query[param] = str(sqlite3_value_text(value))
        elif value_type == SQLITE_BLOB:
            query[param] = <bytes>sqlite3_value_blob(value)
        elif value_type == SQLITE_NULL:
            query[param] = None
        else:
            query[param] = None

    table_func.initialize(**query)
    pCur.stopped = False
    try:
        row_data = table_func.iterate(0)
    except StopIteration:
        pCur.stopped = True
    else:
        Py_INCREF(row_data)
        pCur.row_data = <void *>row_data
        pCur.idx += 1
    return SQLITE_OK


# SQLite will (in some cases, repeatedly) call the xBestIndex method to try and
# find the best query plan.
cdef int pwBestIndex(sqlite3_vtab *pBase, sqlite3_index_info *pIdxInfo) \
        with gil:
    cdef:
        int i
        int idxNum = 0, nArg = 0
        peewee_vtab *pVtab = <peewee_vtab *>pBase
        object table_func_cls = <object>pVtab.table_func_cls
        sqlite3_index_constraint *pConstraint
        list columns = []
        char *idxStr
        int nParams = len(table_func_cls.params)

    pConstraint = <sqlite3_index_constraint*>0
    for i in range(pIdxInfo.nConstraint):
        pConstraint = &pIdxInfo.aConstraint[i]
        if not pConstraint.usable:
            continue
        if pConstraint.op != SQLITE_INDEX_CONSTRAINT_EQ:
            continue

        columns.append(table_func_cls.params[pConstraint.iColumn -
                                             table_func_cls._ncols])
        nArg += 1
        pIdxInfo.aConstraintUsage[i].argvIndex = nArg
        pIdxInfo.aConstraintUsage[i].omit = 1

    if nArg > 0:
        if nArg == nParams:
            # All parameters are present, this is ideal.
            pIdxInfo.estimatedCost = <double>1
            pIdxInfo.estimatedRows = 10
        else:
            # Penalize score based on number of missing params.
            pIdxInfo.estimatedCost = <double>10000000000000 * <double>(nParams - nArg)
            pIdxInfo.estimatedRows = 10 ** (nParams - nArg)

        # Store a reference to the columns in the index info structure.
        joinedCols = ','.join(columns)
        idxStr = <char *>sqlite3_malloc((len(joinedCols) + 1) * sizeof(char))
        memcpy(idxStr, <char *>joinedCols, len(joinedCols))
        idxStr[len(joinedCols)] = '\x00'
        pIdxInfo.idxStr = idxStr
        pIdxInfo.needToFreeIdxStr = 0
    else:
        pIdxInfo.estimatedCost = DBL_MAX
        pIdxInfo.estimatedRows = 100000
    return SQLITE_OK


cdef class _TableFunctionImpl(object):
    cdef:
        sqlite3_module module
        object table_function

    def __cinit__(self, table_function):
        self.table_function = table_function

    def __dealloc__(self):
        Py_DECREF(self)

    cdef create_module(self, pysqlite_Connection* sqlite_conn):
        cdef:
            sqlite3 *db = sqlite_conn.db
            int rc

        # Populate the SQLite module struct members.
        self.module.iVersion = 0
        self.module.xCreate = NULL
        self.module.xConnect = pwConnect
        self.module.xBestIndex = pwBestIndex
        self.module.xDisconnect = pwDisconnect
        self.module.xDestroy = NULL
        self.module.xOpen = pwOpen
        self.module.xClose = pwClose
        self.module.xFilter = pwFilter
        self.module.xNext = pwNext
        self.module.xEof = pwEof
        self.module.xColumn = pwColumn
        self.module.xRowid = pwRowid
        self.module.xUpdate = NULL
        self.module.xBegin = NULL
        self.module.xSync = NULL
        self.module.xCommit = NULL
        self.module.xRollback = NULL
        self.module.xFindFunction = NULL
        self.module.xRename = NULL

        # Create the SQLite virtual table.
        rc = sqlite3_create_module(
            db,
            <const char *>self.table_function.name,
            &self.module,
            <void *>(self.table_function))

        Py_INCREF(self)

        return rc == SQLITE_OK


class TableFunction(object):
    """
    Implements a table-valued function (eponymous-only virtual table) in
    SQLite.

    Subclasses must define the columns (return values) and params (input
    values) to their function. These are defined as class attributes.

    The subclass also must implement two functions:

    * initialize(**query)
    * iterate(idx)

    The `initialize` function accepts the query parameters passed in from
    the SQL query. The `iterate` function accepts the index of the current
    iteration (zero-based) and must return a tuple of result values or raise
    a `StopIteration` to signal no more results.
    """
    columns = None
    params = None
    name = None
    _ncols = None

    @classmethod
    def register(cls, conn):
        cdef _TableFunctionImpl impl = _TableFunctionImpl(cls)
        impl.create_module(<pysqlite_Connection *>conn)
        cls._ncols = len(cls.columns)

    def initialize(self, **filters):
        raise NotImplementedError

    def iterate(self, idx):
        raise NotImplementedError

    @classmethod
    def get_table_columns_declaration(cls):
        cdef list accum = []

        for column in cls.columns:
            if isinstance(column, tuple):
                if len(column) != 2:
                    raise ValueError('Column must be either a string or a '
                                     '2-tuple of name, type')
                accum.append('%s %s' % column)
            else:
                accum.append(column)

        for param in cls.params:
            accum.append('%s HIDDEN' % param)

        return ', '.join(accum)


cdef list _parse_match_info(object buf):
    # See http://sqlite.org/fts3.html#matchinfo
    cdef:
        int bufsize = len(buf), i

    bufsize = len(buf)  # Length in bytes.
    return [struct.unpack('@I', buf[i:i+4])[0] for i in range(0, bufsize, 4)]


cdef float rank(object raw_match_info):
    # Ranking implementation, which parse matchinfo.
    cdef:
        float score = 0.0
        int p, c, phrase_num, col_num, col_idx, x1, x2
        list match_info
    # Handle match_info called w/default args 'pcx' - based on the example rank
    # function http://sqlite.org/fts3.html#appendix_a
    match_info = _parse_match_info(raw_match_info)
    p, c = match_info[:2]
    for phrase_num in range(p):
        phrase_info_idx = 2 + (phrase_num * c * 3)
        for col_num in range(c):
            col_idx = phrase_info_idx + (col_num * 3)
            x1, x2 = match_info[col_idx:col_idx + 2]
            if x1 > 0:
                score += float(x1) / x2
    return score


cdef float bm25(object raw_match_info, int column_index, float k1, float b):
    """
    Okapi BM25 ranking implementation (FTS4 only).

    * Format string *must* be pcxnal
    * Second parameter to bm25 specifies the index of the column, on
      the table being queries.
    """
    cdef:
        float avg_length, doc_length, D, term_freq, term_matches, idf
        float denom, rhs, score
        int p, c, n_idx, a_idx, l_idx, n
        int total_docs, x_idx
        list match_info, a, l

    k1 = k1 or 1.2
    b = b or 0.75
    match_info = _parse_match_info(raw_match_info)
    score = 0.0
    # p, 1 --> num terms
    # c, 1 --> num cols
    # x, (3 * p * c) --> for each phrase/column,
    #     term_freq for this column
    #     term_freq for all columns
    #     total documents containing this term
    # n, 1 --> total rows in table
    # a, c --> for each column, avg number of tokens in this column
    # l, c --> for each column, length of value for this column (in this row)
    # s, c --> ignore
    p, c = match_info[:2]
    n_idx = 2 + (3 * p * c)
    a_idx = n_idx + 1
    l_idx = a_idx + c
    n = match_info[n_idx]
    a = match_info[a_idx: a_idx + c]
    l = match_info[l_idx: l_idx + c]

    total_docs = n
    avg_length = float(a[column_index])
    doc_length = float(l[column_index])
    if avg_length == 0:
        D = 0
    else:
        D = 1 - b + (b * (doc_length / avg_length))

    for phrase in range(p):
        # p, c, p0c01, p0c02, p0c03, p0c11, p0c12, p0c13, p1c01, p1c02, p1c03..
        # So if we're interested in column <i>, the counts will be at indexes
        x_idx = 2 + (3 * column_index * (phrase + 1))
        term_freq = float(match_info[x_idx])
        term_matches = float(match_info[x_idx + 2])

        # The `max` check here is based on a suggestion in the Wikipedia
        # article. For terms that are common to a majority of documents, the
        # idf function can return negative values. Applying the max() here
        # weeds out those values.
        idf = max(
            log(
                (total_docs - term_matches + 0.5) /
                (term_matches + 0.5)),
            0)

        denom = term_freq + (k1 * D)
        if denom == 0:
            rhs = 0
        else:
            rhs = (term_freq * (k1 + 1)) / denom

        score += (idf * rhs)

    return score


cdef unsigned int murmurhash2(const char *key, int nlen, unsigned int seed):
    cdef:
        unsigned int m = 0x5bd1e995
        int r = 24
        unsigned int l = nlen
        unsigned char *data = <unsigned char *>key
        unsigned int h = seed
        unsigned int k
        unsigned int t = 0

    while nlen >= 4:
        k = <unsigned int>(<unsigned int *>data)[0]

        # mmix(h, k).
        k *= m
        k = k ^ (k >> r)
        k *= m
        h *= m
        h = h ^ k

        data += 4
        nlen -= 4

    if nlen == 3:
        t = t ^ (data[2] << 16)
    if nlen >= 2:
        t = t ^ (data[1] << 8)
    if nlen >= 1:
        t = t ^ (data[0])

    # mmix(h, t).
    t *= m
    t = t ^ (t >> r)
    t *= m
    h *= m
    h = h ^ t

    # mmix(h, l).
    l *= m
    l = l ^ (l >> r)
    l *= m
    h *= m
    h = h ^ l

    h = h ^ (h >> 13)
    h *= m
    h = h ^ (h >> 15)

    return h


def murmurhash(key, seed=None):
    if key is None:
        return

    cdef:
        bytes bkey
        int nseed = seed or 0

    if isinstance(key, unicode):
        bkey = <bytes>key.encode('utf-8')
    else:
        bkey = <bytes>key

    if key:
        return murmurhash2(<char *>bkey, len(bkey), nseed)
    return 0


cdef bint regexp(basestring value, basestring regex):
    # Expose regular expression matching in SQLite.
    return re.search(regex, value, re.I) is not None


def make_hash(hash_impl):
    def inner(*items):
        state = hash_impl()
        for item in items:
            state.update(item)
        return state.hexdigest()
    return inner


hash_md5 = make_hash(hashlib.md5)
hash_sha1 = make_hash(hashlib.sha1)
hash_sha256 = make_hash(hashlib.sha256)


cdef int _aggressive_busy_handler(void *ptr, int n):
    # In concurrent environments, it often seems that if multiple queries are
    # kicked off at around the same time, they proceed in lock-step to check
    # for the availability of the lock. By introducing some "jitter" we can
    # ensure that this doesn't happen. Furthermore, this function makes more
    # attempts in the same time period than the default handler.
    cdef:
        int busyTimeout = <int>ptr
        int current, total

    if n < 20:
        current = 25 - (rand() % 10)  # ~20ms
        total = n * 20
    elif n < 40:
        current = 50 - (rand() % 20)  # ~40ms
        total = 400 + ((n - 20) * 40)
    else:
        current = 120 - (rand() % 40)  # ~100ms
        total = 1200 + ((n - 40) * 100)  # Estimate the amount of time slept.

    if total + current > busyTimeout:
        current = busyTimeout - total
    if current > 0:
        sqlite3_sleep(current)
        return 1
    return 0


cdef class CursorWrapper(object)  # Forward declaration.


cdef class ResultIterator(object):
    cdef:
        bint is_populated
        int index
        CursorWrapper cursor_wrapper

    def __init__(self, CursorWrapper cursor_wrapper):
        self.cursor_wrapper = cursor_wrapper
        self.index = 0

    def __next__(self):
        cdef CursorWrapper cw = self.cursor_wrapper
        if self.index < cw.count:
            obj = cw.result_cache[self.index]
        elif not cw.is_populated:
            obj = cw.iterate()
            cw.result_cache.append(obj)
            cw.count += 1
        else:
            raise StopIteration
        self.index += 1
        return obj


cdef class CursorWrapper(object):
    cdef:
        bint is_initialized, is_populated
        int count, index
        list columns, result_cache
        object cursor, transform

    def __init__(self, cursor, transform=None):
        self.cursor = cursor
        self.transform = transform
        self.is_initialized = False
        self.is_populated = False
        self.result_cache = []

    def __iter__(self):
        if self.is_populated:
            return iter(self.result_cache)
        return ResultIterator(self)

    def __len__(self):
        self.fill_cache()
        return self.count

    cdef initialize(self):
        self.columns = [col[0] for col in self.cursor.description]
        self.count = self.index = 0
        self.is_initialized = True

    cdef iterate(self):
        cdef:
            tuple row = self.cursor.fetchone()

        if not row:
            self.is_populated = True
            raise StopIteration
        elif not self.is_initialized:
            self.initialize()

        if self.transform is not None:
            return self.transform(self, row)
        else:
            return row

    def iterator(self):
        while True:
            yield self.iterate()

    def __next__(self):
        cdef object inst

        if self.index < self.count:
            inst = self.result_cache[self.index]
            self.index += 1
            return inst
        elif self.is_populated:
            raise StopIteration

        inst = self.iterate()
        self.result_cache.append(inst)
        self.count += 1
        self.index += 1
        return inst

    cpdef fill_cache(self, n=None):
        cdef:
            int counter = -1 if n is None else <int>n
        if counter > 0:
            counter = counter - self.count

        self.index = self.count
        while not self.is_populated and counter:
            try:
                next(self)
            except StopIteration:
                break
            else:
                counter -= 1

    def __getitem__(self, value):
        if isinstance(value, slice):
            index = value.stop
        else:
            index = value
        if index is not None:
            index = index + 1 if index >= 0 else None
        self.fill_cache(index)
        return self.result_cache[value]


cdef dict_transform(CursorWrapper cursor_wrapper, tuple row):
    cdef:
        basestring column
        dict accum = {}
        int i

    for i, column in enumerate(cursor_wrapper.columns):
        accum[column] = row[i]

    return accum

def DictCursorWrapper(cursor):
    return CursorWrapper(cursor, dict_transform)

cdef class NamedTupleCursorWrapper(CursorWrapper)  # Forward declaration.

cdef namedtuple_transform(NamedTupleCursorWrapper cursor_wrapper, tuple row):
    return cursor_wrapper.tuple_class(*row)

cdef class NamedTupleCursorWrapper(CursorWrapper):
    cdef:
        object tuple_class

    cdef initialize(self):
        CursorWrapper.initialize(self)
        self.tuple_class = namedtuple('Row', self.columns)
        self.transform = namedtuple_transform


cdef class _callable_context_manager(object):
    def __call__(self, fn):
        def inner(*args, **kwargs):
            with self:
                return fn(*args, **kwargs)
        return inner

__sentinel__ = object()


def __status__(flag, return_highwater=False):
    """
    Expose a sqlite3_status() call for a particular flag as a property of the
    Database object.
    """
    def getter(self):
        cdef int current, highwater
        cdef int rc = sqlite3_status(flag, &current, &highwater, 0)
        if rc == SQLITE_OK:
            if return_highwater:
                return highwater
            else:
                return (current, highwater)
        else:
            raise Exception('Error requesting status: %s' % rc)
    return property(getter)


def __dbstatus__(flag, return_highwater=False, return_current=False):
    """
    Expose a sqlite3_dbstatus() call for a particular flag as a property of the
    Database instance. Unlike sqlite3_status(), the dbstatus properties pertain
    to the current connection.
    """
    def getter(Database self):
        cdef:
            int current, hi
            pysqlite_Connection *conn = <pysqlite_Connection *>(self._local.conn)
            int rc = sqlite3_db_status(conn.db, flag, &current, &hi, 0)

        if rc != SQLITE_OK:
            raise Exception('Error requesting db status: %s' % rc)

        if return_highwater:
            return hi
        elif return_current:
            return current
        else:
            return (current, hi)
    return property(getter)


def __pragma__(name):
    """
    Expose a SQLite PRAGMA operation as a property of the Database, with a
    getter and setter.
    """
    def __get__(self):
        return self.pragma(name)
    def __set__(self, value):
        return self.pragma(name, value)
    return property(__get__, __set__)


cdef inline int _check_connection(pysqlite_Connection *conn) except -1:
    """
    Check that the underlying SQLite database connection is usable. Raises an
    InterfaceError if the connection is either uninitialized or closed.
    """
    if not conn.initialized:
        raise InterfaceError('Connection not initialized.')
    if not conn.db:
        raise InterfaceError('Cannot operate on closed database.')
    return 1


cdef _sqlite_to_python(sqlite3_context *context, int n, sqlite3_value **args):
    # Convert a value stored in a sqlite3_context object into an appropriate
    # Python object. For instance, transform a parameter to a user-defined
    # function call into a Python object.
    cdef:
        int i
        int value_type
        list accum = []
        sqlite3_value *value

    for i in range(n):
        value = args[i]
        value_type = sqlite3_value_type(value)

        if value_type == SQLITE_INTEGER:
            obj = sqlite3_value_int(value)
        elif value_type == SQLITE_FLOAT:
            obj = sqlite3_value_double(value)
        elif value_type == SQLITE_TEXT:
            obj = str(sqlite3_value_text(value))
        elif value_type == SQLITE_BLOB:
            obj = <bytes>sqlite3_value_blob(value)
        else:
            obj = None
        accum.append(obj)

    return accum


cdef _python_to_sqlite(sqlite3_context *context, value):
    # Store a Python value in a sqlite3_context object. For instance, store the
    # result to a user-defined function call so it is accessible in SQLite.
    if value is None:
        sqlite3_result_null(context)
    elif isinstance(value, (int, long)):
        sqlite3_result_int64(context, <sqlite3_int64>value)
    elif isinstance(value, float):
        sqlite3_result_double(context, <double>value)
    elif isinstance(value, basestring):
        sqlite3_result_text(context, <const char *>value, -1,
                            <sqlite3_destructor_type>-1)
    elif isinstance(value, bool):
        sqlite3_result_int(context, int(value))
    else:
        sqlite3_result_error(context, 'Unsupported type %s' % type(value), -1)
        return SQLITE_ERROR

    return SQLITE_OK


cdef void _function_callback(sqlite3_context *context, int nparams,
                             sqlite3_value **values) with gil:
    # C-callback used by user-defined functions implemented in Python. Note
    # that we have to acquire the GIL because Python code is being executed.
    # The Python function pointer itself is stored as the user-data parameter
    # of the function when it's created.
    cdef:
        list params = _sqlite_to_python(context, nparams, values)

    fn = <object>sqlite3_user_data(context)
    _python_to_sqlite(context, fn(*params))


cdef int _commit_callback(void *userData) with gil:
    # C-callback that delegates to the Python commit handler. If the Python
    # function raises a ValueError, then the commit is aborted and the
    # transaction rolled back. Otherwise, regardless of the function return
    # value, the transaction will commit.
    cdef object fn = <object>userData
    try:
        fn()
    except ValueError:
        return 1
    else:
        return SQLITE_OK


cdef void _rollback_callback(void *userData) with gil:
    # C-callback that delegates to the Python rollback handler.
    cdef object fn = <object>userData
    fn()


cdef void _update_callback(void *userData, int queryType, char *database,
                            char *table, sqlite3_int64 rowid) with gil:
    # C-callback that delegates to a Python function that is executed whenever
    # the database is updated (insert/update/delete queries). The Python
    # callback receives a string indicating the query type, the name of the
    # database, the name of the table being updated, and the rowid of the row
    # being updatd.
    cdef object fn = <object>userData
    if queryType == SQLITE_INSERT:
        query = 'INSERT'
    elif queryType == SQLITE_UPDATE:
        query = 'UPDATE'
    elif queryType == SQLITE_DELETE:
        query = 'DELETE'
    else:
        query = ''
    fn(query, str(database), str(table), <int>rowid)


class ConnectionLocal(threading.local):
    """
    Thread-local storage for connection state and transaction management.
    """
    def __init__(self, **kwargs):
        super(ConnectionLocal, self).__init__(**kwargs)
        self.closed = True
        self.conn = None
        self.transactions = []


cdef class Database(object):
    """
    The :py:class:`Database` provides an additional layer of abstraction over
    the underlying database connection. The `Database` allows configuration
    state to be maintained across connections (or threads) by:

    * Automatically registering functions, collations, aggregates, modules,
      and table-functions across re-connects/threads.
    * Managing commit-, rollback- and update-hook registration.
    * Setting PRAGMAs during connection initialization.

    The `Database` is also used to execute queries using the current thread's
    connection, and to track the state of nested atomic blocks.
    """
    cdef:
        dict _aggregates, _collations, _functions, _modules
        dict _function_map
        list _table_functions
        object _lock
        object _commit_hook, _rollback_hook, _update_hook
        public dict connect_kwargs
        public list _pragmas
        public object database
        readonly bint deferred
        readonly object _local

    def __init__(self, database, pragmas=None, journal_mode=None,
                 rank_functions=False, regex_function=True,
                 hash_functions=True, **kwargs):
        self.database = database
        self.connect_kwargs = {}
        self._local = ConnectionLocal()
        self._lock = threading.Lock()

        # Registers for user-defined extensions.
        self._aggregates = {}
        self._collations = {}
        self._functions = {}
        self._table_functions = []

        # Internal function pinboard used to keep references to user-defined
        # functions (as opposed to _functions, which holds function *and*
        # metadata).
        self._function_map = {}

        kwargs.setdefault('detect_types', pysqlite.PARSE_DECLTYPES)
        self.init(database, **kwargs)

        self._pragmas = pragmas or []
        if journal_mode is not None:
            self._pragmas.append(('journal_mode', journal_mode))

        # Register built-in custom functions.
        if rank_functions:
            self.func('rank')(rank)
            self.func('bm25')(bm25)
        if regex_function:
            self.func('regexp')(regexp)
        if hash_functions:
            self.func('murmurhash')(murmurhash)
            self.func('md5')(hash_md5)
            self.func('sha1')(hash_sha1)
            self.func('sha256')(hash_sha256)

    def init(self, database, **connect_kwargs):
        """Initialize the database with a new name and connection params."""
        if not self._local.closed:
            self.close()

        self.deferred = database is None
        self.database = database
        self.connect_kwargs.update(connect_kwargs)

    def __dealloc__(self):
        cdef:
            pysqlite_Connection *conn

        # When deallocating a Database object, we need to ensure that we clear
        # any commit, rollback or update hooks that may have been applied.
        if self._local and getattr(self._local, 'conn', None) is not None:
            conn = <pysqlite_Connection *>(self._local.conn)
            if conn.db:
                if self._commit_hook is not None:
                    sqlite3_commit_hook(conn.db, NULL, NULL)
                if self._rollback_hook is not None:
                    sqlite3_rollback_hook(conn.db, NULL, NULL)
                if self._update_hook is not None:
                    sqlite3_update_hook(conn.db, NULL, NULL)

    cpdef connect(self):
        """
        Check that connection is closed, then create a new connection to the
        given pysqlite database.

        Raises an InterfaceError if the database hasn't been initialized or if
        the connection was already opened.
        """
        with self._lock:
            if self.deferred:
                raise InterfaceError('Database has not been initialized.')
            if not self._local.closed:
                raise InterfaceError('Connection already open in this thread.')
            conn = self._local.conn = pysqlite.connect(self.database,
                                                       **self.connect_kwargs)
            self._local.closed = False
            self.initialize_connection(conn)

        return True

    cpdef bint close(self):
        """
        Close the currently-open connection for the calling thread.

        Raises an InterfaceError if the database hasn't been initialized. If
        the database is already closed, this is a no-op.
        """
        with self._lock:
            if self.deferred:
                raise InterfaceError('Database has not been initialized.')
            if not self._local.closed:
                self._local.conn.close()
                self._local.closed = True
                self._local.transactions = []
                return True
            return False

    def initialize_connection(self, conn):
        """
        Initialize the connection by setting per-connection-state, registering
        user-defined extensions, and configuring any hooks.
        """
        conn.isolation_level = None  # Disable transaction state-machine.

        if self._pragmas:
            cursor = conn.cursor()
            for pragma, value in self._pragmas:
                cursor.execute('PRAGMA %s = %s;' % (pragma, value))
            cursor.close()

        if self._aggregates:
            for name, klass in self._aggregates.items():
                self._create_aggregate(conn, klass, name)

        if self._collations:
            for name, fn in self._collations.items():
                self._create_collation(conn, fn, name)

        if self._functions:
            for name, (fn, n, deterministic) in self._functions.items():
                self._create_function(conn, fn, name, n, deterministic)

        if self._table_functions:
            for table_function in self._table_functions:
                table_function.register(conn)

        if self._commit_hook is not None:
            self._set_commit_hook(conn, self._commit_hook)
        if self._rollback_hook is not None:
            self._set_rollback_hook(conn, self._rollback_hook)
        if self._update_hook is not None:
            self._set_update_hook(conn, self._update_hook)

    def _create_aggregate(self, conn, klass, name, nparams=-1):
        conn.create_aggregate(name, nparams, klass)

    def aggregate(self, name=None):
        """
        Class decorator for user-defined aggregates.

        Example::

            @db.aggregate('avg')
            class Average(object):
                def __init__(self):
                    self.vals = []

                def step(self, value):
                    self.vals.append(value)

                def finalize(self):
                    return avg(self.vals)
        """
        def decorator(klass):
            aggregate_name = name or klass.__name__
            self._aggregates[aggregate_name] = klass
            if not self.is_closed():
                self._create_aggregate(self.get_conn(), klass, aggregate_name)
            return klass
        return decorator

    cdef _create_collation(self, conn, fn, name):
        conn.create_collation(name, fn)

    def collation(self, name=None):
        """
        Register a custom collation.

        Example::

            @db.collation('numeric')
            def numeric(lhs, rhs):
                # Sort strings with numbers in them.
                l1 = [int(t) if t.isdigit() else t
                      for t in re.split('(\d+)', lhs)]
                l2 = [int(t) if t.isdigit() else t
                      for t in re.split('(\d+)', lhs)]
                return cmp(l1, l2)
        """
        def decorator(fn):
            collation_name = name or fn.__name__
            self._collations[collation_name] = fn
            if not self.is_closed():
                self._create_collation(self.get_conn(), fn, collation_name)
        return decorator

    cdef _create_function(self, conn, fn, name, nparams, deterministic):
        """
        Register a user-defined function using our own implementation. This has
        the advantage of allowing users to specify whether a function is non-
        deterministic (which has ramifications for indexes/query planning).
        """
        cdef:
            pysqlite_Connection *c_conn = <pysqlite_Connection *>conn
            int flags = SQLITE_UTF8
            int rc

        _check_connection(c_conn)
        if deterministic:
            flags |= SQLITE_DETERMINISTIC

        rc = sqlite3_create_function(c_conn.db, <const char *>name, nparams,
                                     flags, <void *>fn, _function_callback,
                                     NULL, NULL)

        if rc != SQLITE_OK:
            raise ValueError('Error calling sqlite3_create_function.')
        else:
            self._function_map[fn] = name

    def func(self, name=None, n=-1, deterministic=True):
        """
        Create a user-defined function.

        Example::

            @db.func('md5')
            def md5(s):
                return hashlib.md5(s).hexdigest()
        """
        def decorator(fn):
            fn_name = name or fn.__name__
            self._functions[fn_name] = (fn, n, deterministic)
            if not self.is_closed():
                self._create_function(self.get_conn(), fn, fn_name, n,
                                      deterministic)
            return fn
        return decorator

    def table_function(self, name=None):
        """
        Register a table-function with the database.

        @db.table_function('series')
        class Series(TableFunction):
            columns = ['value']
            params = ['start', 'stop']

            def initialize(self, start=0, stop=None):
                self.start, self.stop = start, (stop or float('Inf'))
                self.curr = self.start

            def iterate(self, idx):
                if self.curr > self.stop:
                    raise StopIteration
                ret = self.curr
                self.curr += 1
                return (ret,)
        """
        def decorator(klass):
            if name is not None:
                klass.name = name
            self._table_functions.append(klass)
            if not self.is_closed():
                klass.register(self.get_conn())
            return klass
        return decorator

    def on_commit(self, fn):
        """
        Register a post-commit hook. The handler's return value is ignored,
        but if a ValueError is raised, then the transaction will be rolled
        back.

        Example::

            @db.on_commit
            def commit_handler():
                if datetime.date.today().weekday() == 6:
                    raise ValueError('no commits on sunday!')
        """
        self._commit_hook = fn
        if not self.is_closed():
            self._set_commit_hook(self.get_conn(), fn)
        return fn

    def _set_commit_hook(self, connection, fn):
        cdef pysqlite_Connection *conn = <pysqlite_Connection *>connection
        if fn is None:
            sqlite3_commit_hook(conn.db, NULL, NULL)
        else:
            sqlite3_commit_hook(conn.db, _commit_callback, <void *>fn)

    def on_rollback(self, fn):
        """
        Register a rollback handler. Return value is ignored.
        """
        self._rollback_hook = fn
        if not self.is_closed():
            self._set_rollback_hook(self.get_conn(), fn)
        return fn

    def _set_rollback_hook(self, connection, fn):
        cdef pysqlite_Connection *conn = <pysqlite_Connection *>connection
        if fn is None:
            sqlite3_rollback_hook(conn.db, NULL, NULL)
        else:
            sqlite3_rollback_hook(conn.db, _rollback_callback, <void *>fn)

    def on_update(self, fn):
        """
        Register an update hook. Hook is executed for each row that is
        inserted, updated or deleted. Return value is ignored.

        User-defined callback must accept the following parameters:

        * query type (INSERT, UPDATE or DELETE)
        * database name (typically 'main')
        * table name
        * rowid of affected row

        Example::

            @db.on_update
            def change_logger(query_type, db, table, rowid):
                logger.debug('%s query on %s.%s row %s', query_type, db,
                             table, rowid)
        """
        self._update_hook = fn
        if not self.is_closed():
            self._set_update_hook(self.get_conn(), fn)
        return fn

    def _set_update_hook(self, connection, fn):
        cdef pysqlite_Connection *conn = <pysqlite_Connection *>connection
        if fn is None:
            sqlite3_update_hook(conn.db, NULL, NULL)
        else:
            sqlite3_update_hook(conn.db, _update_callback, <void *>fn)

    cpdef bint is_closed(self):
        """Return a boolean indicating whether the DB is closed."""
        return self._local.closed

    cdef get_conn(self):
        # Internal method for quickly getting (or opening) a connection.
        if self._local.closed:
            self.connect()
        return self._local.conn

    cpdef execute_sql(self, sql, params=None, commit=True):
        """Execute the given SQL query and return the cursor."""
        if self._local.closed:
            self.connect()
        cursor = self._local.conn.cursor()
        cursor.execute(sql, params or ())
        if commit and len(self._local.transactions) == 0:
            self.commit()
        return cursor

    cpdef execute(self, query):
        """Execute a the SQL query represented by a Query instance."""
        sql, params = Context().parse(query)
        return self.execute_sql(sql, params, query.commit)

    cpdef sql(self, query):
        """Compile a Query object into it's SQL and parameters."""
        return Context().parse(query)

    def pragma(self, key, value=__sentinel__):
        """
        Issue a PRAGMA query on the current connection. To query the status of
        a specific PRAGMA, typically only the key is specified. See also the
        list of PRAGMAs which are exposed as properties on the Database.
        """
        sql = 'PRAGMA %s' % key
        if value is not __sentinel__:
            sql += ' = %s' % value
        row = self.execute_sql(sql).fetchone()
        if row:
            return row[0]

    def begin(self, lock_type=None):
        """
        Start a transaction using the specified lock type. If the lock type is
        unspecified, then a bare BEGIN statement is used.
        """
        statement = 'BEGIN %s' % lock_type if lock_type else 'BEGIN'
        self.execute_sql(statement, commit=False)

    cpdef commit(self):
        """Call commit() on the pysqlite connection object."""
        self._local.conn.commit()

    cpdef rollback(self):
        """Call rollback() on the pysqlite connection object."""
        self._local.conn.rollback()

    def __getitem__(self, name):
        """
        Factory method for creating Table instances.

        Example::

            UserTbl = db['users']
            query = UserTbl.select(...)
        """
        return Table(name)

    def __enter__(self):
        self.connect()
        self.begin()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Clean up the context manager, committing or rolling back and closing
        the connection.
        """
        if exc_val is not None:
            try:
                self.rollback()
            except:
                pass
        else:
            self.commit()
        if not self._local.closed:
            self.close()

    cdef _push_transaction(self, txn):
        self._local.transactions.append(txn)

    cdef _pop_transaction(self):
        self._local.transactions.pop()

    cdef _transaction_depth(self):
        return len(self._local.transactions)

    cdef last_insert_rowid(self):
        cdef:
            pysqlite_Connection *c = <pysqlite_Connection *>(self._local.conn)
        _check_connection(c)
        return <int>sqlite3_last_insert_rowid(c.db)

    @property
    def last_insert_id(self):
        """Return the rowid of the most-recently-inserted row."""
        return self.last_insert_rowid()

    @property
    def autocommit(self):
        """Return a boolean value indicating the status of autocommit."""
        cdef pysqlite_Connection *c = <pysqlite_Connection *>(self._local.conn)
        _check_connection(c)
        return bool(sqlite3_get_autocommit(c.db))

    cdef changes(self):
        cdef pysqlite_Connection *c = <pysqlite_Connection *>(self._local.conn)
        _check_connection(c)
        return sqlite3_changes(c.db)

    @property
    def rowcount(self):
        """
        Return the number of rows changed by the most-recently executed query.
        """
        return self.changes()

    def set_busy_handler(self, timeout=5000):
        """
        Replace the default busy handler with one that introduces some "jitter"
        into the amount of time delayed between checks.
        """
        cdef:
            int n = timeout
            pysqlite_Connection *conn = <pysqlite_Connection *>(self._local.conn)
            sqlite3 *db = conn.db

        sqlite3_busy_handler(db, _aggressive_busy_handler, <void *>n)
        return True

    cpdef _atomic atomic(self):
        """
        Execute statements in either a transaction or a savepoint. The
        outer-most call to *atomic* will use a transaction, and any subsequent
        nested calls will use savepoints.

        ``atomic`` can be used as either a context manager or a decorator.

        .. note::
            For most use-cases, it makes the most sense to always use
            ``atomic`` when you wish to execute queries in a transaction.
            The benefit of using ``atomic`` is that you do not need to
            manually keep track of the transaction stack depth, as this will
            be managed for you.
        """
        return _atomic(self)

    cpdef _transaction transaction(self):
        """
        Execute statements in a transaction using either a context manager or
        decorator. If an error is raised inside the wrapped block, the
        transaction will be rolled back, otherwise statements are committed
        when exiting. Transactions can also be explicitly rolled back or
        committed within the transaction block by calling
        :py:meth:`~transaction.rollback` or :py:meth:`~transaction.commit`.
        If you manually commit or roll back, a new transaction will be started
        automatically.

        Nested blocks can be wrapped with ``transaction`` - the database
        will keep a stack and only commit when it reaches the end of the outermost
        function / block.
        """
        return _transaction(self)

    cpdef _savepoint savepoint(self):
        """
        Execute statements in a savepoint using either a context manager or
        decorator. If an error is raised inside the wrapped block, the
        savepoint will be rolled back, otherwise statements are committed when
        exiting. Like :py:meth:`~Database.transaction`, a savepoint can also
        be explicitly rolled-back or committed by calling
        :py:meth:`~savepoint.rollback` or :py:meth:`~savepoint.commit`. If you
        manually commit or roll back, a new savepoint **will not** be created.

        Savepoints can be thought of as nested transactions.

        :param str sid: An optional string identifier for the savepoint.
        """
        return _savepoint(self)

    cpdef list get_tables(self):
        """
        Returns a sorted list of tables in the database.
        """
        cursor = self.execute_sql('SELECT name FROM sqlite_master WHERE '
                                  'type = ? ORDER BY name;', ('table',))
        return [row[0] for row in cursor.fetchall()]

    cpdef list get_indexes(self, table):
        """
        Returns a list of index metadata for the given table. Index metadata
        is returned as a 4-tuple consisting of:

        * Index name.
        * SQL used to create the index.
        * Names of columns being indexed.
        * Whether the index is unique.
        """
        cdef:
            bint is_unique
            dict index_columns = {}, index_to_sql
            set unique_indexes = set()

        query = ('SELECT name, sql FROM sqlite_master '
                 'WHERE tbl_name = ? AND type = ? ORDER BY name')
        cursor = self.execute_sql(query, (table, 'index'))
        index_to_sql = dict(cursor.fetchall())

        # Determine which indexes have a unique constraint.
        cursor = self.execute_sql('PRAGMA index_list("%s")' % table)
        for row in cursor.fetchall():
            name = row[1]
            is_unique = int(row[2]) == 1
            if is_unique:
                unique_indexes.add(name)

        # Retrieve the indexed columns.
        for index_name in sorted(index_to_sql):
            cursor = self.execute_sql('PRAGMA index_info("%s")' % index_name)
            index_columns[index_name] = [row[2] for row in cursor.fetchall()]

        return [(
            name,
            index_to_sql[name],
            index_columns[name],
            name in unique_indexes)
            for name in sorted(index_to_sql)]

    cpdef list get_columns(self, table):
        """
        Returns a list of column metadata for the given table. Column
        metadata is returned as a 4-tuple consisting of:

        * Column name.
        * Data-type column was declared with.
        * Whether the column can be NULL.
        * Whether the column is the primary key.
        """
        cursor = self.execute_sql('PRAGMA table_info("%s")' % table)
        return [(row[1], row[2], not row[3], bool(row[5]))
                for row in cursor.fetchall()]

    cpdef list get_primary_keys(self, table):
        """
        Returns a list of column(s) that comprise the table's foreign key.
        """
        cursor = self.execute_sql('PRAGMA table_info("%s")' % table)
        return [row[1] for row in cursor.fetchall() if row[-1]]

    cpdef list get_foreign_keys(self, table):
        """
        Returns a list of foreign key metadata for the given table. Foreign
        key metadata is returned as a 3-tuple consisting of:

        * Source column name, i.e. the column on the given table.
        * Destination table.
        * Destination column.
        """
        cursor = self.execute_sql('PRAGMA foreign_key_list("%s")' % table)
        return [(row[3], row[2], row[4]) for row in cursor.fetchall()]

    # Pragma queries.
    cache_size = __pragma__('cache_size')
    foreign_keys = __pragma__('foreign_keys')
    journal_mode = __pragma__('journal_mode')
    journal_size_limit = __pragma__('journal_size_limit')
    mmap_size = __pragma__('mmap_size')
    page_size = __pragma__('page_size')
    read_uncommitted = __pragma__('read_uncommitted')
    synchronous = __pragma__('synchronous')
    wal_autocheckpoint = __pragma__('wal_autocheckpoint')

    # Status properties.
    memory_used = __status__(SQLITE_STATUS_MEMORY_USED)
    malloc_size = __status__(SQLITE_STATUS_MALLOC_SIZE, True)
    malloc_count = __status__(SQLITE_STATUS_MALLOC_COUNT)
    pagecache_used = __status__(SQLITE_STATUS_PAGECACHE_USED)
    pagecache_overflow = __status__(SQLITE_STATUS_PAGECACHE_OVERFLOW)
    pagecache_size = __status__(SQLITE_STATUS_PAGECACHE_SIZE, True)
    scratch_used = __status__(SQLITE_STATUS_SCRATCH_USED)
    scratch_overflow = __status__(SQLITE_STATUS_SCRATCH_OVERFLOW)
    scratch_size = __status__(SQLITE_STATUS_SCRATCH_SIZE, True)

    # Connection status properties.
    lookaside_used = __dbstatus__(SQLITE_DBSTATUS_LOOKASIDE_USED)
    lookaside_hit = __dbstatus__(SQLITE_DBSTATUS_LOOKASIDE_HIT, True)
    lookaside_miss = __dbstatus__(SQLITE_DBSTATUS_LOOKASIDE_MISS_SIZE, True)
    lookaside_miss_full = __dbstatus__(SQLITE_DBSTATUS_LOOKASIDE_MISS_FULL,
                                       True)
    cache_used = __dbstatus__(SQLITE_DBSTATUS_CACHE_USED, False, True)
    cache_used_shared = __dbstatus__(SQLITE_DBSTATUS_CACHE_USED_SHARED,
                                     False, True)
    schema_used = __dbstatus__(SQLITE_DBSTATUS_SCHEMA_USED, False, True)
    statement_used = __dbstatus__(SQLITE_DBSTATUS_STMT_USED, False, True)
    cache_hit = __dbstatus__(SQLITE_DBSTATUS_CACHE_HIT, False, True)
    cache_miss = __dbstatus__(SQLITE_DBSTATUS_CACHE_MISS, False, True)
    cache_write = __dbstatus__(SQLITE_DBSTATUS_CACHE_WRITE, False, True)


cdef class _atomic(_callable_context_manager):
    cdef:
        Database db
        object _helper

    def __init__(self, Database db):
        self.db = db

    def __enter__(self):
        if self.db._transaction_depth() == 0:
            self._helper = self.db.transaction()
        else:
            self._helper = self.db.savepoint()
        return self._helper.__enter__()

    def __exit__(self, exc_type, exc_val, exc_tb):
        return self._helper.__exit__(exc_type, exc_val, exc_tb)


cdef class _transaction(_callable_context_manager):
    cdef:
        basestring lock
        bint _orig
        Database db

    def __init__(self, Database db, lock='DEFERRED'):
        self.db = db
        self.lock = lock

    cpdef _begin(self):
        self.db.begin(self.lock)

    cpdef commit(self, bint begin=True):
        self.db.commit()
        if begin:
            self._begin()

    cpdef rollback(self, bint begin=True):
        self.db.rollback()
        if begin:
            self._begin()

    def __enter__(self):
        if self.db._transaction_depth() == 0:
            self._begin()
        self.db._push_transaction(self)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            if exc_type:
                self.rollback(False)
            elif self.db._transaction_depth() == 1:
                try:
                    self.commit(False)
                except:
                    self.rollback(False)
                    raise
        finally:
            self.db._pop_transaction()


cdef class _savepoint(_callable_context_manager):
    cdef:
        basestring sid, quoted_sid
        Database db

    def __init__(self, Database db, sid=None):
        self.db = db
        self.sid = sid or 's' + uuid.uuid4().hex
        self.quoted_sid = "%s" % self.sid

    cpdef _execute(self, basestring query):
        self.db.execute_sql(query)

    cpdef commit(self):
        self._execute('RELEASE SAVEPOINT %s;' % self.quoted_sid)

    cpdef rollback(self):
        self._execute('ROLLBACK TO SAVEPOINT %s;' % self.quoted_sid)

    def __enter__(self):
        self._execute('SAVEPOINT %s;' % self.quoted_sid)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            self.rollback()
        else:
            try:
                self.commit()
            except:
                self.rollback()
                raise


# Query Builder.
SCOPE_NORMAL = 0
SCOPE_SOURCE = 1
SCOPE_VALUES = 2
SCOPE_CTE = 3


cdef class State(object):
    """
    Lightweight object for representing the rules applied at a given scope.
    """
    cdef:
        bint parentheses, subquery
        int scope

    def __cinit__(self, scope=SCOPE_NORMAL, parentheses=False,
                  subquery=False):
        self.scope = scope
        self.parentheses = parentheses
        self.subquery = subquery

    def __call__(self, scope=None, parentheses=None, subquery=None):
        cdef:
            int i_scope = self.scope if scope is None else scope
            bint i_subquery = self.subquery if subquery is None else subquery
        return State.__new__(State, i_scope, parentheses, i_subquery)


def __scope_context__(int scope):
    @contextmanager
    def inner(self, **kwargs):
        with self(scope=scope, **kwargs):
            yield self
    return inner


cdef class Context(object):
    """
    The Context converts SQL fragments into queries and bind-values. Maintains
    state during the SQL generation to ensure different entities render
    correctly.
    """
    cdef:
        public parentheses, subquery
        public int scope
        public list stack, _sql, _values
        public State state

    def __init__(self):
        self.stack = []
        self._sql = []
        self._values = []
        self.state = State()
        self.refresh()

    cdef refresh(self):
        self.scope = self.state.scope
        self.parentheses = self.state.parentheses
        self.subquery = self.state.subquery

    def __call__(self, **overrides):
        if overrides and overrides.get('scope') == self.scope:
            del overrides['scope']

        self.stack.append(self.state)
        self.state = self.state(**overrides)
        self.refresh()
        return self

    scope_normal = __scope_context__(SCOPE_NORMAL)
    scope_source = __scope_context__(SCOPE_SOURCE)
    scope_values = __scope_context__(SCOPE_VALUES)
    scope_cte = __scope_context__(SCOPE_CTE)

    def __enter__(self):
        if self.parentheses:
            self.literal('(')
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.parentheses:
            self.literal(')')
        self.state = self.stack.pop()
        self.refresh()

    cpdef Context sql(self, obj):
        if isinstance(obj, (Node, Context)):
            obj.__sql__(self)
            return self
        else:
            return self.sql(Value(obj))

    cpdef Context literal(self, sql):
        self._sql.append(sql)
        return self

    cdef Context bind_value(self, value):
        self._values.append(value)
        return self

    cdef Context __sql__(self, Context ctx):
        ctx._sql.extend(self._sql)
        ctx._values.extend(self._values)
        return ctx

    cpdef tuple parse(self, query):
        self.sql(query)
        return ''.join(self._sql), self._values

    cpdef tuple query(self):
        return ''.join(self._sql), self._values


cdef class ColumnFactory(object):
    """
    Lightweight factory for creating Column objects dynamically by overriding
    the getattr hook.

    For example::

        User = Table('users')
        username = User.c.username
        # The above is equivalent to:
        username = Column(User, 'username')
    """
    cdef object source
    def __init__(self, source):
        self.source = source
    def __getattr__(self, attr):
        return Column(self.source, attr)


cdef class _DynamicColumn(object):
    """
    Descriptor that exposes the ColumnFactory as a "magic" attribute.
    """
    def __get__(self, instance, instance_type):
        if instance is not None:
            return ColumnFactory(instance)
        return self


cdef class _ExplicitColumn(object):
    """
    Descriptor that prevents "magic" attribute lookup for sources that define
    their own column attributes.
    """
    def __get__(self, instance, instance_type):
        if instance is not None:
            raise AttributeError('%s specifies columns explicitly and does not'
                                 ' support dynamic column lookups.')
        return self


class Node(object):
    def clone(self):
        obj = self.__class__.__new__(self.__class__)
        obj.__dict__ = self.__dict__.copy()
        return obj

    @staticmethod
    def copy(method):
        def inner(self, *args, **kwargs):
            clone = self.clone()
            method(clone, *args, **kwargs)
            return clone
        return inner


class Source(Node):
    c = _DynamicColumn()

    def __init__(self, alias=None):
        super(Source, self).__init__()
        self._alias = alias

    @Node.copy
    def alias(self, name=None):
        self._alias = name

    def select(self, *selection):
        return Select((self,), selection)

    def join(self, other, join_type='INNER', on=None):
        return Join(self, other, join_type, on)

    def left_outer_join(self, other, on=None):
        return self.join(other, 'LEFT OUTER', on)

    def apply_alias(self, Context ctx):
        if ctx.scope == SCOPE_SOURCE and self._alias:
            ctx.literal(' AS ').sql(Entity(self._alias))
        return ctx


class Table(Source):
    def __init__(self, name, columns=None, schema=None, alias=None):
        self.name = name
        self._columns = columns
        self._schema = schema
        self._path = (self._schema, name) if self._schema else (name,)
        super(Table, self).__init__(alias=alias)

        if self._columns:
            self.c = _ExplicitColumn()
            for column in self._columns:
                setattr(self, column, Column(self, column))

    def clone(self):
        return Table(self.name, self._columns, schema=self._schema,
                     alias=self._alias)

    def select(self, *selection):
        if not selection and self._columns:
            selection = [Column(self, column) for column in self._columns]
        return Select((self,), selection)

    def insert(self, data, **kwargs):
        return Insert(self, data, **kwargs)

    def update(self, data):
        return Update(self, data)

    def delete(self):
        return Delete(self)

    def __sql__(self, Context ctx):
        if ctx.scope == SCOPE_SOURCE:
            ctx.sql(Entity(*self._path))
            if self._alias:
                ctx.literal(' AS ')
                ctx.sql(Entity(self._alias))
        elif self._alias:
            ctx.sql(Entity(self._alias))
        else:
            ctx.sql(Entity(*self._path))
        return ctx


class Join(Source):
    def __init__(self, lhs, rhs, join_type='INNER', on=None):
        super(Join, self).__init__()
        self._lhs = lhs
        self._rhs = rhs
        self._join_type = join_type
        self._on = on

    def on(self, predicate):
        self._on = predicate
        return self

    def __sql__(self, Context ctx):
        (ctx
         .sql(self._lhs)
         .literal(' %s JOIN ' % self._join_type)
         .sql(self._rhs))
        if self._on is not None:
            ctx.literal(' ON ').sql(self._on)
        return ctx


class CTE(Source):
    def __init__(self, name, query, recursive=False, columns=None):
        self._alias = name
        self._nested_cte_list = query._cte_list
        query._cte_list = ()
        self._query = query
        self._recursive = recursive
        self._columns = columns
        super(CTE, self).__init__(alias=name)

    def __sql__(self, Context ctx):
        ctx.sql(Entity(self._alias))
        if ctx.scope == SCOPE_CTE:
            if self._columns:
                ctx.literal(' ').sql(EnclosedList(self._columns))
            ctx.literal(' AS (')
            with ctx.scope_normal():
                ctx.sql(self._query)
            ctx.literal(')')
        return ctx


class EntityBase(Node):
    def alias(self, name):
        return Alias(self, name)
    def __inv__(self):
        return Negated(self)
    def __eq__(self, other):
        return Expression(self, '=', other)
    def __ne__(self, other):
        return Expression(self, '!=', other)
    def __neg__(self):
        return self.desc()
    def __pos__(self):
        return self.asc()
    def __e__(op, inv=False):
        def inner(self, rhs):
            if inv:
                return Expression(rhs, op, self)
            return Expression(self, op, rhs)
        return inner
    __and__ = __e__('AND')
    __or__ = __e__('OR')
    __add__ = __e__('+')
    __sub__ = __e__('-')
    __mul__ = __e__('*')
    __div__ = __e__('/')
    __rand__ = __e__('AND', True)
    __ror__ = __e__('OR', True)
    __radd__ = __e__('+', True)
    __rsub__ = __e__('-', True)
    __rmul__ = __e__('*', True)
    __rdiv__ = __e__('/', True)
    __lt__ = __e__('<')
    __gt__ = __e__('>')
    __le__ = __e__('<=')
    __ge__ = __e__('>=')
    __lshift__ = __e__('IN')
    __rshift__ = __e__('IS')
    __mod__ = __e__('LIKE')
    __pow__ = __e__('GLOB')
    in_ = __e__('IN')
    not_in = __e__('NOT IN')
    def is_null(self, is_null=True):
        return Expression(self, ('IS' if is_null else 'IS NOT'), None)
    def between(self, lo, hi):
        return Expression(self, 'BETWEEN', Expression(lo, 'AND', hi, True))

    def asc(self):
        return Asc(self)
    def desc(self):
        return Desc(self)


class Value(EntityBase):
    def __init__(self, value):
        self.value = value
        self.multi = isinstance(self.value, (list, tuple))
        if self.multi:
            self.bind_values = []
            for item in self.value:
                if isinstance(item, Node):
                    self.bind_values.append(item)
                else:
                    self.bind_values.append(Value(item))

    def __sql__(self, Context ctx):
        if self.multi:
            return ctx.sql(EnclosedList(self.bind_values))
        else:
            return ctx.literal('?').bind_value(self.value)


class Column(EntityBase):
    def __init__(self, table, name):
        self.table = table
        self.name = name

    def __sql__(self, Context ctx):
        if ctx.scope == SCOPE_VALUES:
            return ctx.sql(Entity(self.name))
        else:
            with ctx.scope_normal():
                return ctx.sql(self.table).literal('.').sql(Entity(self.name))


class Expression(EntityBase):
    def __init__(self, lhs, op, rhs, is_flat=False):
        if not isinstance(lhs, Node):
            lhs = Value(lhs)
        if not isinstance(rhs, Node):
            rhs = Value(rhs)
        self.lhs = lhs
        self.op = op
        self.rhs = rhs
        self._is_flat = is_flat

    def __sql__(self, Context ctx):
        with ctx(parentheses=not self._is_flat):
            return (ctx
                    .sql(self.lhs)
                    .literal(' %s ' % self.op)
                    .sql(self.rhs))


class EntityWrapper(EntityBase):
    def __init__(self, entity, *args, **kwargs):
        self.entity = entity
        super(EntityWrapper, self).__init__(*args, **kwargs)


class Alias(EntityWrapper):
    def __init__(self, entity, name):
        super(Alias, self).__init__(entity)
        self.name = name

    def alias(self, name):
        if not name:
            return self.entity
        self.name = name
        return self

    def __sql__(self, Context ctx):
        return ctx.sql(self.entity).literal(' AS "%s"' % self.name)


class Negated(EntityWrapper):
    def __invert__(self):
        return self.entity

    def __sql__(self, Context ctx):
        return ctx.literal(' NOT ').sql(self.entity)


class Cast(EntityWrapper):
    def __init__(self, entity, cast):
        super(Cast, self).__init__(entity)
        self.cast = cast

    def __sql__(self, Context ctx):
        return (ctx
                .literal('CAST(')
                .sql(self.entity)
                .literal(' AS %s)' % self.cast))


class Ordering(EntityWrapper):
    def __init__(self, entity, direction, collation=None, nulls=None):
        super(Ordering, self).__init__(entity)
        self.direction = direction
        self.collation = collation
        self.nulls = nulls

    def collate(self, collation=None):
        return Ordering(self.entity, self.direction, collation, self.nulls)

    def __sql__(self, Context ctx):
        ctx.sql(self.entity).literal(' %s' % self.direction)
        if self.collation:
            ctx.literal(' COLLATE %s' % self.collation)
        if self.nulls:
            ctx.literal(' NULLS %s' % self.nulls)
        return ctx


def Asc(entity, collation=None, nulls=None):
    return Ordering(entity, 'ASC', collation, nulls)


def Desc(entity, collation=None, nulls=None):
    return Ordering(entity, 'DESC', collation, nulls)


class Entity(EntityBase):
    def __init__(self, *path):
        self._path = filter(None, path)
        self._quoted = '.'.join('"%s"' % part for part in self._path)

    def __sql__(self, Context ctx):
        return ctx.literal(self._quoted)


class SQL(EntityBase):
    def __init__(self, sql, params=None):
        self.sql = sql
        self.params = params

    def __sql__(self, Context ctx):
        ctx.literal(self.sql)
        if self.params:
            for param in self.params:
                if isinstance(param, Node):
                    ctx.sql(param)
                else:
                    ctx.bind_value(param)
        return ctx


def Check(constraint):
    return SQL('CHECK (%s)' % constraint)


class Function(EntityBase):
    def __init__(self, name, arguments):
        self.name = name
        self.arguments = arguments

    def __getattr__(self, attr):
        def decorator(*args):
            return Function(attr, args)
        return decorator

    def __sql__(self, Context ctx):
        ctx.literal(self.name)
        if not len(self.arguments):
            ctx.literal('()')
        else:
            ctx.sql(EnclosedList([
                (arg if isinstance(arg, Node) else Value(arg))
                for arg in self.arguments]))
        return ctx

fn = Function(None, None)


class List(Node):
    def __init__(self, nodes, glue=' ', parentheses=False):
        self.nodes = nodes
        self.glue = glue
        self.parentheses = parentheses
        # Hack to avoid double-parentheses.
        if parentheses and len(self.nodes) == 1:
            if isinstance(self.nodes[0], Expression):
                self.nodes[0].flat = True

    def __sql__(self, Context ctx):
        n = len(self.nodes)
        if n == 0:
            return ctx
        with ctx(parentheses=self.parentheses):
            for i in range(n - 1):
                ctx.sql(self.nodes[i])
                ctx.literal(self.glue)
            ctx.sql(self.nodes[n - 1])
        return ctx


def CommaList(nodes):
    return List(nodes, ', ')


def EnclosedList(nodes):
    return List(nodes, ', ', True)


class Query(Node):
    commit = True

    def __init__(self, order_by=None, limit=None, offset=None, **kwargs):
        super(Query, self).__init__(**kwargs)
        self._order_by, self._limit, self._offset = order_by, limit, offset
        self._cte_list = None
        self._cursor = None

    def clone(self):
        query = super(Query, self).clone()
        query._cursor = None
        return query

    @Node.copy
    def with_(self, *cte_list):
        self._cte_list = cte_list

    @Node.copy
    def order_by(self, *values):
        self._order_by = values

    @Node.copy
    def order_by_extend(self, *values):
        self._order_by = ((self._order_by or ()) + values) or None

    @Node.copy
    def limit(self, limit=None):
        self._limit = limit

    @Node.copy
    def offset(self, offset=None):
        self._offset = offset

    @Node.copy
    def paginate(self, page, n=20):
        if page > 0:
            page -= 1
        self._limit = n
        self._offset = page * n

    def apply_ordering(self, Context ctx):
        if self._order_by:
            ctx.literal(' ORDER BY ').sql(CommaList(self._order_by))
        if self._limit is not None or self._offset is not None:
            ctx.literal(' LIMIT %d' % (self._limit or -1))
        if self._offset is not None:
            ctx.literal(' OFFSET %d' % self._offset)
        return ctx

    def __sql__(self, Context ctx):
        if self._cte_list:
            recursive = any(cte._recursive for cte in self._cte_list)
            with ctx.scope_cte():
                (ctx
                 .literal('WITH RECURSIVE ' if recursive else 'WITH ')
                 .sql(CommaList(self._cte_list))
                 .literal(' '))
        return ctx

    def execute(self, database):
        raise NotImplementedError


class SelectBase(Source, Query):
    commit = False

    def __add__(self, rhs):
        return CompoundSelect(self, 'UNION ALL', rhs)
    def __or__(self, rhs):
        return CompoundSelect(self, 'UNION', rhs)
    def __and__(self, rhs):
        return CompoundSelect(self, 'INTERSECT', rhs)
    def __sub__(self, rhs):
        return CompoundSelect(self, 'EXCEPT', rhs)
    def cte(self, name, recursive=False, columns=None):
        return CTE(name, self, recursive, columns)


class CompoundSelect(SelectBase):
    def __init__(self, lhs, op, rhs):
        super(CompoundSelect, self).__init__()
        self.lhs = lhs
        self.op = op
        self.rhs = rhs

    def __sql__(self, Context ctx):
        with ctx(parentheses=ctx.scope == SCOPE_SOURCE):
            with ctx(parentheses=False):
                ctx.sql(self.lhs)
            ctx.literal(' %s ' % self.op)
            with ctx(parentheses=False):
                ctx.sql(self.rhs)
        ctx = self.apply_ordering(ctx)
        return self.apply_alias(ctx)

    def execute(self, database):
        return CursorWrapper(database.execute(self))


class Select(SelectBase):
    def __init__(self, from_clause=None, columns=None, where=None,
                 group_by=None, having=None, order_by=None, limit=None,
                 offset=None, distinct=None):
        super(Select, self).__init__()
        self._from = (list(from_clause) if isinstance(from_clause, tuple)
                      else from_clause)  or []
        self._columns = columns
        self._where = where
        self._group_by = group_by
        self._having = having
        self._order_by = order_by
        self._limit = limit
        self._offset = offset
        self._distinct = distinct
        self._cursor = None
        self._dicts = False
        self._objects = False

    @Node.copy
    def select(self, *columns):
        self._columns = columns

    @Node.copy
    def join(self, dest, join_type='INNER', on=None):
        if not self._from:
            raise ValueError('No sources to join on.')
        item = self._from.pop()
        self._from.append(Join(item, dest, join_type, on))

    @Node.copy
    def where(self, *expressions):
        if self._where is not None:
            expressions = (self._where,) + expressions
        self._where = reduce(operator.and_, expressions)

    @Node.copy
    def group_by(self, *columns):
        self._group_by = columns

    @Node.copy
    def group_by_extend(self, *columns):
        self._group_by = ((self._group_by or ()) + columns) or None

    @Node.copy
    def having(self, *expressions):
        if self._having is not None:
            expressions = (self._having,) + expressions
        self._having = reduce(operator.and_, expressions)

    @Node.copy
    def distinct(self, is_distinct=True):
        self._distinct = is_distinct

    @Node.copy
    def dicts(self, as_dict=True):
        self._dicts = as_dict

    @Node.copy
    def objects(self, as_object=True):
        self._objects = as_object

    def __sql__(self, Context ctx):
        super(Select, self).__sql__(ctx)
        is_subquery = ctx.subquery
        parentheses = is_subquery or (ctx.scope == SCOPE_SOURCE)
        with ctx.scope_normal(parentheses=parentheses, subquery=True):
            ctx.literal('SELECT DISTINCT ' if self._distinct else 'SELECT ')
            with ctx.scope_source():
                ctx.sql(CommaList(self._columns))

            if self._from:
                with ctx.scope_source(parentheses=False):
                    ctx.literal(' FROM ').sql(CommaList(self._from))

            if self._where is not None:
                ctx.literal(' WHERE ').sql(self._where)

            if self._group_by:
                ctx.literal(' GROUP BY ').sql(CommaList(self._group_by))

            if self._having is not None:
                ctx.literal(' HAVING ').sql(self._having)

            self.apply_ordering(ctx)

        return self.apply_alias(ctx)

    def execute(self, Database database):
        cursor = database.execute(self)
        if self._dicts:
            return DictCursorWrapper(cursor)
        elif self._objects:
            return NamedTupleCursorWrapper(cursor)
        else:
            return CursorWrapper(cursor)


class WriteQuery(Query):
    def __init__(self, table, *args, **kwargs):
        super(WriteQuery, self).__init__(*args, **kwargs)
        self.table = table


class Update(WriteQuery):
    def __init__(self, table, data=None, where=None, order_by=None, limit=None,
                 offset=None, on_conflict=None):
        super(Update, self).__init__(table, order_by=order_by, limit=limit,
                                     offset=offset)
        self.data = data
        self._where = where
        self._on_conflict = on_conflict

    @Node.copy
    def where(self, *expressions):
        if self._where is not None:
            expressions = (self._where,) + expressions
        self._where = reduce(operator.and_, expressions)

    @Node.copy
    def on_conflict(self, on_conflict):
        self._on_conflict = on_conflict

    def __sql__(self, Context ctx):
        with ctx.scope_values(subquery=True):
            ctx.literal('UPDATE ')
            if self._on_conflict:
                ctx.literal('OR %s ' % self._on_conflict)

            (ctx
             .sql(self.table)
             .literal(' SET ')
             .sql(CommaList([
                 List((key, SQL('='), value))
                 for key, value in self.data.items()])))

            if self._where is not None:
                ctx.literal(' WHERE ').sql(self._where)

            return self.apply_ordering(ctx)

    def execute(self, Database database):
        database.execute(self)
        return database.changes()


class Insert(WriteQuery):
    def __init__(self, table, data=None, columns=None, on_conflict=None):
        super(Insert, self).__init__(table)
        self.data = data
        self._columns = columns
        self._on_conflict = on_conflict

    @Node.copy
    def on_conflict(self, on_conflict):
        self._on_conflict = on_conflict

    def _simple_insert(self, Context ctx):
        columns, values = [], []
        for key, value in self.data.items():
            columns.append(key)
            if not isinstance(value, Node):
                value = Value(value)
            values.append(value)
        return (ctx
                .sql(EnclosedList(columns))
                .literal(' VALUES ')
                .sql(EnclosedList(values)))

    def _multi_insert(self, Context ctx):
        rows_iter = iter(self.data)
        columns = self._columns
        if not columns:
            try:
                row = next(rows_iter)
            except StopIteration:
                return ctx.sql('DEFAULT VALUES')
            columns = row.keys()
            rows_iter = itertools.chain(iter((row,)), rows_iter)

        ctx.sql(EnclosedList(columns)).literal(' VALUES ')
        all_values = []
        for row in rows_iter:
            values = []
            for column in columns:
                value = row[column]
                if not isinstance(value, Node):
                    value = Value(value)
                values.append(value)

            all_values.append(EnclosedList(values))

        return ctx.sql(CommaList(all_values))

    def _query_insert(self, Context ctx):
        return (ctx
                .sql(EnclosedList(self._columns))
                .literal(' ')
                .sql(self.data))

    def __sql__(self, Context ctx):
        super(Insert, self).__sql__(ctx)
        with ctx.scope_values():
            ctx.literal('INSERT ')
            if self._on_conflict:
                ctx.literal('OR %s ' % self._on_conflict)
            ctx.literal('INTO ').sql(self.table).literal(' ')
            if isinstance(self.data, dict):
                ctx = self._simple_insert(ctx)
            elif isinstance(self.data, SelectBase):
                ctx = self._query_insert(ctx)
            else:
                ctx = self._multi_insert(ctx)
        return ctx

    def execute(self, Database database):
        database.execute(self)
        return database.last_insert_rowid()


class Delete(WriteQuery):
    def __init__(self, table, where=None, **kwargs):
        super(Delete, self).__init__(table, **kwargs)
        self._where = where

    @Node.copy
    def where(self, *expressions):
        if self._where is not None:
            expressions = (self._where,) + expressions
        self._where = reduce(operator.and_, expressions)

    def __sql__(self, Context ctx):
        super(Delete, self).__sql__(ctx)

        with ctx.scope_values(subquery=True):
            ctx.literal('DELETE FROM ').sql(self.table)
            if self._where is not None:
                ctx.literal(' WHERE ').sql(self._where)
            return self.apply_ordering(ctx)

    def execute(self, Database database):
        database.execute(self)
        return database.changes()

SQLITE_DATETIME_FORMATS = (
    '%Y-%m-%d %H:%M:%S',
    '%Y-%m-%d %H:%M:%S.%f',
    '%Y-%m-%d',
    '%H:%M:%S',
    '%H:%M:%S.%f',
    '%H:%M')
SQLITE_DATE_FORMATS = (
    '%Y-%m-%d',
    '%Y-%m-%d %H:%M:%S',
    '%Y-%m-%d %H:%M:%S.%f')
SQLITE_TIME_FORMATS = (
    '%H:%M:%S',
    '%H:%M:%S.%f',
    '%H:%M',
    '%Y-%m-%d %H:%M:%S',
    '%Y-%m-%d %H:%M:%S.%f')

cpdef format_datetime(date_value):
    for date_format in SQLITE_DATETIME_FORMATS:
        try:
            return datetime.datetime.strptime(date_value, date_format)
        except ValueError:
            pass

cpdef format_date(date_value):
    cdef datetime.datetime date_obj
    for date_format in SQLITE_DATE_FORMATS:
        try:
            date_obj = datetime.datetime.strptime(date_value, date_format)
        except ValueError:
            pass
        else:
            return date_obj.date()

cpdef format_time(time_value):
    cdef datetime.datetime date_obj
    for date_format in SQLITE_TIME_FORMATS:
        try:
            date_obj = datetime.datetime.strptime(time_value, date_format)
        except ValueError:
            pass
        else:
            return date_obj.time()


pysqlite.register_adapter(decimal.Decimal, str)
pysqlite.register_adapter(datetime.date, str)
pysqlite.register_adapter(datetime.time, str)
pysqlite.register_converter('DATETIME', format_datetime)
pysqlite.register_converter('DATE', format_date)
pysqlite.register_converter('TIME', format_time)
