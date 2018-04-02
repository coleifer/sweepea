from cpython cimport datetime
from cpython.bytes cimport PyBytes_Check
from cpython.mem cimport PyMem_Free
from cpython.object cimport PyObject
from cpython.ref cimport Py_INCREF, Py_DECREF
from cpython.unicode cimport PyUnicode_AsUTF8String
from cpython.unicode cimport PyUnicode_Check
from libc.float cimport DBL_MAX
from libc.math cimport log, sqrt
from libc.stdint cimport uint8_t
from libc.stdint cimport uint32_t
from libc.stdlib cimport free, malloc, rand
from libc.string cimport memcpy, memset

from collections import namedtuple
from contextlib import contextmanager
from functools import partial
try:
    from functools import reduce
except ImportError:
    pass
from random import randint
import decimal
import hashlib
import itertools
import logging
import operator
import re
import sqlite3 as pysqlite
import struct
import threading
import uuid
import zlib
from sqlite3 import DatabaseError
from sqlite3 import InterfaceError
from sqlite3 import OperationalError


try:  # Python 2.7+
    from logging import NullHandler
except ImportError:
    class NullHandler(logging.Handler):
        def emit(self, record):
            pass

logger = logging.getLogger('sweepea')
logger.addHandler(NullHandler())


cdef inline bytes encode(key):
    cdef bytes bkey
    if PyUnicode_Check(key):
        bkey = PyUnicode_AsUTF8String(key)
    else:
        bkey = <bytes>key
    return bkey


cdef inline unicode decode(key):
    cdef unicode ukey
    if PyBytes_Check(key):
        ukey = key.decode('utf-8')
    else:
        ukey = key
    return ukey


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
    ctypedef struct sqlite3_backup
    ctypedef struct sqlite3_blob
    ctypedef struct sqlite3_context
    ctypedef struct sqlite3_value
    ctypedef long long sqlite3_int64
    ctypedef unsigned long long sqlite_uint64

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
    #cdef int SQLITE_DBSTATUS_CACHE_USED_SHARED = 11
    cdef int sqlite3_db_status(sqlite3 *, int op, int *pCur, int *pHigh, int reset)

    cdef int SQLITE_DELETE = 9
    cdef int SQLITE_INSERT = 18
    cdef int SQLITE_UPDATE = 23

    cdef int SQLITE_CONFIG_SINGLETHREAD = 1  # None
    cdef int SQLITE_CONFIG_MULTITHREAD = 2  # None
    cdef int SQLITE_CONFIG_SERIALIZED = 3  # None
    cdef int SQLITE_CONFIG_SCRATCH = 6  # void *, int sz, int N
    cdef int SQLITE_CONFIG_PAGECACHE = 7  # void *, int sz, int N
    cdef int SQLITE_CONFIG_HEAP = 8  # void *, int nByte, int min
    cdef int SQLITE_CONFIG_MEMSTATUS = 9  # boolean
    cdef int SQLITE_CONFIG_LOOKASIDE = 13  # int, int
    cdef int SQLITE_CONFIG_URI = 17  # int
    cdef int SQLITE_CONFIG_MMAP_SIZE = 22  # sqlite3_int64, sqlite3_int64
    cdef int SQLITE_CONFIG_STMTJRNL_SPILL = 26  # int nByte
    cdef int SQLITE_DBCONFIG_MAINDBNAME = 1000  # const char*
    cdef int SQLITE_DBCONFIG_LOOKASIDE = 1001  # void* int int
    cdef int SQLITE_DBCONFIG_ENABLE_FKEY = 1002  # int int*
    cdef int SQLITE_DBCONFIG_ENABLE_TRIGGER = 1003  # int int*
    cdef int SQLITE_DBCONFIG_ENABLE_FTS3_TOKENIZER = 1004  # int int*
    cdef int SQLITE_DBCONFIG_ENABLE_LOAD_EXTENSION = 1005  # int int*
    cdef int SQLITE_DBCONFIG_NO_CKPT_ON_CLOSE = 1006  # int int*
    cdef int SQLITE_DBCONFIG_ENABLE_QPSG = 1007  # int int*

    cdef int sqlite3_config(int, ...)
    cdef int sqlite3_db_config(sqlite3*, int op, ...)

    # Misc.
    cdef int sqlite3_busy_handler(sqlite3 *db, int(*)(void *, int), void *)
    cdef int sqlite3_sleep(int ms)
    cdef sqlite3_backup *sqlite3_backup_init(
        sqlite3 *pDest,
        const char *zDestName,
        sqlite3 *pSource,
        const char *zSourceName)

    # Backup.
    cdef int sqlite3_backup_step(sqlite3_backup *p, int nPage)
    cdef int sqlite3_backup_finish(sqlite3_backup *p)
    cdef int sqlite3_backup_remaining(sqlite3_backup *p)
    cdef int sqlite3_backup_pagecount(sqlite3_backup *p)

    # Error handling.
    cdef int sqlite3_errcode(sqlite3 *db)
    cdef int sqlite3_errstr(int)
    cdef const char *sqlite3_errmsg(sqlite3 *db)

    cdef int sqlite3_blob_open(
          sqlite3*,
          const char *zDb,
          const char *zTable,
          const char *zColumn,
          sqlite3_int64 iRow,
          int flags,
          sqlite3_blob **ppBlob)
    cdef int sqlite3_blob_reopen(sqlite3_blob *, sqlite3_int64)
    cdef int sqlite3_blob_close(sqlite3_blob *)
    cdef int sqlite3_blob_bytes(sqlite3_blob *)
    cdef int sqlite3_blob_read(sqlite3_blob *, void *Z, int N, int iOffset)
    cdef int sqlite3_blob_write(sqlite3_blob *, const void *z, int n,
                                int iOffset)


cdef extern from "_pysqlite/connection.h":
    ctypedef struct pysqlite_Connection:
        sqlite3* db
        double timeout
        int initialized
        PyObject* isolation_level
        char* begin_statement


# The sweepea_vtab struct embeds the base sqlite3_vtab struct, and adds a field
# to store a reference to the Python implementation.
ctypedef struct sweepea_vtab:
    sqlite3_vtab base
    void *table_func_cls


# Like sweepea_vtab, the sweepea_cursor embeds the base sqlite3_vtab_cursor and
# adds fields to store references to the current index, the Python
# implementation, the current rows' data, and a flag for whether the cursor has
# been exhausted.
ctypedef struct sweepea_cursor:
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
        sweepea_vtab *pNew

    rc = sqlite3_declare_vtab(
        db,
        encode('CREATE TABLE x(%s);' %
               table_func_cls.get_table_columns_declaration()))
    if rc == SQLITE_OK:
        pNew = <sweepea_vtab *>sqlite3_malloc(sizeof(pNew[0]))
        memset(<char *>pNew, 0, sizeof(pNew[0]))
        ppVtab[0] = &(pNew.base)

        pNew.table_func_cls = <void *>table_func_cls
        Py_INCREF(table_func_cls)

    return rc


cdef int pwDisconnect(sqlite3_vtab *pBase) with gil:
    cdef:
        sweepea_vtab *pVtab = <sweepea_vtab *>pBase
        object table_func_cls = <object>(pVtab.table_func_cls)

    Py_DECREF(table_func_cls)
    sqlite3_free(pVtab)
    return SQLITE_OK


# The xOpen method is used to initialize a cursor. In this method we
# instantiate the TableFunction class and zero out a new cursor for iteration.
cdef int pwOpen(sqlite3_vtab *pBase, sqlite3_vtab_cursor **ppCursor) with gil:
    cdef:
        sweepea_vtab *pVtab = <sweepea_vtab *>pBase
        sweepea_cursor *pCur
        object table_func_cls = <object>pVtab.table_func_cls

    pCur = <sweepea_cursor *>sqlite3_malloc(sizeof(pCur[0]))
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
        sweepea_cursor *pCur = <sweepea_cursor *>pBase
        object table_func = <object>pCur.table_func
    Py_DECREF(table_func)
    sqlite3_free(pCur)
    return SQLITE_OK


# Iterate once, advancing the cursor's index and assigning the row data to the
# `row_data` field on the sweepea_cursor struct.
cdef int pwNext(sqlite3_vtab_cursor *pBase) with gil:
    cdef:
        sweepea_cursor *pCur = <sweepea_cursor *>pBase
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
        bytes bval
        sweepea_cursor *pCur = <sweepea_cursor *>pBase
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
        bval = encode(value)
        sqlite3_result_text(
            ctx,
            <const char *>bval,
            -1,
            <sqlite3_destructor_type>-1)
    elif isinstance(value, bool):
        sqlite3_result_int(ctx, int(value))
    else:
        sqlite3_result_error(
            ctx,
            encode('Unsupported type %s' % type(value)),
            -1)
        return SQLITE_ERROR

    return SQLITE_OK


cdef int pwRowid(sqlite3_vtab_cursor *pBase, sqlite3_int64 *pRowid):
    cdef:
        sweepea_cursor *pCur = <sweepea_cursor *>pBase
    pRowid[0] = <sqlite3_int64>pCur.idx
    return SQLITE_OK


# Return a boolean indicating whether the cursor has been consumed.
cdef int pwEof(sqlite3_vtab_cursor *pBase):
    cdef:
        sweepea_cursor *pCur = <sweepea_cursor *>pBase
    if pCur.stopped:
        return 1
    return 0


# The filter method is called on the first iteration. This method is where we
# get access to the parameters that the function was called with, and call the
# TableFunction's `initialize()` function.
cdef int pwFilter(sqlite3_vtab_cursor *pBase, int idxNum,
                  const char *idxStr, int argc, sqlite3_value **argv) with gil:
    cdef:
        sweepea_cursor *pCur = <sweepea_cursor *>pBase
        object table_func = <object>pCur.table_func
        dict query = {}
        int idx
        int value_type
        tuple row_data
        void *row_data_raw

    if not idxStr or argc == 0 and len(table_func.params):
        return SQLITE_ERROR
    elif idxStr:
        params = decode(idxStr).split(',')
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
            query[param] = decode(sqlite3_value_text(value))
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
        sweepea_vtab *pVtab = <sweepea_vtab *>pBase
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
        joinedCols = encode(','.join(columns))
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

    cdef create_module(self, pysqlite_Connection* sqlite_conn):
        cdef:
            bytes name = encode(self.table_function.name)
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
            <const char *>name,
            &self.module,
            <void *>(self.table_function))

        Py_INCREF(self)

        return rc == SQLITE_OK


class TableFunction(object):
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


def rank(py_match_info, *raw_weights):
    cdef:
        unsigned int *match_info
        unsigned int *phrase_info
        bytes _match_info_buf = bytes(py_match_info)
        char *match_info_buf = _match_info_buf
        int argc = len(raw_weights)
        int ncol, nphrase, icol, iphrase, hits, global_hits
        int P_O = 0, C_O = 1, X_O = 2
        double score = 0.0, weight
        double *weights

    match_info = <unsigned int *>match_info_buf
    nphrase = match_info[P_O]
    ncol = match_info[C_O]

    weights = <double *>malloc(sizeof(double) * ncol)
    for icol in range(ncol):
        if icol < argc:
            weights[icol] = <double>raw_weights[icol]
        else:
            weights[icol] = 1.0

    for iphrase in range(nphrase):
        phrase_info = &match_info[X_O + iphrase * ncol * 3]
        for icol in range(ncol):
            weight = weights[icol]
            if weight == 0:
                continue
            hits = phrase_info[3 * icol]
            global_hits = phrase_info[3 * icol + 1]
            if hits > 0:
                score += weight * (<double>hits / <double>global_hits)

    free(weights)
    return -1 * score


def lucene(py_match_info, *raw_weights):
    # Usage: lucene(matchinfo(table, 'pcnalx'), 1)
    cdef:
        unsigned int *match_info
        unsigned int *phrase_info
        bytes _match_info_buf = bytes(py_match_info)
        char *match_info_buf = _match_info_buf
        int argc = len(raw_weights)
        int term_count, col_count
        double total_docs, term_frequency,
        double doc_length, docs_with_term, avg_length
        double idf, weight, rhs, denom
        double *weights
        int P_O = 0, C_O = 1, N_O = 2, L_O, X_O
        int i, j, x

        double score = 0.0

    match_info = <unsigned int *>match_info_buf
    term_count = match_info[P_O]
    col_count = match_info[C_O]
    total_docs = match_info[N_O]

    L_O = 3 + col_count
    X_O = L_O + col_count

    weights = <double *>malloc(sizeof(double) * col_count)
    for i in range(col_count):
        if argc == 0:
            weights[i] = 1.
        elif i < argc:
            weights[i] = <double>raw_weights[i]
        else:
            weights[i] = 0

    for i in range(term_count):
        for j in range(col_count):
            weight = weights[j]
            if weight == 0:
                continue
            doc_length = match_info[L_O + j]
            x = X_O + (3 * j * (i + 1))
            term_frequency = match_info[x]
            docs_with_term = match_info[x + 2]
            idf = log(total_docs / (docs_with_term + 1.))
            tf = sqrt(term_frequency)
            fieldNorms = 1.0 / sqrt(doc_length)
            score += (idf * tf * fieldNorms)

    free(weights)
    return -1 * score


def bm25(py_match_info, *raw_weights):
    # Usage: bm25(matchinfo(table, 'pcnalx'), 1)
    # where the second parameter is the index of the column and
    # the 3rd and 4th specify k and b.
    cdef:
        unsigned int *match_info
        unsigned int *phrase_info
        bytes _match_info_buf = bytes(py_match_info)
        char *match_info_buf = _match_info_buf
        int argc = len(raw_weights)
        int term_count, col_count
        double B = 0.75, K = 1.2, D
        double total_docs, term_frequency,
        double doc_length, docs_with_term, avg_length
        double idf, weight, rhs, denom
        double *weights
        int P_O = 0, C_O = 1, N_O = 2, A_O = 3, L_O, X_O
        int i, j, x

        double score = 0.0

    match_info = <unsigned int *>match_info_buf
    term_count = match_info[P_O]
    col_count = match_info[C_O]
    total_docs = match_info[N_O]

    L_O = A_O + col_count
    X_O = L_O + col_count

    weights = <double *>malloc(sizeof(double) * col_count)
    for i in range(col_count):
        if argc == 0:
            weights[i] = 1.
        elif i < argc:
            weights[i] = <double>raw_weights[i]
        else:
            weights[i] = 0

    for i in range(term_count):
        for j in range(col_count):
            weight = weights[j]
            if weight == 0:
                continue
            avg_length = match_info[A_O + j]
            doc_length = match_info[L_O + j]
            if avg_length == 0:
                D = 0
            else:
                D = 1 - B + (B * (doc_length / avg_length))

            x = X_O + (3 * j * (i + 1))
            term_frequency = match_info[x]
            docs_with_term = match_info[x + 2]
            idf = max(
                log(
                    (total_docs - docs_with_term + 0.5) /
                    (docs_with_term + 0.5)),
                0)
            denom = term_frequency + (K * D)
            if denom == 0:
                rhs = 0
            else:
                rhs = (term_frequency * (K + 1)) / denom

            score += (idf * rhs) * weight

    free(weights)
    return -1 * score


cdef uint32_t murmurhash2(const unsigned char *key, ssize_t nlen,
                          uint32_t seed):
    cdef:
        uint32_t m = 0x5bd1e995
        int r = 24
        const unsigned char *data = key
        uint32_t h = seed ^ nlen
        uint32_t k

    while nlen >= 4:
        k = <uint32_t>((<uint32_t *>data)[0])

        k *= m
        k = k ^ (k >> r)
        k *= m

        h *= m
        h = h ^ k

        data += 4
        nlen -= 4

    if nlen == 3:
        h = h ^ (data[2] << 16)
    if nlen >= 2:
        h = h ^ (data[1] << 8)
    if nlen >= 1:
        h = h ^ (data[0])
        h *= m

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
        return murmurhash2(<unsigned char *>bkey, len(bkey), nseed)
    return 0


cdef bint regexp(basestring value, basestring regex):
    # Expose regular expression matching in SQLite.
    return re.search(regex, value, re.I) is not None


def make_hash(hash_impl):
    def inner(*items):
        state = hash_impl()
        for item in items:
            state.update(encode(item))
        return state.hexdigest()
    return inner


hash_md5 = make_hash(hashlib.md5)
hash_sha1 = make_hash(hashlib.sha1)
hash_sha256 = make_hash(hashlib.sha256)


cdef class median(object):
    cdef:
        int ct
        list items

    def __init__(self):
        self.ct = 0
        self.items = []

    cdef selectKth(self, int k, int s=0, int e=-1):
        cdef:
            int idx
        if e < 0:
            e = len(self.items)
        idx = randint(s, e-1)
        idx = self.partition_k(idx, s, e)
        if idx > k:
            return self.selectKth(k, s, idx)
        elif idx < k:
            return self.selectKth(k, idx + 1, e)
        else:
            return self.items[idx]

    cdef int partition_k(self, int pi, int s, int e):
        cdef:
            int i, x

        val = self.items[pi]
        # Swap pivot w/last item.
        self.items[e - 1], self.items[pi] = self.items[pi], self.items[e - 1]
        x = s
        for i in range(s, e):
            if self.items[i] < val:
                self.items[i], self.items[x] = self.items[x], self.items[i]
                x += 1
        self.items[x], self.items[e-1] = self.items[e-1], self.items[x]
        return x

    def step(self, item):
        self.items.append(item)
        self.ct += 1

    def finalize(self):
        if self.ct == 0:
            return None
        elif self.ct < 3:
            return self.items[0]
        else:
            return self.selectKth(self.ct / 2)


def _register_functions(database, pairs):
    for func, name in pairs:
        database.func(name)(func)


def register_hash_functions(database):
    _register_functions(database, (
        (murmurhash, 'murmurhash'),
        (hash_md5, 'md5'),
        (hash_sha1, 'sha1'),
        (hash_sha256, 'sha256'),
        (zlib.adler32, 'adler32'),
        (zlib.crc32, 'crc32')))


def register_rank_functions(database):
    _register_functions(database, (
        (bm25, 'fts_bm25'),
        (lucene, 'fts_lucene'),
        (rank, 'fts_rank')))


class DateSeries(TableFunction):
    params = ['start', 'stop', 'step_seconds']
    columns = ['date']
    name = 'date_series'

    def initialize(self, start, stop, step_seconds=86400):
        self.start = format_datetime(start)
        self.stop = format_datetime(stop)
        self.step_seconds_i = int(step_seconds)
        self.step_seconds = datetime.timedelta(seconds=self.step_seconds_i)
        self.format = self.get_format()

    def get_format(self):
        if self.is_zero_time(self.start) and self.step_seconds_i >= 86400:
            return '%Y-%m-%d'
        elif self.is_zero_date(self.start) and self.is_zero_date(self.stop) \
                and self.step_seconds_i < 86400:
            return '%H:%M:%S'
        else:
            return '%Y-%m-%d %H:%M:%S'

    def is_zero_time(self, dt):
        return dt.hour == dt.minute == dt.second == 0

    def is_zero_date(self, dt):
        return (dt.year, dt.month, dt.day) == (1900, 1, 1)

    def iterate(self, idx):
        if self.start > self.stop:
            raise StopIteration
        current = self.start
        self.start += self.step_seconds
        return (current.strftime(self.format),)


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


class DoesNotExist(Exception): pass


cdef class CursorWrapper(object)  # Forward declaration.


cdef class ResultIterator(object):
    cdef:
        CursorWrapper cursor_wrapper
        int index

    def __init__(self, cursor_wrapper):
        self.cursor_wrapper = cursor_wrapper
        self.index = 0

    def __iter__(self):
        return self

    def __next__(self):
        if self.index < self.cursor_wrapper.count:
            obj = self.cursor_wrapper.result_cache[self.index]
        elif not self.cursor_wrapper.is_populated:
            self.cursor_wrapper.iterate()
            obj = self.cursor_wrapper.result_cache[self.index]
        else:
            raise StopIteration
        self.index += 1
        return obj
    next = __next__


cdef class CursorWrapper(object):
    cdef:
        bint is_initialized, is_populated
        int count
        list columns, result_cache
        object cursor

    def __init__(self, cursor):
        self.cursor = cursor
        self.count = 0
        self.is_initialized = False
        self.is_populated = False
        self.result_cache = []

    def __iter__(self):
        if self.is_populated:
            return iter(self.result_cache)
        return ResultIterator(self)

    def __getitem__(self, item):
        if isinstance(item, slice):
            stop = item.stop
            if stop is None or stop < 0:
                self.fill_cache()
            else:
                self.fill_cache(stop)
            return self.result_cache[item]
        elif isinstance(item, int):
            fill_line = item + 1 if item >= 0 else item
            self.fill_cache(fill_line)
            return self.result_cache[item]
        else:
            raise ValueError('CursorWrapper only supports integer and slice '
                             'indexes.')

    def __len__(self):
        self.fill_cache()
        return self.count

    cdef initialize(self):
        self.columns = [col[0][col[0].find('.') + 1:]
                        for col in self.cursor.description]

    def iterate(self, cache=True):
        cdef:
            tuple row = self.cursor.fetchone()

        if row is None:
            self.is_populated = True
            self.cursor.close()
            raise StopIteration
        elif not self.is_initialized:
            self.initialize()  # Lazy initialization.
            self.is_initialized = True

        self.count += 1
        result = self.transform(row)
        if cache:
            self.result_cache.append(result)
        return result

    cdef transform(self, tuple row):
        return row

    def iterator(self):
        while True:
            yield self.iterate(cache=False)

    cpdef fill_cache(self, n=None):
        cdef:
            int counter = -1 if n is None else <int>n
            ResultIterator iterator = ResultIterator(self)

        iterator.index = self.count
        while not self.is_populated and (counter < 0 or counter > self.count):
            try:
                iterator.next()
            except StopIteration:
                break

    cpdef first(self):
        if not self.is_populated:
            self.fill_cache(1)
        if self.result_cache:
            return self.result_cache[0]

    cpdef get(self):
        obj = self.first()
        if obj is None:
            raise DoesNotExist('No objects found matching this query.')
        else:
            return obj

    cpdef scalar(self):
        return self.get()[0]


cdef class DictCursorWrapper(CursorWrapper):
    cdef transform(self, tuple row):
        cdef:
            basestring column
            dict accum = {}
            int i
        for i, column in enumerate(self.columns):
            accum[column] = row[i]
        return accum


cdef class NamedTupleCursorWrapper(CursorWrapper):
    cdef:
        object tuple_class

    cdef initialize(self):
        CursorWrapper.initialize(self)
        self.tuple_class = namedtuple('Row', self.columns)

    cdef transform(self, tuple row):
        return self.tuple_class(*row)


cdef class ObjectCursorWrapper(DictCursorWrapper):
    cdef:
        public object constructor

    def __init__(self, cursor, constructor):
        super(ObjectCursorWrapper, self).__init__(cursor)
        self.constructor = constructor

    cdef transform(self, tuple row):
        cdef:
            dict accum = DictCursorWrapper.transform(self, row)
        return self.constructor(**accum)


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
            obj = decode(sqlite3_value_text(value))
        elif value_type == SQLITE_BLOB:
            obj = <bytes>sqlite3_value_blob(value)
        else:
            obj = None
        accum.append(obj)

    return accum


cdef _python_to_sqlite(sqlite3_context *context, value):
    cdef bytes bval
    # Store a Python value in a sqlite3_context object. For instance, store the
    # result to a user-defined function call so it is accessible in SQLite.
    if value is None:
        sqlite3_result_null(context)
    elif isinstance(value, (int, long)):
        sqlite3_result_int64(context, <sqlite3_int64>value)
    elif isinstance(value, float):
        sqlite3_result_double(context, <double>value)
    elif isinstance(value, basestring):
        bval = encode(value)
        sqlite3_result_text(context, <const char *>bval, -1,
                            <sqlite3_destructor_type>-1)
    elif isinstance(value, bool):
        sqlite3_result_int(context, int(value))
    else:
        sqlite3_result_error(
            context,
            encode('Unsupported type %s' % type(value)),
            -1)
        return SQLITE_ERROR

    return SQLITE_OK


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
    fn(query, decode(database), decode(table), <int>rowid)


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
                 hash_functions=False, **kwargs):
        self.database = database
        self.connect_kwargs = {}
        self._local = ConnectionLocal()
        self._lock = threading.Lock()

        # Registers for user-defined extensions.
        self._aggregates = {}
        self._collations = {}
        self._functions = {}
        self._table_functions = []

        kwargs.setdefault('detect_types', pysqlite.PARSE_DECLTYPES)
        self.init(database, **kwargs)

        self._pragmas = list(pragmas) if pragmas else []
        if journal_mode is not None:
            self._pragmas.append(('journal_mode', journal_mode))

        # Register built-in custom functions.
        if rank_functions:
            register_rank_functions(self)
            self.aggregate('median')(median)
        if regex_function:
            self.func('regexp')(regexp)
        if hash_functions:
            register_hash_functions(self)

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

    cpdef connect(self, reuse_if_open=False):
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
                if reuse_if_open:
                    return False
                raise OperationalError('Connection already open in this '
                                       'thread.')
            self._local.conn = conn = self._connect()
            self._local.closed = False
            self.initialize_connection(conn)

        return True

    cpdef _connect(self):
        return pysqlite.connect(self.database, **self.connect_kwargs)

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
                self._close(self._local.conn)
                self._local.closed = True
                self._local.transactions = []
                return True
            return False

    cpdef _close(self, conn):
        conn.close()

    def initialize_connection(self, conn):
        """
        Initialize the connection by setting per-connection-state, registering
        user-defined extensions, and configuring any hooks.
        """
        conn.isolation_level = None  # Disable transaction state-machine.

        self._set_pragmas(conn)

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
        def decorator(klass):
            agg_name = name or klass.__name__
            self._aggregates[agg_name] = klass
            if not self.is_closed():
                self._create_aggregate(self.connection(), klass, agg_name)
            return klass
        return decorator

    cdef _create_collation(self, conn, fn, name):
        conn.create_collation(name, fn)

    def collation(self, name=None):
        def decorator(fn):
            collation_name = name or fn.__name__
            self._collations[collation_name] = fn
            if not self.is_closed():
                self._create_collation(self.connection(), fn, collation_name)
        return decorator

    cdef _create_function(self, conn, fn, name, nparams, deterministic):
        conn.create_function(name, nparams, fn)

    def func(self, name=None, n=-1, deterministic=True):
        def decorator(fn):
            fn_name = name or fn.__name__
            self._functions[fn_name] = (fn, n, deterministic)
            if not self.is_closed():
                self._create_function(self.connection(), fn, fn_name, n,
                                      deterministic)
            return fn
        return decorator

    def table_function(self, name=None):
        def decorator(klass):
            if name is not None:
                klass.name = name
            self._table_functions.append(klass)
            if not self.is_closed():
                klass.register(self.connection())
            return klass
        return decorator

    def on_commit(self, fn):
        self._commit_hook = fn
        if not self.is_closed():
            self._set_commit_hook(self.connection(), fn)
        return fn

    def _set_commit_hook(self, connection, fn):
        cdef pysqlite_Connection *conn = <pysqlite_Connection *>connection
        if fn is None:
            sqlite3_commit_hook(conn.db, NULL, NULL)
        else:
            sqlite3_commit_hook(conn.db, _commit_callback, <void *>fn)

    def on_rollback(self, fn):
        self._rollback_hook = fn
        if not self.is_closed():
            self._set_rollback_hook(self.connection(), fn)
        return fn

    def _set_rollback_hook(self, connection, fn):
        cdef pysqlite_Connection *conn = <pysqlite_Connection *>connection
        if fn is None:
            sqlite3_rollback_hook(conn.db, NULL, NULL)
        else:
            sqlite3_rollback_hook(conn.db, _rollback_callback, <void *>fn)

    def on_update(self, fn):
        self._update_hook = fn
        if not self.is_closed():
            self._set_update_hook(self.connection(), fn)
        return fn

    def _set_update_hook(self, connection, fn):
        cdef pysqlite_Connection *conn = <pysqlite_Connection *>connection
        if fn is None:
            sqlite3_update_hook(conn.db, NULL, NULL)
        else:
            sqlite3_update_hook(conn.db, _update_callback, <void *>fn)

    cpdef bint is_closed(self):
        return self._local.closed

    cpdef connection(self):
        # Internal method for quickly getting (or opening) a connection.
        if self._local.closed:
            self.connect()
        return self._local.conn

    cpdef execute_sql(self, sql, params=None, commit=True):
        """Execute the given SQL query and return the cursor."""
        logger.debug((sql, params))
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

    def _set_pragmas(self, conn):
        if self._pragmas:
            cursor = conn.cursor()
            for pragma, value in self._pragmas:
                cursor.execute('PRAGMA %s = %s;' % (pragma, value))
            cursor.close()

    def pragma(self, key, value=__sentinel__, permanent=False):
        sql = 'PRAGMA %s' % key
        if value is not __sentinel__:
            sql += ' = %s' % (value or 0)
            if permanent:
                pragmas = dict(self._pragmas or ())
                pragmas[key] = value
                self._pragmas = list(pragmas.items())
        elif permanent:
            raise ValueError('Cannot specify a permanent pragma without value')
        row = self.execute_sql(sql).fetchone()
        if row:
            return row[0]

    def add_pragma(self, key, value):
        self.pragma(key, value, True)

    def remove_pragma(self, key):
        self._pragmas = [(k, v) for k, v in self._pragmas if k != key]

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
        return BoundTable(self, name)

    def __enter__(self):
        self.connection()
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
        self.close()

    cpdef last_insert_rowid(self):
        cdef:
            pysqlite_Connection *c = <pysqlite_Connection *>(self._local.conn)
        _check_connection(c)
        return <int>sqlite3_last_insert_rowid(c.db)

    @property
    def autocommit(self):
        """Return a boolean value indicating the status of autocommit."""
        cdef pysqlite_Connection *c = <pysqlite_Connection *>(self._local.conn)
        _check_connection(c)
        return bool(sqlite3_get_autocommit(c.db))

    cpdef changes(self):
        cdef pysqlite_Connection *c = <pysqlite_Connection *>(self._local.conn)
        _check_connection(c)
        return sqlite3_changes(c.db)

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

    cdef push_transaction(self, txn):
        self._local.transactions.append(txn)

    cdef pop_transaction(self):
        self._local.transactions.pop()

    cdef transaction_depth(self):
        return len(self._local.transactions)

    cdef top_transaction(self):
        if self._local.transactions:
            return self._local.transactions[-1]

    cpdef _manual manual_commit(self):
        return _manual(self)

    cpdef _atomic atomic(self):
        return _atomic(self)

    cpdef _transaction transaction(self):
        return _transaction(self)

    cpdef _savepoint savepoint(self):
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

    def backup(self, Database destination):
        return backup(self.connection(), destination.connection())

    def backup_to_file(self, filename):
        return backup_to_file(self.connection(), filename)

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
    schema_used = __dbstatus__(SQLITE_DBSTATUS_SCHEMA_USED, False, True)
    statement_used = __dbstatus__(SQLITE_DBSTATUS_STMT_USED, False, True)
    cache_hit = __dbstatus__(SQLITE_DBSTATUS_CACHE_HIT, False, True)
    cache_miss = __dbstatus__(SQLITE_DBSTATUS_CACHE_MISS, False, True)
    cache_write = __dbstatus__(SQLITE_DBSTATUS_CACHE_WRITE, False, True)


cdef class _manual(_callable_context_manager):
    cdef:
        Database db

    def __init__(self, Database db):
        self.db = db

    def __enter__(self):
        top = self.db.top_transaction()
        if top and not isinstance(self.db.top_transaction(), _manual):
            raise ValueError('Cannot enter manual commit block while a '
                             'transaction is active.')
        self.db.push_transaction(self)

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.db.pop_transaction() is not self:
            raise ValueError('Transaction stack corrupted while exiting '
                             'manual commit block.')


cdef class _atomic(_callable_context_manager):
    cdef:
        Database db
        object _helper

    def __init__(self, Database db):
        self.db = db

    def __enter__(self):
        if self.db.transaction_depth() == 0:
            self._helper = self.db.transaction()
        else:
            self._helper = self.db.savepoint()
            if isinstance(self.db.top_transaction(), _manual):
                raise ValueError('Cannot enter atomic commit block while in '
                                 'manual commit mode.')
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
        if self.db.transaction_depth() == 0:
            self._begin()
        self.db.push_transaction(self)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            if exc_type:
                self.rollback(False)
            elif self.db.transaction_depth() == 1:
                try:
                    self.commit(False)
                except:
                    self.rollback(False)
                    raise
        finally:
            self.db.pop_transaction()


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

    cpdef _begin(self):
        self._execute('SAVEPOINT %s;' % self.quoted_sid)

    cpdef commit(self, begin=True):
        self._execute('RELEASE SAVEPOINT %s;' % self.quoted_sid)
        if begin: self._begin()

    cpdef rollback(self):
        self._execute('ROLLBACK TO SAVEPOINT %s;' % self.quoted_sid)

    def __enter__(self):
        self._begin()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            self.rollback()
        else:
            try:
                self.commit(begin=False)
            except:
                self.rollback()
                raise


# Query Builder.
SCOPE_NORMAL = 0
SCOPE_SOURCE = 1
SCOPE_VALUES = 2
SCOPE_CTE = 3
SCOPE_COLUMN = 4

DJANGO_OPERATIONS = {
    'eq': '=',
    'ne': '!=',
    'gte': '>=',
    'gt': '>',
    'lte': '<=',
    'lt': '<',
    'in': 'IN',
    'is': 'IS',
    'is_not': 'IS NOT',
    'like': 'LIKE',
    'glob': 'GLOB',
    'regexp': 'REGEXP'}


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
        public list stack, _sql, _values
        public State state

    def __init__(self):
        self.stack = []
        self._sql = []
        self._values = []
        self.state = State()

    @property
    def scope(self):
        return self.state.scope

    @property
    def parentheses(self):
        return self.state.parentheses

    @property
    def subquery(self):
        return self.state.subquery

    def __call__(self, **overrides):
        if overrides and overrides.get('scope') == self.scope:
            del overrides['scope']

        self.stack.append(self.state)
        self.state = self.state(**overrides)
        return self

    scope_normal = __scope_context__(SCOPE_NORMAL)
    scope_source = __scope_context__(SCOPE_SOURCE)
    scope_values = __scope_context__(SCOPE_VALUES)
    scope_cte = __scope_context__(SCOPE_CTE)
    scope_column = __scope_context__(SCOPE_COLUMN)

    def __enter__(self):
        if self.parentheses:
            self.literal('(')
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.parentheses:
            self.literal(')')
        self.state = self.stack.pop()

    cpdef Context sql(self, obj):
        if isinstance(obj, (Node, Context)):
            obj.__sql__(self)
            return self
        else:
            return self.sql(Value(obj))

    cpdef Context literal(self, sql):
        self._sql.append(sql)
        return self

    cdef Context value(self, value):
        self._values.append(value)
        return self

    cdef Context __sql__(self, Context ctx):
        ctx._sql.extend(self._sql)
        ctx._values.extend(self._values)
        return ctx

    cpdef tuple parse(self, node):
        return self.sql(node).query()

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

    def __sql__(self, ctx):
        raise NotImplementedError

    @staticmethod
    def copy(method):
        def inner(self, *args, **kwargs):
            clone = self.clone()
            method(clone, *args, **kwargs)
            return clone
        return inner

    def unwrap(self):
        return self


class Source(Node):
    c = _DynamicColumn()

    def __init__(self, alias=None):
        super(Source, self).__init__()
        self._alias = alias
        self._query_name = None

    @Node.copy
    def alias(self, alias=None):
        self._alias = alias

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

    def apply_column(self, ctx):
        return ctx.sql(Entity(self._alias or self._query_name))


def __join__(join_type='INNER', inverted=False):
    def method(self, other):
        if inverted:
            self, other = other, self
        return Join(self, other, join_type=join_type)
    return method


class BaseTable(Source):
    """
    Base class for table-like objects, which support JOINs via operator
    overloading.
    """
    __and__ = __join__('INNER')
    __add__ = __join__('LEFT_OUTER')
    __or__ = __join__('FULL_OUTER')
    __mul__ = __join__('CROSS')
    __rand__ = __join__('INNER', inverted=True)
    __radd__ = __join__('LEFT_OUTER', inverted=True)
    __ror__ = __join__('FULL_OUTER', inverted=True)
    __rmul__ = __join__('CROSS', inverted=True)


class Table(BaseTable):
    def __init__(self, name, columns=None, schema=None, alias=None):
        self._name = self._query_name = name
        self._columns = columns
        self._schema = schema
        self._path = (self._schema, name) if self._schema else (name,)
        super(Table, self).__init__(alias=alias)

        if self._columns:
            self.c = _ExplicitColumn()
            for column in self._columns:
                setattr(self, column, Column(self, column))

    def _get_column(self, name):
        if self._columns:
            return getattr(self, name)
        else:
            return getattr(self.c, name)

    def __hash__(self):
        return hash((self._schema, self._name, self._alias))

    def clone(self):
        return Table(self._name, self._columns, schema=self._schema,
                     alias=self._alias)

    def bind(self, database):
        return BoundTable(database, self._name, self._columns,
                          schema=self._schema, alias=self._alias)

    def select(self, *selection):
        if not selection and self._columns:
            selection = [Column(self, column) for column in self._columns]
        return Select((self,), selection)

    def insert(self, data=None, columns=None, on_conflict=None, **kwargs):
        if kwargs:
            data = data or {}
            for key, value in kwargs.items():
                data[Column(self, key)] = value
        return Insert(self, data, columns=columns, on_conflict=on_conflict)

    def update(self, data=None, on_conflict=None, **kwargs):
        if kwargs:
            data = data or {}
            for key, value in kwargs.items():
                data[Column(self, key)] = value
        return Update(self, data, on_conflict=on_conflict)

    def delete(self):
        return Delete(self)

    def filter(self, **kwargs):
        accum = []
        for key, value in kwargs.items():
            if '__' in key:
                key, op = key.rsplit('__', 1)
            else:
                op = 'eq'

            if op not in DJANGO_OPERATIONS:
                raise ValueError('Unrecognized operation: "%s". Supported '
                                 'operations are: %s.' % (
                                     op, ', '.join(sorted(DJANGO_OPERATIONS))))

            if self._columns and key not in self._columns:
                raise ValueError('Unable to find column "%s" on table "%s". '
                                 'Available columns are: %s' %
                                 (key, self._name, ', '.join(self._columns)))

            col = Column(self, key)
            accum.append(Expression(col, DJANGO_OPERATIONS[op], value))

        return self.select().where(reduce(operator.and_, accum))

    def rank(self):
        return fn.fts_rank(fn.matchinfo(Entity(self._name), 'pcx'))

    def bm25(self):
        return fn.fts_bm25(fn.matchinfo(Entity(self._name), 'pcnalx'))

    def match(self, rhs):
        return Expression(Entity(self._name), 'MATCH', rhs)

    def __sql__(self, Context ctx):
        if not self._alias:
            return ctx.sql(Entity(*self._path))

        if ctx.scope == SCOPE_SOURCE:
            ctx.sql(Entity(*self._path)).literal(' AS ')
        return ctx.sql(Entity(self._alias))


class BoundTable(Table):
    def __init__(self, database, *args, **kwargs):
        self._database = database
        super(BoundTable, self).__init__(*args, **kwargs)

    def clone(self):
        return BoundTable(self._database, self._name, self._columns,
                          schema=self._schema, alias=self._alias)

    def select(self, *selection):
        if not selection and self._columns:
            selection = [Column(self, column) for column in self._columns]
        return BoundSelect(self._database, (self,), selection)

    def insert(self, data=None, columns=None, on_conflict=None, **kwargs):
        if kwargs:
            data = data or {}
            for key, value in kwargs.items():
                data[Column(self, key)] = value
        return BoundInsert(self._database, self, data, columns=columns,
                           on_conflict=on_conflict)

    def update(self, data=None, on_conflict=None, **kwargs):
        if kwargs:
            data = data or {}
            for key, value in kwargs.items():
                data[Column(self, key)] = value
        return BoundUpdate(self._database, self, data, on_conflict=on_conflict)

    def delete(self):
        return BoundDelete(self._database, self)


class Join(BaseTable):
    def __init__(self, lhs, rhs, join_type='INNER', on=None, alias=None):
        super(Join, self).__init__(alias=alias)
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
                ctx.literal(' ').sql(EnclosedNodeList(self._columns))
            ctx.literal(' AS (')
            with ctx.scope_normal():
                ctx.sql(self._query)
            ctx.literal(')')
        return ctx


class ColumnBase(Node):
    def alias(self, name):
        if name:
            return Alias(self, name)
        return self

    def unalias(self):
        return self

    def asc(self):
        return Asc(self)
    __pos__ = asc

    def desc(self):
        return Desc(self)
    __neg__ = desc

    def __invert__(self):
        return Negated(self)

    def __e__(op, inv=False):
        def inner(self, rhs):
            if inv:
                return Expression(rhs, op, self)
            return Expression(self, op, rhs)
        return inner
    __and__ = __e__('AND')
    __or__ = __e__('OR')

    def __add__(self, other):
        if isinstance(other, basestring):
            return self.concat(other)
        return Expression(self, '+', other)
    def __radd__(self, lhs):
        if isinstance(lhs, basestring):
            return Expression(lhs, '||', self)
        return Expression(lhs, '+', self)
    __sub__ = __e__('-')
    __mul__ = __e__('*')
    __div__ = __truediv__ = __e__('/')
    __rand__ = __e__('AND', True)
    __ror__ = __e__('OR', True)
    __rsub__ = __e__('-', True)
    __rmul__ = __e__('*', True)
    __rdiv__ = __e__('/', True)

    def __eq__(self, other):
        return Expression(self, 'IS' if other is None else '=', other)
    def __ne__(self, other):
        return Expression(self, 'IS NOT' if other is None else '!=', other)
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
    bin_and = __e__('&')
    bin_or = __e__('|')
    def is_null(self, is_null=True):
        return Expression(self, ('IS' if is_null else 'IS NOT'), None)
    def between(self, lo, hi):
        return Expression(self, 'BETWEEN', Expression(lo, 'AND', hi, True))

    def contains(self, rhs):
        return Expression(self, 'LIKE', '%%%s%%' % rhs)
    def startswith(self, rhs):
        return Expression(self, 'LIKE', '%s%%' % rhs)
    def endswith(self, rhs):
        return Expression(self, 'LIKE', '%%%s' % rhs)
    def regexp(self, expression):
        return Expression(self, 'REGEXP', expression)
    def concat(self, rhs):
        return StringExpression(self, '||', rhs)
    def match(self, rhs):
        return Expression(self, 'MATCH', rhs)


class Column(ColumnBase):
    def __init__(self, source, name):
        self.source = source
        self.name = name

    def __hash__(self):
        return hash((self.source, self.name))

    def __sql__(self, Context ctx):
        if ctx.scope == SCOPE_VALUES:
            return ctx.sql(Entity(self.name))
        else:
            with ctx.scope_column():
                return ctx.sql(self.source).literal('.').sql(Entity(self.name))


class WrappedNode(ColumnBase):
    def __init__(self, node):
        self.node = node

    def unwrap(self):
        return self.node.unwrap()


class Alias(WrappedNode):
    def __init__(self, node, alias):
        super(Alias, self).__init__(node)
        self._alias = alias

    def alias(self, alias=None):
        if alias is None:
            return self.node
        return Alias(self.node, alias)

    def unalias(self):
        return self.node

    def __sql__(self, Context ctx):
        return ctx.sql(self.node).literal(' AS ').sql(Entity(self._alias))


class Negated(WrappedNode):
    def __invert__(self):
        return self.node

    def __sql__(self, Context ctx):
        return ctx.literal(' NOT ').sql(self.node)


class Value(ColumnBase):
    def __init__(self, value, unpack=True):
        self.value = value
        self.multi = isinstance(self.value, (list, tuple)) and unpack
        if self.multi:
            self.values = []
            for item in self.value:
                if isinstance(item, Node):
                    self.values.append(item)
                else:
                    self.values.append(Value(item))

    def __sql__(self, Context ctx):
        if self.multi:
            return ctx.sql(EnclosedNodeList(self.values))
        else:
            return ctx.literal('?').value(self.value)


class Cast(WrappedNode):
    def __init__(self, node, cast):
        super(Cast, self).__init__(node)
        self.cast = cast

    def __sql__(self, Context ctx):
        return (ctx
                .literal('CAST(')
                .sql(self.node)
                .literal(' AS %s)' % self.cast))


class Ordering(WrappedNode):
    def __init__(self, node, direction, collation=None):
        super(Ordering, self).__init__(node)
        self.direction = direction
        self.collation = collation

    def collate(self, collation=None):
        return Ordering(self.node, self.direction, collation)

    def __sql__(self, Context ctx):
        ctx.sql(self.node).literal(' %s' % self.direction)
        if self.collation:
            ctx.literal(' COLLATE %s' % self.collation)
        return ctx


def Asc(entity, collation=None):
    return Ordering(entity, 'ASC', collation)


def Desc(entity, collation=None):
    return Ordering(entity, 'DESC', collation)


class Expression(ColumnBase):
    def __init__(self, lhs, op, rhs, flat=False):
        self.lhs = lhs
        self.op = op
        self.rhs = rhs
        self.flat = flat

    def __sql__(self, Context ctx):
        with ctx(parentheses=not self.flat):
            if self.op == 'IN' and not Context().sql(self.rhs).query()[0]:
                return ctx.literal('0 = 1')

            return (ctx
                    .sql(self.lhs)
                    .literal(' %s ' % self.op)
                    .sql(self.rhs))


class StringExpression(Expression):
    def __add__(self, rhs):
        return self.concat(rhs)
    def __radd__(self, lhs):
        return StringExpression(lhs, '||', self)


def quote(list path):
    cdef:
        int n = len(path)
        str part
        tuple quotes = ('"', '"')

    if n == 1:
        return path[0].join(quotes)
    elif n > 1:
        return '.'.join([part.join(quotes) for part in path])
    return ''


class Entity(ColumnBase):
    def __init__(self, *path):
        self._path = [part.replace('"', '""') for part in path if part]

    def __sql__(self, Context ctx):
        return ctx.literal(quote(self._path))


class SQL(ColumnBase):
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
                    ctx.value(param)
        return ctx


def Check(constraint):
    return SQL('CHECK (%s)' % constraint)


class Function(ColumnBase):
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
            if len(self.arguments) == 1 and isinstance(self.arguments[0],
                                                       SelectBase):
                wrapper = CommaNodeList
            else:
                wrapper = EnclosedNodeList
            ctx.sql(wrapper([
                (arg if isinstance(arg, Node) else Value(arg))
                for arg in self.arguments]))
        return ctx

fn = Function(None, None)


def Case(predicate, expression_tuples, default=None):
    clauses = [SQL('CASE')]
    if predicate is not None:
        clauses.append(predicate)
    for expr, value in expression_tuples:
        clauses.extend((SQL('WHEN'), expr, SQL('THEN'), value))
    if default is not None:
        clauses.extend((SQL('ELSE'), default))
    clauses.append(SQL('END'))
    return NodeList(clauses)


class NodeList(Node):
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


def CommaNodeList(nodes):
    return NodeList(nodes, ', ')


def EnclosedNodeList(nodes):
    return NodeList(nodes, ', ', True)


class Query(Node):
    commit = True

    def __init__(self, order_by=None, limit=None, offset=None, **kwargs):
        super(Query, self).__init__(**kwargs)
        self._order_by = order_by
        self._limit = limit
        self._offset = offset
        self._cte_list = None

    @Node.copy
    def with_cte(self, *cte_list):
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
            ctx.literal(' ORDER BY ').sql(CommaNodeList(self._order_by))
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
                 .sql(CommaNodeList(self._cte_list))
                 .literal(' '))
        return ctx

    def execute(self, database):
        raise NotImplementedError

    def sql(self):
        return Context().parse(self)


def __compound_select__(operation, inverted=False):
    def method(self, other):
        if inverted:
            self, other = other, self
        return CompoundSelect(self, operation, other)
    return method


class SelectBase(Source, Query):
    commit = False
    __add__ = __compound_select__('UNION ALL')
    __or__ = __compound_select__('UNION')
    __and__ = __compound_select__('INTERSECT')
    __sub__ = __compound_select__('EXCEPT')
    __radd__ = __compound_select__('UNION ALL', inverted=True)
    __ror__ = __compound_select__('UNION', inverted=True)
    __rand__ = __compound_select__('INTERSECT', inverted=True)
    __rsub__ = __compound_select__('EXCEPT', inverted=True)

    def __init__(self, *args, **kwargs):
        super(SelectBase, self).__init__(*args, **kwargs)
        self._dicts = False
        self._namedtuples = False
        self._objects = False
        self._query_name = 'sq'

    def cte(self, name, recursive=False, columns=None):
        return CTE(name, self, recursive, columns)

    @Node.copy
    def tuples(self):
        self._dicts = self._namedtuples = self._objects = False

    @Node.copy
    def dicts(self, as_dict=True):
        self._dicts = as_dict
        self._namedtuples = self._objects = False

    @Node.copy
    def namedtuples(self, as_namedtuples=True):
        self._namedtuples = as_namedtuples
        self._dicts = self._objects = False

    @Node.copy
    def objects(self, constructor=None):
        self._objects = constructor
        self._dicts = self._namedtuples = False

    def get_cursor_wrapper(self):
        if self._dicts:
            return DictCursorWrapper
        elif self._namedtuples:
            return NamedTupleCursorWrapper
        elif self._objects:
            return partial(ObjectCursorWrapper, constructor=self._objects)
        else:
            return CursorWrapper

    def execute(self, database):
        cursor_wrapper_cls = self.get_cursor_wrapper()
        return cursor_wrapper_cls(database.execute(self))

    def exists(self, database):
        clone = self.select(SQL('1'))
        clone._limit = 1
        clone._offset = None
        try:
            clone.execute(database)
        except DoesNotExist:
            return False
        else:
            return True

    def count(self, database, clear_limit=False):
        clone = self.order_by().alias('_wrapped')
        if clear_limit:
            clone._limit = clone._offset = None

        query = Select([clone], [fn.COUNT(SQL('1'))]).tuples()
        cursor = query.execute(database)
        return next(iter(cursor))[0]


class CompoundSelect(SelectBase):
    def __init__(self, lhs, op, rhs):
        super(CompoundSelect, self).__init__()
        self.lhs = lhs
        self.op = op
        self.rhs = rhs

    def __sql__(self, Context ctx):
        if ctx.scope == SCOPE_COLUMN:
            return self.apply_column(ctx)

        outer_parens = ctx.subquery or (ctx.scope == SCOPE_SOURCE)
        with ctx(parentheses=outer_parens):
            with ctx.scope_normal(parentheses=False, subquery=False):
                ctx.sql(self.lhs)
            ctx.literal(' %s ' % self.op)
            with ctx.scope_normal(parentheses=False, subquery=False):
                ctx.sql(self.rhs)
        ctx = self.apply_ordering(ctx)
        return self.apply_alias(ctx)


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

    @Node.copy
    def select(self, *columns):
        self._columns = columns

    @Node.copy
    def from_(self, *sources):
        self._from = list(sources)

    @Node.copy
    def join(self, dest, join_type='INNER', on=None):
        if not self._from:
            raise ValueError('No sources to join on.')
        item = self._from.pop()
        self._from.append(Join(item, dest, join_type, on))

    def inner_join(self, dest, on=None):
        return self.join(dest, 'INNER', on)

    def left_outer_join(self, dest, on=None):
        return self.join(dest, 'LEFT OUTER', on)

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

    def __sql__(self, Context ctx):
        super(Select, self).__sql__(ctx)
        if ctx.scope == SCOPE_COLUMN:
            return self.apply_column(ctx)

        is_subquery = ctx.subquery
        parentheses = is_subquery or (ctx.scope == SCOPE_SOURCE)
        with ctx.scope_normal(parentheses=parentheses, subquery=True):
            ctx.literal('SELECT DISTINCT ' if self._distinct else 'SELECT ')
            with ctx.scope_source():
                ctx.sql(CommaNodeList(self._columns))

            if self._from:
                with ctx.scope_source(parentheses=False):
                    ctx.literal(' FROM ').sql(CommaNodeList(self._from))

            if self._where is not None:
                ctx.literal(' WHERE ').sql(self._where)

            if self._group_by:
                ctx.literal(' GROUP BY ').sql(CommaNodeList(self._group_by))

            if self._having is not None:
                ctx.literal(' HAVING ').sql(self._having)

            self.apply_ordering(ctx)

        return self.apply_alias(ctx)


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
        super(Update, self).__sql__(ctx)

        with ctx.scope_values(subquery=True):
            ctx.literal('UPDATE ')
            if self._on_conflict:
                ctx.literal('OR %s ' % self._on_conflict)

            (ctx
             .sql(self.table)
             .literal(' SET ')
             .sql(CommaNodeList([
                 NodeList((key, SQL('='), value))
                 for key, value in self.data.items()])))

            if self._where is not None:
                ctx.literal(' WHERE ').sql(self._where)

            return self.apply_ordering(ctx)

    def execute(self, Database database):
        cursor = database.execute(self)
        return cursor.rowcount


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
        for key in sorted(self.data, key=operator.attrgetter('name')):
            value = self.data[key]
            columns.append(key)
            if not isinstance(value, Node):
                value = Value(value)
            values.append(value)
        return (ctx
                .sql(EnclosedNodeList(columns))
                .literal(' VALUES ')
                .sql(EnclosedNodeList(values)))

    def _multi_insert(self, Context ctx):
        rows_iter = iter(self.data)
        columns = self._columns
        if not columns:
            try:
                row = next(rows_iter)
            except StopIteration:
                return ctx.sql('DEFAULT VALUES')
            columns = [c if isinstance(c, Node) else self.table._get_column(c)
                       for c in row]
            rows_iter = itertools.chain(iter((row,)), rows_iter)

        ctx.sql(EnclosedNodeList(columns)).literal(' VALUES ')
        all_values = []
        ncols = range(len(columns))

        for row in rows_iter:
            values = []
            if isinstance(row, dict):
                indexes = columns
            else:
                indexes = ncols

            for index in indexes:
                value = row[index]
                if not isinstance(value, Node):
                    value = Value(value)
                values.append(value)

            all_values.append(EnclosedNodeList(values))

        return ctx.sql(CommaNodeList(all_values))

    def _query_insert(self, Context ctx):
        return (ctx
                .sql(EnclosedNodeList(self._columns))
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
            elif self.data is None:
                ctx.literal('DEFAULT VALUES')
            else:
                ctx = self._multi_insert(ctx)
        return ctx

    def execute(self, Database database):
        cursor = database.execute(self)
        return cursor.lastrowid


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
        cursor = database.execute(self)
        return cursor.rowcount


class _BoundQuery(object):
    def __init__(self, database, *args, **kwargs):
        self._database = database
        super(_BoundQuery, self).__init__(*args, **kwargs)

    def execute(self):
        return super(_BoundQuery, self).execute(self._database)


class BoundSelect(_BoundQuery, Select):
    def __init__(self, *args, **kwargs):
        super(BoundSelect, self).__init__(*args, **kwargs)
        self._cursor = None

    def clone(self):
        clone = super(BoundSelect, self).clone()
        clone._cursor = None
        return clone

    def execute(self):
        if self._cursor is None:
            self._cursor = super(BoundSelect, self).execute()
        return self._cursor

    def __iter__(self):
        return iter(self.execute())

    def __getitem__(self, item):
        return self.execute()[item]

    def first(self):
        return self.execute().first()

    def get(self):
        return self.execute().get()

    def scalar(self):
        return self.execute().scalar()


class BoundUpdate(_BoundQuery, Update): pass
class BoundInsert(_BoundQuery, Insert): pass
class BoundDelete(_BoundQuery, Delete): pass


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
    cdef unicode date_str = decode(date_value)
    for date_format in SQLITE_DATETIME_FORMATS:
        try:
            return datetime.datetime.strptime(date_str, date_format)
        except ValueError:
            pass


cpdef format_date(date_value):
    cdef:
        datetime.datetime date_obj
        unicode date_str = decode(date_value)
    for date_format in SQLITE_DATE_FORMATS:
        try:
            date_obj = datetime.datetime.strptime(date_str, date_format)
        except ValueError:
            pass
        else:
            return date_obj.date()


cpdef format_time(time_value):
    cdef:
        datetime.datetime date_obj
        unicode date_str = decode(time_value)
    for date_format in SQLITE_TIME_FORMATS:
        try:
            date_obj = datetime.datetime.strptime(date_str, date_format)
        except ValueError:
            pass
        else:
            return date_obj.time()


def backup(src_conn, dest_conn):
    cdef:
        pysqlite_Connection *src = <pysqlite_Connection *>src_conn
        pysqlite_Connection *dest = <pysqlite_Connection *>dest_conn
        sqlite3 *src_db = src.db
        sqlite3 *dest_db = dest.db
        sqlite3_backup *backup

    backup = sqlite3_backup_init(dest_db, 'main', src_db, 'main')
    if (backup == NULL):
        raise OperationalError('Unable to initialize backup.')

    sqlite3_backup_step(backup, -1)
    sqlite3_backup_finish(backup)
    if sqlite3_errcode(dest_db):
        raise OperationalError('Error finishing backup: %s' %
                               sqlite3_errmsg(dest_db))
    return True


def backup_to_file(src_conn, filename):
    dest_conn = pysqlite.connect(filename)
    backup(src_conn, dest_conn)
    dest_conn.close()
    return True


pysqlite.register_adapter(decimal.Decimal, str)
pysqlite.register_adapter(datetime.date, str)
pysqlite.register_adapter(datetime.time, str)
pysqlite.register_converter('DATETIME', format_datetime)
pysqlite.register_converter('DATE', format_date)
pysqlite.register_converter('TIME', format_time)
pysqlite.register_converter('DECIMAL', decimal.Decimal)
