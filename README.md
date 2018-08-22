# tkvdb
Trie (radix trie in fact) key-value database

`tkvdb` is an embedded database library for key-value data. It is similar to Berkeley DB, LevelDB or SQLite4 LSM.

Keys are always sorted in memcmp() order.

## Supported operations

  * Add a new key/value pair to the database.
  * Delete an existing key from the database.
  * Querying the database for a specific key.
  * Iterating through a range of database keys (either forwards or backwards).

## Portability

`tkvdb` is written in ANSI C, without using platform or OS-specific functions.

It uses traditional `open/seek/read/write/close` API for operations with data files, memory allocation (`malloc/realloc/free`) and some string functions (`memset/memcpy`) for dealing with in-memory transactions.

There is no limitations for 32-bit CPU's, except for size of memory buffers.

`tkvdb` was tested on Linux(x32/x64 CPU's and 32 bit ARM) and under Wine using mingw (hopefully it will work under Windows).

## Basic usage

API is quite a simple, but requires some explanation.

tkvdb database file (in simplified append-only form) looks like:

![tkvdb database layout](docs/nonvac_db.png?raw=true "tkvdb database layout")

A database file is a set of blocks - "Transactions" (probably not the best name for it).
In each block, there is a footer with a pointer to a current root node, signature and some additional DB file information.
Each transaction is a small subtree (radix tree) which contains nodes of the database that was changed in this transaction.
Tree node may contain pointers (offsets in the file) to other, unchanged nodes from previous transactions. These pointers remain unchanged in a new subtree.

So, to modify the database you need to open it, create a transaction, make changes and "commit" (or "rollback").
However, you may create transaction without the underlying database file. In this case, `tkvdb` will act as the RAM-only database.
Commits and rollbacks will just drop all the data and reset transaction to initial state.

Here is a simple example:

```
tkvdb_datum key, value;

db = tkvdb_open("db.tkvdb", NULL);           /* optional, only if you need to keep data on disk */

transaction = tkvdb_tr_create(db, NULL);     /* pass NULL instead of db for RAM-only db */

transaction->begin(transaction);             /* start */
transaction->put(transaction, &key, &val);   /* add key-value pair or overwrite existing */
transaction->commit(transaction);            /* commit */

/* you may reuse transaction later */
transaction->begin(transaction);             /* start new transaction */
transaction->get(transaction, &key, &val);   /* get key-value pair */
transaction->del(transaction, &key, 1);      /* delete all keys starting with key.data */
transaction->rollback(transaction);          /* dismiss */

transaction->free(transaction);

tkvdb_close(db);                             /* close on-disk database */
```

## Searching in database and cursors

Use `transaction->get()` if you need to get a value by key.
On success, it returns `TKVDB_OK` and pointer to data in memory and length.
You can modify the value "in place" if a length is not changed.

If you need to iterate through the database (or through a part of the database) you may use cursors.

```
TKVDB_RES rc;

tkvdb_cursor *cursor = tkvdb_cursor_create(transaction);

rc = cursor->first(cursor);                  /* position cursor to the first key-value pair of database */
while (rc == TKVDB_OK) {
	key = cursor->key(cursor);           /* get pointer to key */
	keysize = cursor->keysize(cursor);   /* and size of a key */
	val = cursor->val(cursor);           /* pointer to value */
	valsize = cursor->valsize(cursor);   /* and size of value */

	rc = cursor->next(cursor);           /* jump to next key-value pair */
}

cursor->free(cursor);
```

To iterate in reverse order use `cursor->last()` and `cursor->prev()`.

If you want to search a key-value pair in database by prefix use `cursor->seek(cursor, &key, TKVDB_SEEK)`
where `TKVDB_SEEK` can be:
  * `TKVDB_SEEK_EQ` : search for the exact key match
  * `TKVDB_SEEK_LE` : search for less (in terms of memcpy()) or equal key
  * `TKVDB_SEEK_GE` : search for greater (in terms of memcpy()) or equal key

After seeking to key-value pair you can use `cursor->next()` or `cursor->prev()`

## Database and transaction parameters

You can tune some database or transaction parameters. Transaction parameters are inherited from database, but can be overridden.
Here is an example:

```
tkvdb_params *params;

params = tkvdb_params_create();
tkvdb_param_set(params, TKVDB_PARAM_TR_DYNALLOC, 0);      /* don't use dynamic nodes allocation */
tkvdb_param_set(params, TKVDB_PARAM_TR_LIMIT, 1024*1024); /* memory block of 1M will be used for transaction */

db = tkvdb_open("db.tkvdb", params);
transaction1 = tkvdb_tr_create(db, NULL);                  /* transactions of parent db will use theese parameters */

/* and you can override parameters for some transactions with different values */
tkvdb_param_set(params, TKVDB_PARAM_TR_LIMIT, 1024*1024*10);
transaction2 = tkvdb_tr_create(db, params);

/* or use with RAM-only transaction */
transaction3 = tkvdb_tr_create(NULL, params);

tkvdb_params_free(params);

```

## Multithreading

`tkvdb` does not use any OS-dependent synchronization mechanisms.
You must explicitly lock transaction update operations.

However, on some CPU's (at least on x32/x64) we can guarantee that `transaction->put()` will never put the in-memory transaction in inconsistent state.
Updates of tree are lock-free and atomic.
You can use one writer and multiple readers without locks.
But be careful with `transaction->rollback()` and `transaction->commit()` - there is no such guarantees for theese functions, reading from transaction while resetting it can lead to unpredicatable consequences.

## Bugs and caveats (sort of TODO)

  * There is still no `vacuum` routine for database file. We have initial and bogus implementation, but it's not tested, so the database now is append-only.
  * Cursor operations (seeks and traversal) has fixed limit for tree depth. Limit is defined in `tkvdb.c` (`TKVDB_STACK_MAX_DEPTH 128`), you can increase it and recompile if needed.
  * Cursors operations are slow (compared with `get()` or even `put()` with tkvdb builtin allocator). There is a call to `realoc()` on each node hit. Probably it will be fixed.
  * There is no easy way to get N-th record of database. However, it's possible to implement such seeks using some nodes metadata.
  * In RAM-only mode (without underlying database file) non-leaf nodes still contains array of offsets in file (2Kbytes per node). This is definitely not needed.
  * There is no publicly available benchmarks and nice performance charts. You can run `perf_test` from `extra` directory, it will show ops(inserts/updates and lookups) per second for 4 and 16 byte keys with different number of keys in transaction. Test is single-threaded and shows RAM-only operations. Depending on hardware you may get up to tens of millions ops per second (or even more than 100 millions lookups per second for short keys). Probably we will make more accurate, complete and readable performance tests.

## Compiling and running tests

Unit test:
```sh
$ cc -g -Wall -pedantic -Wextra -I. extra/tkvdb_test.c tkvdb.c -o tkvdb_test
$ ./tkvdb_test
```

Simple performance test:
```sh
$ cc -O3 -Wall -pedantic -Wextra -I. extra/perf_test.c tkvdb.c -o perf_test
$ ./perf_test
```
