# tkvdb
Trie (radix trie in fact) key-value database

`tkvdb` is an embedded database library for key-value data. It is similar to Berkeley DB, LevelDB or SQLite4 LSM.

Keys are always sorted in memcmp() order.

## Supported operations

  * Add a new key/value pair to the database.
  * Delete an existing key from the database.
  * Querying the database for a specific key.
  * Iterating through a range of database keys (either forwards or backwards).

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

db = tkvdb_open("db.tkvdb", NULL);    /* optional, only if you need to keep data on disk */

transaction = tkvdb_tr_create(db);    /* pass NULL here for RAM-only db */

tkvdb_begin(transaction);             /* start */
tkvdb_put(transaction, &key, &val);   /* add key-value pair or overwrite existing */
tkvdb_commit(transaction);            /* commit */

/* you may reuse transaction later */
tkvdb_begin(transaction);             /* start new transaction */
tkvdb_get(transaction, &key, &val);   /* get key-value pair */
tkvdb_del(transaction, &key, 1);      /* delete all keys starting with key.data */
tkvdb_rollback(transaction);          /* dismiss */

tkvdb_tr_free(transaction);

tkvdb_close(db);                      /* close on-disk database */
```

## Searching in database and cursors

Use `tkvdb_get()` if you need to get a value by key.
On success, it returns `TKVDB_OK` and pointer to data in memory and length.
You can modify the value "in place" if a length is not changed.

If you need to iterate through the database (or through a part of the database) you may use cursors.

```
TKVDB_RES rc;

tkvdb_cursor *cursor = tkvdb_cursor_create(transaction);

rc = tkvdb_first(cursor);                         /* position cursor to the first key-value pair of database */
while (rc == TKVDB_OK) {
	key = tkvdb_cursor_key(cursor);           /* get pointer to key */
	keysize = tkvdb_cursor_keysize(cursor);   /* and size of a key */
	val = tkvdb_cursor_val(cursor);           /* pointer to value */
	valsize = tkvdb_cursor_valsize(cursor);   /* and size of value */

	rc = tkvdb_next(cursor);                  /* jump to next key-value pair */
}

tkvdb_cursor_free(cursor);
```

To iterate in reverse order use `tkvdb_last()` and `tkvdb_prev()`.

If you want to search a key-value pair in database by prefix use `tkvdb_seek(cursor, &key, TKVDB_SEEK)`
where `TKVDB_SEEK` can be:
  * `TKVDB_SEEK_EQ` : search for the exact key match
  * `TKVDB_SEEK_LE` : search for less (in terms of memcpy()) or equal key
  * `TKVDB_SEEK_GE` : search for greater (in terms of memcpy()) or equal key

After seeking to key-value pair you can still use `tkvdb_next()` or `tkvdb_prev()`


## Manual memory control

By default nodes in transactions allocated when needed using system malloc().
You may pre-allocate memory block for transaction using `tkvdb_tr_create_m(db, BLOCKSIZE_IN_BYTES, 0)`
In this case allocations of nodes in the tree becomes faster, but the size of the transaction becomes limited to a fixed value.
Functions will return `TKVDB_ENOMEM` if you have reached a limit.

## Multithreading

`tkvdb` does not use any OS-dependent synchronization mechanisms.
You must explicitly lock transaction update operations.

However, on some CPU's (at least on x32/x64) we can guarantee that `tkvdb_put()` will never put the in-memory transaction in inconsistent state.
Updates of tree are lock-free and atomic.
You can use one writer and multiple readers without locks.
But be careful with `tkvdb_rollback()` and `tkvdb_commit()` - there is no such guarantees for theese functions, reading from transaction while resetting it can lead to unpredicatable consequences.

## Compiling and running test

```sh
$ cc -Wall -pedantic -Wextra -I. extra/tkvdb_test.c tkvdb.c -o tkvdb_test
$ ./tkvdb_test
```
