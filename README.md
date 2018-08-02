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

Use `cursor->get()` if you need to get a value by key.
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


## Multithreading

`tkvdb` does not use any OS-dependent synchronization mechanisms.
You must explicitly lock transaction update operations.

However, on some CPU's (at least on x32/x64) we can guarantee that `transaction->put()` will never put the in-memory transaction in inconsistent state.
Updates of tree are lock-free and atomic.
You can use one writer and multiple readers without locks.
But be careful with `transaction->rollback()` and `transaction->commit()` - there is no such guarantees for theese functions, reading from transaction while resetting it can lead to unpredicatable consequences.

## Compiling and running test

```sh
$ cc -Wall -pedantic -Wextra -I. extra/tkvdb_test.c tkvdb.c -o tkvdb_test
$ ./tkvdb_test
```
