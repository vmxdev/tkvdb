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

```
db = tkvdb_open(db_name, params);
/* ... */
tkvdb_close(db);
```

## Compiling and running test

```sh
$ cc -Wall -pedantic -Wextra -I. extra/tkvdb_test.c tkvdb.c -o tkvdb_test
$ ./tkvdb_test
```
