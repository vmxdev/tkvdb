# tkvdb
Trie (radix trie in fact) key-value database

`tkvdb` is an embedded database library for key-value data. It is similar to Berkeley DB, LevelDB or SQLite4 LSM.

Keys are always sorted in memcmp() order.

## Supported operations

  * Add a new key/value pair to the database.
  * Delete an existing key from the database. (NOT IMPLEMENTED YET)
  * Querying the database for a specific key. (NOT IMPLEMENTED YET)
  * Iterating through a range of database keys (either forwards or backwards).

## Basic usage

```
db = tkvdb_open(db_name, params);
/* ... */
tkvdb_close(db);
```
