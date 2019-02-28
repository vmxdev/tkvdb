# Utilities

## tkvdb-dump

Dump contents of database into file. File is set of "key":"value" pairs,  one line per pair. Some characters ('\n', '\"', '\\') are escaped.

### Compiling and using:

```sh
$ cd utils
$ cc -g -Wall -Wextra -pedantic -I.. tkvdb_dump.c ../tkvdb.c -o tkvdb-dump
```

Dump to `dump.txt` using 1M transaction buffer:

```sh
$ ./tkvdb-dump -s 1000000 -o dump.txt database.tkv
```


## tkvdb-restore

Restore database from dump. Note it doesn't clean contents of existing database before inserting data.

### Compiling and using

```sh
$ cd utils
$ cc -g -Wall -Wextra -pedantic -I.. tkvdb_restore.c ../tkvdb.c -o tkvdb-restore
```

```sh
$ ./tkvdb-restore -i dump.txt new_database.tkv
```
