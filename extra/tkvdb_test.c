#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include "tkvdb.h"

#include "cutest.h"

void
test_open_db(void)
{
	const char fn[] = "data_test.tkv";
	tkvdb *db;

	db = tkvdb_open(fn, NULL);
	TEST_CHECK(db != NULL);
	tkvdb_close(db);

	unlink(fn);
}

void
test_open_incorrect_db(void)
{
	FILE *f;
	tkvdb *db;
	const char fn[] = "bogus_test.tkv";

	f = fopen(fn, "w");
	fprintf(f, "incorrect header");
	fclose(f);

	db = tkvdb_open(fn, NULL);
	TEST_CHECK(db == NULL);
	tkvdb_close(db);

	unlink(fn);
}

void
test_create_db(void)
{
	const char fn[] = "data_test.tkv";
	tkvdb *db;
	tkvdb_tr *tr;
	TKVDB_RES r;

	db = tkvdb_open(fn, NULL);
	TEST_CHECK(db != NULL);
	tr = tkvdb_tr_create(db);
	TEST_CHECK(tr != NULL);

	r = tkvdb_begin(tr);
	TEST_CHECK(r == TKVDB_OK);

	/* fill database with some data */
	tkvdb_put(tr, "123", 3, "3", 1);
	tkvdb_put(tr, "12345", 5, "5", 1);
	tkvdb_put(tr, "12344", 5, "4", 1);

	r = tkvdb_commit(tr);
	TEST_CHECK(r == TKVDB_OK);

	/* one more transaction */
	r = tkvdb_begin(tr);
	TEST_CHECK(r == TKVDB_OK);

	r = tkvdb_put(tr, "321", 3, "1", 1);
	TEST_CHECK(r == TKVDB_OK);

	r = tkvdb_commit(tr);
	TEST_CHECK(r == TKVDB_OK);

	tkvdb_tr_free(tr);
	tkvdb_close(db);
}

TEST_LIST = {
	{ "open db", test_open_db },
	{ "open incorrect db file", test_open_incorrect_db },
	{ "create db", test_create_db },
	{ 0 }
};

