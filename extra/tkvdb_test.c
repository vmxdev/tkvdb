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

struct kv
{
	char *key;
	char *val;
} kvs[] = {
	{"123", "0"},
	{"12345", "1"},
	{"12344", "2"},
	{"321", "3"},
	{"43210", "4"},
	{"0123456789", "5"}
};

void
test_fill_db(void)
{
	const char fn[] = "data_test.tkv";
	tkvdb *db;
	tkvdb_tr *tr;

	db = tkvdb_open(fn, NULL);
	TEST_CHECK(db != NULL);
	tr = tkvdb_tr_create(db);
	TEST_CHECK(tr != NULL);

	TEST_CHECK(tkvdb_begin(tr) == TKVDB_OK);

	/* fill database with some data */

#define PUT(I) TEST_CHECK(tkvdb_put(tr, kvs[I].key, strlen(kvs[I].key), \
		kvs[I].val, strlen(kvs[I].val)) == TKVDB_OK)

	PUT(0);
	PUT(1);
	PUT(2);

	TEST_CHECK(tkvdb_commit(tr) == TKVDB_OK);

	/* one more transaction */
	TEST_CHECK(tkvdb_begin(tr) == TKVDB_OK);

	PUT(3);
	PUT(4);
	PUT(5);

#undef PUT

	TEST_CHECK(tkvdb_commit(tr) == TKVDB_OK);

	tkvdb_tr_free(tr);
	tkvdb_close(db);
}

void
test_iter(void)
{
	const char fn[] = "data_test.tkv";
	tkvdb *db;
	tkvdb_tr *tr;
	tkvdb_cursor *c;

	db = tkvdb_open(fn, NULL);
	TEST_CHECK(db != NULL);
	tr = tkvdb_tr_create(db);
	TEST_CHECK(tr != NULL);

	TEST_CHECK(tkvdb_begin(tr) == TKVDB_OK);

	c = tkvdb_cursor_create(tr);
	TEST_CHECK(c != NULL);

	/* check iteration */
	TEST_CHECK(tkvdb_first(c) == TKVDB_OK);
	TEST_CHECK(memcmp(tkvdb_cursor_key(c), kvs[5].key,
		tkvdb_cursor_keysize(c)) == 0);

#define EXPECT_NEXT(N)\
	TEST_CHECK(tkvdb_next(c) == TKVDB_OK);\
	TEST_CHECK(memcmp(tkvdb_cursor_key(c), kvs[N].key,\
		tkvdb_cursor_keysize(c)) == 0)

#define EXPECT_PREV(N)\
	TEST_CHECK(tkvdb_prev(c) == TKVDB_OK);\
	TEST_CHECK(memcmp(tkvdb_cursor_key(c), kvs[N].key,\
		tkvdb_cursor_keysize(c)) == 0)

	EXPECT_NEXT(0);
	EXPECT_NEXT(2);
	EXPECT_NEXT(1);
	EXPECT_NEXT(3);
	EXPECT_NEXT(4);

	EXPECT_PREV(3);
	EXPECT_PREV(1);
	EXPECT_PREV(2);
	EXPECT_PREV(0);
	EXPECT_PREV(5);

#undef EXPECT_PREV
#undef EXPECT_NEXT

	TEST_CHECK(tkvdb_prev(c) == TKVDB_EMPTY);

	tkvdb_cursor_free(c);
	TEST_CHECK(tkvdb_rollback(tr) == TKVDB_OK);

	tkvdb_tr_free(tr);
	tkvdb_close(db);
}

TEST_LIST = {
	{ "open db", test_open_db },
	{ "open incorrect db file", test_open_incorrect_db },
	{ "fill db", test_fill_db },
	{ "iterate db", test_iter },
	{ 0 }
};

