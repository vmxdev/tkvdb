#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <search.h>

#include <ctype.h>
#include <time.h>

#include "tkvdb.h"

static void test_init();
#define CUTEST_INIT__ test_init()
#include "cutest.h"

#define KLEN 100
#define VLEN 100
#define N 2000
#define TR_SIZE 10

struct kv
{
	char key[KLEN];
	char val[VLEN];

	size_t klen, vlen;
} kvs[N], kvs_unsorted[N];

static int
keycmp(const void *m1, const void *m2)
{
	size_t minlen;
	const struct kv *a = m1;
	const struct kv *b = m2;
	int r;

	minlen = (a->klen < b->klen) ? a->klen : b->klen;
	r = memcmp(a->key, b->key, minlen);
	if (r) {
		return r;
	}
	return (a->klen - b->klen);
}


static void
gen_rand()
{
	size_t i, j;

	for (i=0; i<N; i++) {
		struct kv datum;

		for (;;) {
			size_t nmemb = i;
			datum.klen = rand() % (KLEN - 1) + 1;
			for (j=0; j<datum.klen; j++) {
				datum.key[j] = rand();
			}
			if (lfind(&datum, &kvs_unsorted, &nmemb, sizeof(struct kv), &keycmp) == NULL) {
				break;
			}
		}

		datum.vlen = rand() % (VLEN - 1) + 1;
		for (j=0; j<datum.vlen; j++) {
			datum.val[j] = rand();
		}
		kvs_unsorted[i] = datum;
	}

	memcpy(&kvs, &kvs_unsorted, sizeof(struct kv) * N);
	/* sort generated data */
	qsort(&kvs, N, sizeof(struct kv), &keycmp);
}

static void
test_init()
{
	gen_rand();
}

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
test_fill_db(void)
{
	const char fn[] = "data_test.tkv";
	tkvdb *db;
	tkvdb_tr *tr;
	size_t i, j;

	db = tkvdb_open(fn, NULL);
	TEST_CHECK(db != NULL);
	tr = tkvdb_tr_create(db);
	TEST_CHECK(tr != NULL);

	/* fill database */
	for (i=0; i<N/TR_SIZE; i++) {
		TEST_CHECK(tkvdb_begin(tr) == TKVDB_OK);

		for (j=0; j<TR_SIZE; j++) {
			TEST_CHECK(tkvdb_put(tr, kvs_unsorted[i * TR_SIZE + j].key,
				kvs_unsorted[i * TR_SIZE + j].klen,
				kvs_unsorted[i * TR_SIZE + j].val,
				kvs_unsorted[i * TR_SIZE + j].vlen) == TKVDB_OK);
		}

		TEST_CHECK(tkvdb_commit(tr) == TKVDB_OK);
	}

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
	size_t i;
	int r;

	db = tkvdb_open(fn, NULL);
	TEST_CHECK(db != NULL);
	tr = tkvdb_tr_create(db);
	TEST_CHECK(tr != NULL);

	TEST_CHECK(tkvdb_begin(tr) == TKVDB_OK);

	c = tkvdb_cursor_create(tr);
	TEST_CHECK(c != NULL);

	/* iterate forward */
	TEST_CHECK(tkvdb_first(c) == TKVDB_OK);
	i = 0;
	do {
		TEST_CHECK(memcmp(tkvdb_cursor_key(c), kvs[i].key,
			tkvdb_cursor_keysize(c)) == 0);
		i++;
	} while ((r = tkvdb_next(c)) == TKVDB_OK);

	TEST_CHECK(i == N);

	/* backward */
	TEST_CHECK(tkvdb_last(c) == TKVDB_OK);
	i = 0;
	do {
		TEST_CHECK(memcmp(tkvdb_cursor_key(c), kvs[N - i - 1].key,
			tkvdb_cursor_keysize(c)) == 0);
		i++;
	} while (tkvdb_prev(c) == TKVDB_OK);

	TEST_CHECK(i == N);

	tkvdb_cursor_free(c);
	TEST_CHECK(tkvdb_rollback(tr) == TKVDB_OK);

	tkvdb_tr_free(tr);
	tkvdb_close(db);
}

void
test_seek(void)
{
	const char fn[] = "data_test.tkv";
	tkvdb *db;
	tkvdb_tr *tr;
	tkvdb_cursor *c;
	size_t i;
	const size_t NITER = 1000;

	db = tkvdb_open(fn, NULL);
	TEST_CHECK(db != NULL);
	tr = tkvdb_tr_create(db);
	TEST_CHECK(tr != NULL);

	TEST_CHECK(tkvdb_begin(tr) == TKVDB_OK);

	c = tkvdb_cursor_create(tr);
	TEST_CHECK(c != NULL);

	/* existent keys */
	for (i=0; i<NITER; i++) {
		int idx;
		TKVDB_RES r;

		idx = rand() % N;

		r = tkvdb_seek(c, kvs[idx].key, kvs[idx].klen, TKVDB_SEEK_EQ);
		TEST_CHECK(r == TKVDB_OK);
	}

	/* nonexistent */
	for (i=0; i<NITER; i++) {
		TKVDB_RES r;
		size_t j;
		struct kv datum;

		do {
			datum.klen = rand() % (KLEN - 1) + 1;
			for (j=0; j<datum.klen; j++) {
				datum.key[j] = rand();
			}
		} while (bsearch(&datum, &kvs, N, sizeof(struct kv), &keycmp));

		r = tkvdb_seek(c, datum.key, datum.klen, TKVDB_SEEK_EQ);

		TEST_CHECK(r == TKVDB_NOT_FOUND);
	}

	/* less or equal */
	for (i=0; i<NITER; i++) {
		TKVDB_RES r;
		size_t j;
		struct kv search, dat_db;
		int kidx;

		search.klen = rand() % (KLEN - 1) + 1;
		for (j=0; j<search.klen; j++) {
			search.key[j] = rand();
		}

		/* search for less or equal key in memory */
		for (kidx=0; kidx<N; kidx++) {
			int cmpres = keycmp(&search, &kvs[kidx]);
			if (cmpres == 0) {
				break;
			}
			if (cmpres < 0) {
				kidx--;
				break;
			}
		}
		if (kidx == N) {
			kidx--;
		}

		r = tkvdb_seek(c, search.key, search.klen, TKVDB_SEEK_LE);
		if (kidx >= 0) {
			TEST_CHECK(r == TKVDB_OK);
			dat_db.klen = tkvdb_cursor_keysize(c);
			memcpy(dat_db.key, tkvdb_cursor_key(c), dat_db.klen);
			TEST_CHECK(keycmp(&dat_db, &kvs[kidx]) == 0);
		} else {
			TEST_CHECK(r == TKVDB_NOT_FOUND);
		}
	}

	/* greater or equal */
	for (i=0; i<NITER; i++) {
		TKVDB_RES r;
		size_t j;
		struct kv search, dat_db;
		int kidx;

		search.klen = rand() % (KLEN - 1) + 1;
		for (j=0; j<search.klen; j++) {
			search.key[j] = rand();
		}

		/* search for greater or equal key in memory */
		for (kidx=N; kidx-- > 0; ) {
			int cmpres = keycmp(&search, &kvs[kidx]);
			if (cmpres == 0) {
				break;
			}
			if (cmpres > 0) {
				kidx++;
				break;
			}
		}
		if (kidx < 0) {
			kidx++;
		}
		if (kidx == N) {
			kidx--;
		}

		r = tkvdb_seek(c, search.key, search.klen, TKVDB_SEEK_GE);
		if ((kidx >= 0) && (kidx < (N - 1))) {
			TEST_CHECK(r == TKVDB_OK);
			dat_db.klen = tkvdb_cursor_keysize(c);
			memcpy(dat_db.key, tkvdb_cursor_key(c), dat_db.klen);
			TEST_CHECK(keycmp(&dat_db, &kvs[kidx]) == 0);
		} else {
			TEST_CHECK(r == TKVDB_NOT_FOUND);
		}
	}

	tkvdb_cursor_free(c);
	TEST_CHECK(tkvdb_rollback(tr) == TKVDB_OK);

	tkvdb_tr_free(tr);
	tkvdb_close(db);
}

void
test_del(void)
{
	const char fn[] = "data_test.tkv";
	tkvdb *db;
	tkvdb_tr *tr;
	tkvdb_cursor *c;
	size_t i;
	int r;

	db = tkvdb_open(fn, NULL);
	TEST_CHECK(db != NULL);
	tr = tkvdb_tr_create(db);
	TEST_CHECK(tr != NULL);

	TEST_CHECK(tkvdb_begin(tr) == TKVDB_OK);

	for (i=0; i<N; i++) {
		if (i % 2) {
			r = tkvdb_del(tr, kvs[i].key, kvs[i].klen, 0);
			TEST_CHECK(r == TKVDB_OK);
		}
	}

	c = tkvdb_cursor_create(tr);
	TEST_CHECK(c != NULL);

	/* iterate forward */
	TEST_CHECK(tkvdb_first(c) == TKVDB_OK);
	i = 0;
	do {
		TEST_CHECK(memcmp(tkvdb_cursor_key(c), kvs[i].key,
			tkvdb_cursor_keysize(c)) == 0);
		TEST_CHECK(memcmp(tkvdb_cursor_val(c), kvs[i].val,
			tkvdb_cursor_valsize(c)) == 0);

		i += 2;
	} while ((r = tkvdb_next(c)) == TKVDB_OK);

	TEST_CHECK(i == N);

	tkvdb_cursor_free(c);
	TEST_CHECK(tkvdb_rollback(tr) == TKVDB_OK);

	tkvdb_tr_free(tr);
	tkvdb_close(db);
}

void
test_get(void)
{
	const char fn[] = "data_test.tkv";
	tkvdb *db;
	tkvdb_tr *tr;
	size_t i;
	const size_t NITER = 10000;
	char *val;
	size_t vlen;

	db = tkvdb_open(fn, NULL);
	TEST_CHECK(db != NULL);
	tr = tkvdb_tr_create(db);
	TEST_CHECK(tr != NULL);

	TEST_CHECK(tkvdb_begin(tr) == TKVDB_OK);

	/* existent keys */
	for (i=0; i<NITER; i++) {
		int idx;
		TKVDB_RES r;

		idx = rand() % N;

		r = tkvdb_get(tr, kvs[idx].key, kvs[idx].klen,
			(void **)&val, &vlen);
		TEST_CHECK(r == TKVDB_OK);
		TEST_CHECK(kvs[idx].vlen == vlen);

		TEST_CHECK(memcmp(val, kvs[idx].val, vlen) == 0);
	}

	/* nonexistent */
	for (i=0; i<NITER; i++) {
		TKVDB_RES r;
		size_t j;
		struct kv datum;

		do {
			datum.klen = rand() % (KLEN - 1) + 1;
			for (j=0; j<datum.klen; j++) {
				datum.key[j] = rand();
			}
		} while (bsearch(&datum, &kvs, N, sizeof(struct kv), &keycmp));

		r = tkvdb_get(tr, datum.key, datum.klen,
			(void **)&val, &vlen);

		TEST_CHECK(r == TKVDB_NOT_FOUND);
	}

	TEST_CHECK(tkvdb_rollback(tr) == TKVDB_OK);

	tkvdb_tr_free(tr);
	tkvdb_close(db);
}

void
test_vacuum(void)
{
	const char fn[] = "data_test_vac.tkv";
	tkvdb *db;
	tkvdb_tr *tr, *vac, *trres;
	tkvdb_cursor *c;
	size_t i, j;
	const size_t NI = 10, NJ = 10, NK = 20;
	uint64_t root_off, gap_begin, gap_end;

	/* fill database */
	db = tkvdb_open(fn, NULL);
	TEST_CHECK(db != NULL);
	tr = tkvdb_tr_create(db);
	TEST_CHECK(tr != NULL);

	for (i=0; i<NI; i++) {

		TEST_CHECK(tkvdb_begin(tr) == TKVDB_OK);

		for (j=0; j<NJ; j++) {
			char key[NK];
			size_t klen;

			sprintf(key, "%u-%03u", (unsigned int)i,
				rand() % 1000);
			klen = strlen(key) + 1;

			TEST_CHECK(tkvdb_put(tr, key, klen, key, klen)
				== TKVDB_OK);
		}
		TEST_CHECK(tkvdb_commit(tr) == TKVDB_OK);
	}

	/* vacuum */
	vac = tkvdb_tr_create(db);
	TEST_CHECK(vac != NULL);

	trres = tkvdb_tr_create(db);
	TEST_CHECK(trres != NULL);

	c = tkvdb_cursor_create(tr);
	TEST_CHECK(c != NULL);

	TEST_CHECK(tkvdb_vacuum(tr, vac, trres, c) == TKVDB_OK);

	/* get file info */
	TEST_CHECK(tkvdb_dbinfo(db, &root_off,
		&gap_begin, &gap_end) == TKVDB_OK);
	/*printf("%lu:%lu:%lu\n", root_off, gap_begin, gap_end);*/


	tkvdb_tr_free(tr);
	tkvdb_tr_free(vac);
	tkvdb_tr_free(trres);
	tkvdb_close(db);
}


TEST_LIST = {
	{ "open db", test_open_db },
	{ "open incorrect db file", test_open_incorrect_db },
	{ "fill db", test_fill_db },
	{ "first/last and next/prev", test_iter },
	{ "random seeks", test_seek },
	{ "get", test_get },
	{ "delete", test_del },
	{ "vacuum", test_vacuum },
	{ 0 }
};

