#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <search.h>

#include <ctype.h>
#include <time.h>

#include "tkvdb.h"

static void test_init();
#define ACUTEST_INIT__ test_init()
#include "acutest.h"

#define KLEN 100
#define VLEN 100
#define N 20000
#define TR_SIZE 10

/* for alignment tests, must be power of two  */
#define VAL_ALIGNMENT 8

static int test_aligned = 0;

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
gen_rand(void)
{
	size_t i, j;
	unsigned int seed = 0;

	seed = time(NULL);
	srand(seed);

	fprintf(stderr, "Generating random data with seed %u ... ", seed);

	for (i=0; i<N; i++) {
		struct kv datum;

		memset(&datum, 0, sizeof(struct kv));

		for (;;) {
			size_t nmemb = i;

			memset(datum.key, 0, KLEN);

			datum.klen = rand() % (KLEN - 1) + 1;
			for (j=0; j<datum.klen; j++) {
				datum.key[j] = rand();
			}
			if (lfind(&datum, &kvs_unsorted, &nmemb,
				sizeof(struct kv), &keycmp) == NULL) {

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

	fprintf(stderr, "done\n");
}

static void
test_init(void)
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
	tkvdb_params *params;

	params = tkvdb_params_create();
	TEST_CHECK(params != NULL);
	if (test_aligned) {
		tkvdb_param_set(params, TKVDB_PARAM_ALIGNVAL, VAL_ALIGNMENT);
	}

	db = tkvdb_open(fn, params);
	TEST_CHECK(db != NULL);
	tkvdb_params_free(params);

	tr = tkvdb_tr_create(db, NULL);
	TEST_CHECK(tr != NULL);

	/* fill database */
	for (i=0; i<N/TR_SIZE; i++) {
		TEST_CHECK(tr->begin(tr) == TKVDB_OK);

		for (j=0; j<TR_SIZE; j++) {
			tkvdb_datum key, val;

			key.data = kvs_unsorted[i * TR_SIZE + j].key;
			key.size = kvs_unsorted[i * TR_SIZE + j].klen;
			val.data = kvs_unsorted[i * TR_SIZE + j].val;
			val.size = kvs_unsorted[i * TR_SIZE + j].vlen;
			TEST_CHECK(tr->put(tr, &key, &val) == TKVDB_OK);
		}

		TEST_CHECK(tr->commit(tr) == TKVDB_OK);
	}

	tr->free(tr);
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
	tkvdb_params *params;

	params = tkvdb_params_create();
	TEST_CHECK(params != NULL);
	if (test_aligned) {
		tkvdb_param_set(params, TKVDB_PARAM_ALIGNVAL, VAL_ALIGNMENT);
	}

	db = tkvdb_open(fn, params);
	TEST_CHECK(db != NULL);
	tkvdb_params_free(params);

	tr = tkvdb_tr_create(db, NULL);
	TEST_CHECK(tr != NULL);

	TEST_CHECK(tr->begin(tr) == TKVDB_OK);

	c = tkvdb_cursor_create(tr);
	TEST_CHECK(c != NULL);

	/* iterate forward */
	TEST_CHECK(c->first(c) == TKVDB_OK);
	i = 0;
	do {
		/* check key */
		TEST_CHECK(memcmp(c->key(c), kvs[i].key,
			c->keysize(c)) == 0);
		/* and value */
		TEST_CHECK(memcmp(c->val(c), kvs[i].val,
			c->valsize(c)) == 0);

		if (test_aligned) {
			TEST_CHECK((uintptr_t)c->val(c) % VAL_ALIGNMENT == 0);
		}

		i++;
	} while ((r = c->next(c)) == TKVDB_OK);

	TEST_CHECK(i == N);

	/* backward */
	TEST_CHECK(c->last(c) == TKVDB_OK);
	i = 0;
	do {
		TEST_CHECK(memcmp(c->key(c), kvs[N - i - 1].key,
			c->keysize(c)) == 0);
		TEST_CHECK(memcmp(c->val(c), kvs[N - i - 1].val,
			c->valsize(c)) == 0);

		if (test_aligned) {
			TEST_CHECK((uintptr_t)c->val(c) % VAL_ALIGNMENT == 0);
		}

		i++;
	} while (c->prev(c) == TKVDB_OK);

	TEST_CHECK(i == N);

	c->free(c);
	TEST_CHECK(tr->rollback(tr) == TKVDB_OK);

	tr->free(tr);
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
	const size_t NITER = 10000;
	tkvdb_datum dtk;
	tkvdb_params *params;

	params = tkvdb_params_create();
	TEST_CHECK(params != NULL);
	if (test_aligned) {
		tkvdb_param_set(params, TKVDB_PARAM_ALIGNVAL, VAL_ALIGNMENT);
	}

	db = tkvdb_open(fn, params);
	TEST_CHECK(db != NULL);
	tkvdb_params_free(params);

	tr = tkvdb_tr_create(db, NULL);
	TEST_CHECK(tr != NULL);

	TEST_CHECK(tr->begin(tr) == TKVDB_OK);

	c = tkvdb_cursor_create(tr);
	TEST_CHECK(c != NULL);

	/* existent keys */
	for (i=0; i<NITER; i++) {
		int idx;
		TKVDB_RES r;

		idx = rand() % N;

		dtk.data = kvs[idx].key;
		dtk.size = kvs[idx].klen;
		r = c->seek(c, &dtk, TKVDB_SEEK_EQ);
		TEST_CHECK(r == TKVDB_OK);

		TEST_CHECK(memcmp(c->key(c), kvs[idx].key,
			c->keysize(c)) == 0);
		TEST_CHECK(memcmp(c->val(c), kvs[idx].val,
			c->valsize(c)) == 0);
		if (test_aligned) {
			TEST_CHECK((uintptr_t)c->val(c) % VAL_ALIGNMENT == 0);
		}
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

		dtk.data = datum.key;
		dtk.size = datum.klen;
		r = c->seek(c, &dtk, TKVDB_SEEK_EQ);

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

		dtk.data = search.key;
		dtk.size = search.klen;
		r = c->seek(c, &dtk, TKVDB_SEEK_LE);
		if (kidx >= 0) {
			TEST_CHECK(r == TKVDB_OK);
			dat_db.klen = c->keysize(c);
			memcpy(dat_db.key, c->key(c), dat_db.klen);

			TEST_CHECK(keycmp(&dat_db, &kvs[kidx]) == 0);

			TEST_CHECK(memcmp(c->val(c), kvs[kidx].val,
				c->valsize(c)) == 0);

			if (test_aligned) {
				TEST_CHECK((uintptr_t)c->val(c) % VAL_ALIGNMENT == 0);
			}
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

		/* search for equal or greater key in memory */
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

		dtk.data = search.key;
		dtk.size = search.klen;
		r = c->seek(c, &dtk, TKVDB_SEEK_GE);
		if ((kidx >= 0) && (kidx < N)) {
			TEST_CHECK(r == TKVDB_OK);
			dat_db.klen = c->keysize(c);
			memcpy(dat_db.key, c->key(c), dat_db.klen);
			TEST_CHECK(keycmp(&dat_db, &kvs[kidx]) == 0);

			TEST_CHECK(memcmp(c->val(c), kvs[kidx].val,
				c->valsize(c)) == 0);

			if (test_aligned) {
				TEST_CHECK((uintptr_t)c->val(c) % VAL_ALIGNMENT == 0);
			}
		} else {
			TEST_CHECK(r == TKVDB_NOT_FOUND);
		}
	}

	c->free(c);
	TEST_CHECK(tr->rollback(tr) == TKVDB_OK);

	tr->free(tr);
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
	tr = tkvdb_tr_create(db, NULL);
	TEST_CHECK(tr != NULL);

	TEST_CHECK(tr->begin(tr) == TKVDB_OK);

	for (i=0; i<N; i++) {
		if (i % 2) {
			tkvdb_datum dtk;

			dtk.data = kvs[i].key;
			dtk.size = kvs[i].klen;
			r = tr->del(tr, &dtk, 0);
			TEST_CHECK(r == TKVDB_OK);
		}
	}

	/* save data to disk */
	TEST_CHECK(tr->commit(tr) == TKVDB_OK);

	/* start transaction again */
	TEST_CHECK(tr->begin(tr) == TKVDB_OK);

	c = tkvdb_cursor_create(tr);
	TEST_CHECK(c != NULL);

	/* iterate forward */
	TEST_CHECK(c->first(c) == TKVDB_OK);
	i = 0;
	do {
		TEST_CHECK(memcmp(c->key(c), kvs[i].key,
			c->keysize(c)) == 0);
		TEST_CHECK(memcmp(c->val(c), kvs[i].val,
			c->valsize(c)) == 0);

		i += 2;
	} while ((r = c->next(c)) == TKVDB_OK);

	TEST_CHECK(i == N);
	TEST_CHECK(tr->rollback(tr) == TKVDB_OK);

	c->free(c);

	tr->free(tr);
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
	tkvdb_params *params;
	tkvdb_datum dtk, dtv;

	params = tkvdb_params_create();
	TEST_CHECK(params != NULL);
	if (test_aligned) {
		tkvdb_param_set(params, TKVDB_PARAM_ALIGNVAL, VAL_ALIGNMENT);
	}

	db = tkvdb_open(fn, params);
	TEST_CHECK(db != NULL);
	tkvdb_params_free(params);

	tr = tkvdb_tr_create(db, NULL);
	TEST_CHECK(tr != NULL);

	TEST_CHECK(tr->begin(tr) == TKVDB_OK);

	/* existent keys */
	for (i=0; i<NITER; i++) {
		int idx;
		TKVDB_RES r;

		idx = rand() % N;

		dtk.data = kvs[idx].key;
		dtk.size = kvs[idx].klen;
		r = tr->get(tr, &dtk, &dtv);
		TEST_CHECK(r == TKVDB_OK);
		TEST_CHECK(kvs[idx].vlen == dtv.size);

		TEST_CHECK(memcmp(dtv.data, kvs[idx].val, dtv.size) == 0);
		if (test_aligned) {
			TEST_CHECK((uintptr_t)dtv.data % VAL_ALIGNMENT == 0);
		}
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

		dtk.data = datum.key;
		dtk.size = datum.klen;
		r = tr->get(tr, &dtk, &dtv);

		TEST_CHECK(r == TKVDB_NOT_FOUND);
	}

	TEST_CHECK(tr->rollback(tr) == TKVDB_OK);

	tr->free(tr);
	tkvdb_close(db);
}

void
test_get_put_aligned(void)
{
	const char fn[] = "data_test.tkv";

	/* read from database, it was filled with transactions in non-aligned
	   mode */
	test_aligned = 1;
	test_get();
	test_aligned = 0;

	/* remove data file */
	TEST_CHECK(unlink(fn) == 0);

	/* fill database again using transactions with aligned data */
	test_aligned = 1;
	test_fill_db();
	/* test aligned reads */
	test_get();

	/* non-aligned reads */
	test_aligned = 0;
	test_get();
}

void
test_dbtrav_aligned(void)
{
	test_aligned = 1;

	test_iter();
	test_seek();

	test_aligned = 0;
}

/* RAM-only transaction size vs transaction with underlying DB size */
void
test_ram_mem(void)
{
	const char fn[] = "data_test.tkv";
	tkvdb *db;
	tkvdb_tr *trdb, *trram;
	int i;
	char strkey[20];
	size_t memdb, memram;

	db = tkvdb_open(fn, NULL);
	TEST_CHECK(db != NULL);

	trdb = tkvdb_tr_create(db, NULL);
	TEST_CHECK(trdb != NULL);

	/* fill DB-transaction */
	TEST_CHECK(trdb->begin(trdb) == TKVDB_OK);
	for (i=0; i<N; i++) {
		tkvdb_datum key, val;

		snprintf(strkey, sizeof(strkey), "%d", i);
		key.data = strkey;
		key.size = strlen(strkey);
		val.data = &i;
		val.size = sizeof(int);
		TEST_CHECK(trdb->put(trdb, &key, &val) == TKVDB_OK);
	}
	/* get memory used by transaction */
	memdb = trdb->mem(trdb);

	TEST_CHECK(trdb->rollback(trdb) == TKVDB_OK);

	trdb->free(trdb);
	tkvdb_close(db);

	/* RAM-only transaction with the same data */
	trram = tkvdb_tr_create(NULL, NULL);
	TEST_CHECK(trram != NULL);

	TEST_CHECK(trram->begin(trram) == TKVDB_OK);
	for (i=0; i<N; i++) {
		tkvdb_datum key, val;

		snprintf(strkey, sizeof(strkey), "%d", i);
		key.data = strkey;
		key.size = strlen(strkey);
		val.data = &i;
		val.size = sizeof(int);
		TEST_CHECK(trram->put(trram, &key, &val) == TKVDB_OK);
	}
	memram = trram->mem(trram);

	TEST_CHECK(trram->rollback(trram) == TKVDB_OK);
	trram->free(trram);

	TEST_CHECK(memram < memdb);
}

/* basic triggers test */
struct basic_trigger_data
{
	size_t inserts;
	size_t updates;
};


static TKVDB_RES
trigger_basic(tkvdb_trigger_info *info)
{
	struct basic_trigger_data *data = info->userdata;

	switch (info->type) {
		case TKVDB_TRIGGER_UPDATE:
			data->updates++;
			break;

		case TKVDB_TRIGGER_INSERT_NEWROOT:
		case TKVDB_TRIGGER_INSERT_NEWNODE:
		case TKVDB_TRIGGER_INSERT_SUBKEY:
		case TKVDB_TRIGGER_INSERT_SHORTER:
		case TKVDB_TRIGGER_INSERT_LONGER:
		case TKVDB_TRIGGER_INSERT_SPLIT:
			data->inserts++;
			break;
			break;

		default:
			break;
	}

	return TKVDB_OK;
}

void
test_triggers_basic(void)
{
	tkvdb_tr *tr;
	int i;
	tkvdb_triggers *trg;
	struct basic_trigger_data userdata1, userdata2, userdata3;
	TKVDB_RES r;

	userdata1.inserts = userdata1.updates = 0;
	userdata2.inserts = userdata2.updates = 0;
	userdata3.inserts = userdata3.updates = 0;

	tr = tkvdb_tr_create(NULL, NULL);
	TEST_CHECK(tr != NULL);

	trg = tkvdb_triggers_create(128);
	TEST_CHECK(trg != NULL);

	r = tkvdb_triggers_add(trg, &trigger_basic, sizeof(uint64_t),
		&userdata1);
	TEST_CHECK(r == TKVDB_OK);

	r = tkvdb_triggers_add(trg, &trigger_basic, sizeof(uint64_t),
		&userdata2);
	TEST_CHECK(r == TKVDB_OK);

	r = tkvdb_triggers_add(trg, &trigger_basic, sizeof(uint64_t),
		&userdata3);
	TEST_CHECK(r == TKVDB_OK);

	TEST_CHECK(tr->begin(tr) == TKVDB_OK);
	for (i=0; i<N; i++) {
		tkvdb_datum key, val;

		key.data = kvs_unsorted[i].key;
		key.size = kvs_unsorted[i].klen;
		val.data = kvs_unsorted[i].val;
		val.size = kvs_unsorted[i].vlen;

		/* 1 insert and 2 updates for each key */
		TEST_CHECK(tr->putx(tr, &key, &val, trg) == TKVDB_OK);

		TEST_CHECK(tr->putx(tr, &key, &val, trg) == TKVDB_OK);
		TEST_CHECK(tr->putx(tr, &key, &val, trg) == TKVDB_OK);
	}
	TEST_CHECK(tr->rollback(tr) == TKVDB_OK);
	tr->free(tr);
	tkvdb_triggers_free(trg);

	TEST_CHECK(userdata1.inserts + userdata1.updates == (N + N * 2));
	TEST_CHECK(userdata2.inserts + userdata2.updates == (N + N * 2));
	TEST_CHECK(userdata3.inserts + userdata3.updates == (N + N * 2));
	TEST_CHECK(userdata1.inserts == userdata2.inserts);
	TEST_CHECK(userdata2.inserts == userdata3.inserts);
}


static TKVDB_RES
trigger_nth(tkvdb_trigger_info *info)
{
	size_t i;

	switch (info->type) {
		case TKVDB_TRIGGER_UPDATE:
			break;
		case TKVDB_TRIGGER_INSERT_NEWROOT:
			*((uint64_t *)info->newroot) = 1;

			break;
		case TKVDB_TRIGGER_INSERT_NEWNODE:
			*((uint64_t *)info->subnode1) = 1;

			for (i=0; i<info->stack->size; i++) {
				*((uint64_t *)info->stack->meta[i]) += 1;
			}

			break;
		case TKVDB_TRIGGER_INSERT_SUBKEY:
			*((uint64_t *)info->newroot) += 1;

			for (i=0; i<info->stack->size; i++) {
				*((uint64_t *)info->stack->meta[i]) += 1;
			}

			break;
		case TKVDB_TRIGGER_INSERT_SHORTER:
			*((uint64_t *)info->newroot) =
			*((uint64_t *)info->subnode1) + 1;

			for (i=0; i<info->stack->size; i++) {
				*((uint64_t *)info->stack->meta[i]) += 1;
			}

			break;
		case TKVDB_TRIGGER_INSERT_LONGER:
			*((uint64_t *)info->newroot) += 1;
			*((uint64_t *)info->subnode1) = 1;

			for (i=0; i<info->stack->size; i++) {
				*((uint64_t *)info->stack->meta[i]) += 1;
			}

			break;
		case TKVDB_TRIGGER_INSERT_SPLIT:
			*((uint64_t *)info->newroot) =
				*((uint64_t *)info->subnode1) + 1;
			*((uint64_t *)info->subnode2) = 1;

			for (i=0; i<info->stack->size; i++) {
				*((uint64_t *)info->stack->meta[i]) += 1;
			}

			break;

		case TKVDB_TRIGGER_DELETE_ROOT:
			break;

		case TKVDB_TRIGGER_DELETE_PREFIX:
			break;

		case TKVDB_TRIGGER_DELETE_LEAF:
			for (i=0; i<(info->stack->size - 1); i++) {
				*((uint64_t *)info->stack->meta[i]) -= 1;
			}
			break;

		case TKVDB_TRIGGER_DELETE_INTNODE:
			for (i=0; i<info->stack->size; i++) {
				*((uint64_t *)info->stack->meta[i]) -= 1;
			}
			break;

		default:
			break;
	}

	return TKVDB_OK;
}

#define TKVDB_TRG_GET_NTH_REALLOC_KEY(K, LEN, A)                      \
do {                                                                  \
	if (A < (K->size + LEN)) {                                    \
		void *tmp = realloc(K->data, K->size + LEN);          \
		if (!tmp) {                                           \
			return TKVDB_ENOMEM;                          \
		}                                                     \
		K->data = tmp;                                        \
		A = K->size + LEN;                                    \
	}                                                             \
} while (0)

static TKVDB_RES
tkvdb_get_nth(tkvdb_tr *tr, uint64_t n, tkvdb_datum *key, tkvdb_datum *val,
		tkvdb_datum *prealloc)
{
	tkvdb_datum pfx, meta;
	void *node, *subnode;
	uint64_t s = 0;
	TKVDB_RES r;
	int i, prev_i = 0;
	uint64_t nmeta;
	size_t allocated;

	if (prealloc) {
		allocated = prealloc->size;
		key->data = prealloc->data;
	} else {
		allocated = 0;
		key->data = NULL;
	}
	key->size = 0;

	/* get root node */
	r = tr->subnode(tr, NULL, 0, &node, &pfx, val, &meta);
	if (r != TKVDB_OK) {
		return r;
	}
	nmeta = *((uint64_t *)meta.data);

	if ((n + 1) > nmeta) {
		return TKVDB_NOT_FOUND;
	}
next:
	/* append prefix to key */
	TKVDB_TRG_GET_NTH_REALLOC_KEY(key, pfx.size, allocated);
	memcpy((char *)(key->data) + key->size, pfx.data, pfx.size);
	key->size += pfx.size;

	if (val->data) {
		s += 1;
		if (s == (n + 1)) {
			goto ok;
		}
	}

	for (i=0; i<256; i++) {
		r = tr->subnode(tr, node, i, &subnode, &pfx, val, &meta);
		if (r != TKVDB_OK) {
			continue;
		}

		nmeta = *((uint64_t *)meta.data);

		prev_i = i;

		if ((s + nmeta) == (n + 1)) {
			TKVDB_TRG_GET_NTH_REALLOC_KEY(key, 1, allocated);
			((char *)(key->data))[key->size] = i;
			key->size += 1;

			if (!val->data) {
				/* non-val node */
				node = subnode;
				prev_i = i;
				goto next;
			} else {
				if (nmeta > 1) {
					node = subnode;
					prev_i = i;
					goto next;
				}
				/* append last prefix */
				TKVDB_TRG_GET_NTH_REALLOC_KEY(key, pfx.size,
					allocated);
				memcpy((char *)(key->data) + key->size,
					pfx.data, pfx.size);
				key->size += pfx.size;

				goto ok;
			}
		} else if ((s + nmeta) > (n + 1)) {
			node = subnode;

			TKVDB_TRG_GET_NTH_REALLOC_KEY(key, 1, allocated);
			((char *)(key->data))[key->size] = i;
			key->size += 1;

			prev_i = i;
			goto next;
		}
		s += nmeta;
	}

	if ((!val->data) || (val->data && (nmeta > 1))) {
		TKVDB_TRG_GET_NTH_REALLOC_KEY(key, 1, allocated);
		((char *)(key->data))[key->size] = prev_i;
		key->size += 1;

		s -= nmeta;
		node = subnode;
		goto next;
	}
ok:
	if (prealloc) {
		prealloc->size = allocated;
		prealloc->data = key->data;
	}
	return TKVDB_OK;
}
#undef TKVDB_TRG_GET_NTH_REALLOC_KEY

void
test_triggers_nth(void)
{
	tkvdb_tr *tr;
	int i;
	tkvdb_triggers *trg;
	TKVDB_RES r;

	tr = tkvdb_tr_create(NULL, NULL);
	TEST_CHECK(tr != NULL);

	trg = tkvdb_triggers_create(128);
	TEST_CHECK(trg != NULL);

	r = tkvdb_triggers_add(trg, &trigger_nth, sizeof(uint64_t), NULL);
	TEST_CHECK(r == TKVDB_OK);

	TEST_CHECK(tr->begin(tr) == TKVDB_OK);
	/* fill transaction with unsorted kv-pairs */
	for (i=0; i<N; i++) {
		tkvdb_datum key, val;

		key.data = kvs_unsorted[i].key;
		key.size = kvs_unsorted[i].klen;
		val.data = kvs_unsorted[i].val;
		val.size = kvs_unsorted[i].vlen;

		TEST_CHECK(tr->putx(tr, &key, &val, trg) == TKVDB_OK);
	}

	/* get all pairs by number */
	for (i=0; i<N; i++) {
		tkvdb_datum key, val, prealloc;
		char databuf[KLEN];

		memset(databuf, 0, sizeof(databuf));
		prealloc.size = sizeof(databuf);
		prealloc.data = databuf;

		r = tkvdb_get_nth(tr, i, &key, &val, &prealloc);

		TEST_CHECK(val.size == kvs[i].vlen);
		TEST_CHECK(key.size == kvs[i].klen);
		TEST_CHECK(memcmp(key.data, kvs[i].key, key.size) == 0);
		TEST_CHECK(memcmp(val.data, kvs[i].val, val.size) == 0);
	}

	/* remove each 2-nd key-value pair */
	for (i=0; i<N/2; i++) {
		tkvdb_datum key;

		key.data = kvs[i * 2].key;
		key.size = kvs[i * 2].klen;

		TEST_CHECK(tr->delx(tr, &key, 0, trg) == TKVDB_OK);
	}

	for (i=0; i<N/2; i++) {
		tkvdb_datum key, val, prealloc;
		char databuf[KLEN];

		memset(databuf, 0, sizeof(databuf));
		prealloc.size = sizeof(databuf);
		prealloc.data = databuf;

		r = tkvdb_get_nth(tr, i, &key, &val, &prealloc);

		TEST_CHECK(val.size == kvs[i * 2 + 1].vlen);
		TEST_CHECK(key.size == kvs[i * 2 + 1].klen);
		TEST_CHECK(memcmp(key.data, kvs[i * 2 + 1].key, key.size) == 0);
		TEST_CHECK(memcmp(val.data, kvs[i * 2 + 1].val, val.size) == 0);
	}


	TEST_CHECK(tr->rollback(tr) == TKVDB_OK);
	tr->free(tr);
	tkvdb_triggers_free(trg);
}

#if 0
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
	tr = tkvdb_tr_create(db, NULL);
	TEST_CHECK(tr != NULL);

	for (i=0; i<NI; i++) {

		TEST_CHECK(tkvdb_begin(tr) == TKVDB_OK);

		for (j=0; j<NJ; j++) {
			tkvdb_datum datkey;
			char key[NK];

			sprintf(key, "%u-%03u", (unsigned int)i,
				rand() % 1000);
			datkey.len = strlen(key) + 1;
			datkey.data = key;

			TEST_CHECK(tkvdb_put(tr, &datkey, &datkey)
				== TKVDB_OK);
		}
		TEST_CHECK(tkvdb_commit(tr) == TKVDB_OK);
	}

	/* vacuum */
	vac = tkvdb_tr_create(db, NULL);
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
#endif

TEST_LIST = {
	{ "open db", test_open_db },
	{ "open incorrect db file", test_open_incorrect_db },
	{ "fill db", test_fill_db },
	{ "first/last and next/prev", test_iter },
	{ "random seeks", test_seek },
	{ "get", test_get },
	{ "get/put aligned", test_get_put_aligned },
	{ "db traversal aligned", test_dbtrav_aligned },
	{ "delete", test_del },
	{ "ram-only memory usage", test_ram_mem },
	{ "triggers basic", test_triggers_basic },
	{ "triggers nth", test_triggers_nth },
	/*{ "vacuum", test_vacuum },*/
	{ 0 }
};

