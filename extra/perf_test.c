#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <alloca.h>
#include <time.h>
#include <signal.h>

#include "tkvdb.h"

/* transaction size in bytes */
static size_t trsize = 1024*1024*2000;
/* start benchmark with this number of items */
static size_t nkeys = 10000;
/* up to */
static size_t nitemsmax = 2800000;
/* step */
static size_t step = 10000;

/* number of read iterations  */
size_t nreads = 500;

static void
ctrl_c_handler(int s)
{
	(void)s;

	exit(0);
}

static void
lookups_per_sec(size_t keylen, size_t keys, size_t niters,
	double *tm_put, double *tm_get)
{
	tkvdb_params *params;
	tkvdb_datum dtk, dtv;
	tkvdb_tr *tr;
	size_t i, j, n;
	uint64_t val;
	char *key, *keyinc;
	struct timespec ts_before, ts_after;

	/* allocate key */
	key = alloca(keylen);
	keyinc = alloca(keylen);

	dtk.data = key;
	dtk.size = keylen;
	dtv.data = &val;
	dtv.size = sizeof(val);

	params = tkvdb_params_create();
	assert(params);
	tkvdb_param_set(params, TKVDB_PARAM_TR_DYNALLOC, 0);
	tkvdb_param_set(params, TKVDB_PARAM_TR_LIMIT, trsize);
	tr = tkvdb_tr_create(NULL, params);
	assert(tr);
	tkvdb_params_free(params);

	/* fill transaction */
	val = rand();
	assert(tr->begin(tr) == TKVDB_OK);
	clock_gettime(CLOCK_MONOTONIC, &ts_before);
	for (n=0; n<niters; n++) {
		/* run it 'niters' times */
		for (j=0; j<keylen; j++) {
			key[j] = rand();
			keyinc[j] = rand();
		}
		for (i=0; i<keys; i++) {
			assert(tr->put(tr, &dtk, &dtv) == TKVDB_OK);

			for (j=0; j<keylen; j++) {
				key[j] += keyinc[j];
			}
		}
		if (n != (niters - 1)) {
			tr->rollback(tr);
			tr->begin(tr);
		}
	}
	clock_gettime(CLOCK_MONOTONIC, &ts_after);
	*tm_put = ((double)ts_after.tv_sec + (double)ts_after.tv_nsec / 1e9)
		- ((double)ts_before.tv_sec + (double)ts_before.tv_nsec / 1e9);
	*tm_put /= keys;

	for (j=0; j<keylen; j++) {
		key[j] = rand();
		keyinc[j] = rand();
	}

	clock_gettime(CLOCK_MONOTONIC, &ts_before);
	for (i=0; i<niters; i++) {
		tr->get(tr, &dtk, &dtv);
		for (j=0; j<keylen; j++) {
			key[j] += keyinc[j];
		}
	}
	clock_gettime(CLOCK_MONOTONIC, &ts_after);
	*tm_get = ((double)ts_after.tv_sec + (double)ts_after.tv_nsec / 1e9)
		- ((double)ts_before.tv_sec + (double)ts_before.tv_nsec / 1e9);

	tr->rollback(tr);
	tr->free(tr);
}

int
main()
{
	struct sigaction sig;

	sig.sa_handler = &ctrl_c_handler;
	sigemptyset(&sig.sa_mask);
	sig.sa_flags = 0;
	sigaction(SIGINT, &sig, NULL);

	for (; nkeys<nitemsmax; nkeys+=step) {
		double tm4_put, tm4_get, tm16_put, tm16_get;
		lookups_per_sec(4, nkeys, nreads, &tm4_put, &tm4_get);
		lookups_per_sec(16, nkeys, nreads, &tm16_put, &tm16_get);
		printf("%lu, %f, %f, %f, %f\n", nkeys,
			(double)nreads / tm4_put,
			(double)nreads / tm4_get,
			(double)nreads / tm16_put,
			(double)nreads / tm16_get);
	}

	return EXIT_SUCCESS;
}

