#include <stdlib.h>
#include <pthread.h>
#include <errno.h>

#include "tkvdb_mtn.h"

struct mpmcdata
{
	tkvdb_tr *banks[3];
	int ns_sleep;

	size_t nproducers;
	tkvdb_mtn **producers;

	size_t nsubaggrs;
	tkvdb_mtn **subaggrs;

	size_t npendingadd, npendingdel;
	tkvdb_mtn **pendingadd, **pendingdel;
	pthread_mutex_t pending_mutex;

	pthread_t tid;
	int stop;
};

struct tkvdb_mtn
{
	TKVDB_MTN_TYPE type;

	union data_t
	{
		pthread_mutex_t mutex;
		pthread_spinlock_t spin;
		tkvdb_tr *spmc_banks[2];
		struct mpmcdata mpmc;
	} data;

	tkvdb_tr *tr;
};

struct tkvdb_mtn_cursor
{
	tkvdb_mtn *mtn;
	tkvdb_cursor *c;
};

/* transaction with pthread locks */
tkvdb_mtn *
tkvdb_mtn_create_locked(tkvdb_tr *tr, TKVDB_MTN_TYPE type)
{
	tkvdb_mtn *mtn;

	mtn = malloc(sizeof(tkvdb_mtn));
	if (!mtn) {
		goto fail_alloc;
	}

	switch (type) {
		case TKVDB_MTN_MUTEX:
		case TKVDB_MTN_MUTEX_TRY:
			if (pthread_mutex_init(&mtn->data.mutex, NULL) != 0) {
				goto fail_lock;
			}
			break;
		case TKVDB_MTN_SPINLOCK:
		case TKVDB_MTN_SPINLOCK_TRY:
			if (pthread_spin_init(&mtn->data.spin, 0) != 0) {
				goto fail_lock;
			}
			break;

		default:
			/* unknown type */
			goto fail_unk;
			break;
	}

	mtn->type = type;
	mtn->tr = tr;

	return mtn;

fail_unk:
fail_lock:
	free(mtn);
fail_alloc:
	return NULL;
}

/* "wait-free" single producer, multiple consumers */
tkvdb_mtn *
tkvdb_mtn_create_spmc(tkvdb_tr *tr1, tkvdb_tr *tr2)
{
	tkvdb_mtn *mtn;

	mtn = malloc(sizeof(tkvdb_mtn));
	if (!mtn) {
		goto fail_alloc;
	}

	mtn->type = TKVDB_MTN_WAITFREE_SPMC;

	mtn->data.spmc_banks[0] = tr1;
	mtn->data.spmc_banks[1] = tr2;

	mtn->tr = tr1;

	return mtn;

fail_alloc:
	return NULL;
}


/* "wait-free" multiple producers, multiple consumers */
static void
tkvdb_mtn_mpmc_pendingadd(struct mpmcdata *mpmc)
{
	size_t i, j;

	pthread_mutex_lock(&mpmc->pending_mutex);

	for (i=0; i<mpmc->npendingadd; i++) {
		/* skip existing producers */
		int found = 0;
		tkvdb_mtn **tmpprod;

		for (j=0; j<mpmc->nproducers; j++) {
			if (mpmc->pendingadd[i] == mpmc->producers[j]) {
				found = 1;
			}
		}

		if (found) {
			continue;
		}

		tmpprod = realloc(mpmc->producers,
			sizeof(tkvdb_mtn *)
			* (mpmc->nproducers + 1));

		if (!tmpprod) {
			/* do nothing */
			continue;
		}
		mpmc->producers = tmpprod;
		mpmc->producers[mpmc->nproducers] = mpmc->pendingadd[i];
		mpmc->nproducers++;
	}

	pthread_mutex_unlock(&mpmc->pending_mutex);
}

/* mpmc merge thread */
static void *
tkvdb_mtn_mpmc_thread(void *arg)
{
	struct mpmcdata *mpmc;
	struct timespec delay;
	const int billion = 1000000000;

	mpmc = arg;
	delay.tv_sec  = mpmc->ns_sleep / billion;
	delay.tv_nsec = mpmc->ns_sleep % billion;

	while (!mpmc->stop) {
		size_t i;

		for (i=0; i<mpmc->nproducers; i++) {
			/* FIXME: tbw */
		}

		if (mpmc->ns_sleep) {
			clock_nanosleep(CLOCK_MONOTONIC, 0, &delay, NULL);
		}

		/* check for new producers */
		if (mpmc->npendingadd) {
			tkvdb_mtn_mpmc_pendingadd(mpmc);
		}
	}
	return NULL;
}

/* create multiple producers/multiple consumers transaction
   and start merge thread */
tkvdb_mtn *
tkvdb_mtn_create_mpmc(tkvdb_tr *tr1, tkvdb_tr *tr2, tkvdb_tr *tr3,
	int ns_sleep)
{
	tkvdb_mtn *mtn;

	mtn = malloc(sizeof(tkvdb_mtn));
	if (!mtn) {
		goto fail_alloc;
	}

	if (pthread_mutex_init(&mtn->data.mpmc.pending_mutex, NULL) != 0) {
		goto fail_alloc;
	}

	mtn->type = TKVDB_MTN_WAITFREE_MPMC;

	mtn->tr = tr1;

	mtn->data.mpmc.banks[0] = tr1;
	mtn->data.mpmc.banks[1] = tr2;
	mtn->data.mpmc.banks[2] = tr3;

	mtn->data.mpmc.ns_sleep = ns_sleep;

	mtn->data.mpmc.nproducers = 0;
	mtn->data.mpmc.producers = NULL;

	mtn->data.mpmc.nsubaggrs = 0;
	mtn->data.mpmc.subaggrs = NULL;

	mtn->data.mpmc.stop  = 0;

	/* start merge thread */
	if (pthread_create(&mtn->data.mpmc.tid, NULL,
		&tkvdb_mtn_mpmc_thread, mtn) != 0) {

		goto fail_thread;
	}

	return mtn;

fail_thread:
	free(mtn);

fail_alloc:
	return NULL;
}

int
tkvdb_mtn_mpmc_add_producer(tkvdb_mtn *mpmc, tkvdb_mtn *subaggr, tkvdb_mtn *p)
{
	tkvdb_mtn **tmppadd;

	if (mpmc->type != TKVDB_MTN_WAITFREE_MPMC) {
		return 0;
	}

	if (subaggr) {
	}

	pthread_mutex_lock(&mpmc->data.mpmc.pending_mutex);

	/* append producer to pending producers */
	tmppadd = realloc(mpmc->data.mpmc.pendingadd,
		sizeof(struct tkvdb_mtn *)
		* (mpmc->data.mpmc.npendingadd) + 1);

	if (!tmppadd) {
		return 0;
	}

	mpmc->data.mpmc.pendingadd = tmppadd;
	mpmc->data.mpmc.pendingadd[mpmc->data.mpmc.npendingadd] = p;
	mpmc->data.mpmc.npendingadd++;

	pthread_mutex_unlock(&mpmc->data.mpmc.pending_mutex);

	return 1;
}

void
tkvdb_mtn_free(tkvdb_mtn *mtn)
{
	switch (mtn->type) {
		case TKVDB_MTN_MUTEX:
		case TKVDB_MTN_MUTEX_TRY:
			pthread_mutex_destroy(&mtn->data.mutex);
			break;

		case TKVDB_MTN_SPINLOCK:
		case TKVDB_MTN_SPINLOCK_TRY:
			pthread_spin_destroy(&mtn->data.spin);
			break;

		case TKVDB_MTN_WAITFREE_SPMC:
			break;

		case TKVDB_MTN_WAITFREE_MPMC:
			mtn->data.mpmc.stop = 1;
			pthread_join(mtn->data.mpmc.tid, NULL);
			break;
	}

	free(mtn);
}

#define LOCKED_CASE(FUNC, RC, LOCK, SPIN, LOCKRES)                \
case TKVDB_MTN_MUTEX:                                             \
	pthread_mutex_lock(LOCK);                                 \
	RC = FUNC;                                                \
	pthread_mutex_unlock(LOCK);                               \
	break;                                                    \
case TKVDB_MTN_MUTEX_TRY:                                         \
	LOCKRES = pthread_mutex_trylock(LOCK);                    \
	if (LOCKRES == EBUSY) {                                   \
		return TKVDB_LOCKED;                              \
	}                                                         \
	RC = FUNC;                                                \
	pthread_mutex_unlock(LOCK);                               \
	break;                                                    \
case TKVDB_MTN_SPINLOCK:                                          \
	pthread_spin_lock(SPIN);                                  \
	RC = FUNC;                                                \
	pthread_spin_unlock(SPIN);                                \
	break;                                                    \
case TKVDB_MTN_SPINLOCK_TRY:                                      \
	LOCKRES = pthread_spin_trylock(SPIN);                     \
	if (LOCKRES == EBUSY) {                                   \
		return TKVDB_LOCKED;                              \
	}                                                         \
	RC = FUNC;                                                \
	pthread_spin_unlock(SPIN);                                \
	break;


TKVDB_RES
tkvdb_mtn_begin(tkvdb_mtn *mtn)
{
	TKVDB_RES rc = TKVDB_CORRUPTED;
	int lr;

	switch (mtn->type) {
		LOCKED_CASE( mtn->tr->begin(mtn->tr) , rc,
			&mtn->data.mutex, &mtn->data.spin, lr);

		case TKVDB_MTN_WAITFREE_SPMC:
			rc = mtn->tr->begin(mtn->tr);
			break;
	}

	return rc;
}

TKVDB_RES
tkvdb_mtn_commit(tkvdb_mtn *mtn)
{
	TKVDB_RES rc = TKVDB_CORRUPTED;
	int lr;

	switch (mtn->type) {
		LOCKED_CASE( mtn->tr->commit(mtn->tr) , rc,
			&mtn->data.mutex, &mtn->data.spin, lr);

		case TKVDB_MTN_WAITFREE_SPMC:
			/* commit unused bank and switch to it */
			if (mtn->tr == mtn->data.spmc_banks[0]) {
				rc = mtn->data.spmc_banks[1]
					->commit(mtn->data.spmc_banks[1]);
				mtn->tr = mtn->data.spmc_banks[1];
			} else {
				rc = mtn->data.spmc_banks[0]
					->commit(mtn->data.spmc_banks[0]);
				mtn->tr = mtn->data.spmc_banks[0];
			}

			break;
	}

	return rc;
}

TKVDB_RES
tkvdb_mtn_rollback(tkvdb_mtn *mtn)
{
	TKVDB_RES rc = TKVDB_CORRUPTED;
	int lr;

	switch (mtn->type) {
		LOCKED_CASE( mtn->tr->rollback(mtn->tr) , rc,
			&mtn->data.mutex, &mtn->data.spin, lr);

		case TKVDB_MTN_WAITFREE_SPMC:
			/* reset unused bank and switch to it */
			if (mtn->tr == mtn->data.spmc_banks[0]) {
				rc = mtn->data.spmc_banks[1]
					->rollback(mtn->data.spmc_banks[1]);
				mtn->tr = mtn->data.spmc_banks[1];
			} else {
				rc = mtn->data.spmc_banks[0]
					->rollback(mtn->data.spmc_banks[0]);
				mtn->tr = mtn->data.spmc_banks[0];
			}
			break;
	}

	return rc;
}

TKVDB_RES
tkvdb_mtn_put(tkvdb_mtn *mtn, const tkvdb_datum *key, const tkvdb_datum *val)
{
	TKVDB_RES rc = TKVDB_CORRUPTED;
	int lr;

	switch (mtn->type) {
		LOCKED_CASE( mtn->tr->put(mtn->tr, key, val) , rc,
			&mtn->data.mutex, &mtn->data.spin, lr);

		case TKVDB_MTN_WAITFREE_SPMC:
			rc = mtn->tr->put(mtn->tr, key, val);
			break;
	}

	return rc;
}

TKVDB_RES
tkvdb_mtn_get(tkvdb_mtn *mtn, const tkvdb_datum *key, tkvdb_datum *val)
{
	TKVDB_RES rc = TKVDB_CORRUPTED;
	int lr;

	switch (mtn->type) {
		LOCKED_CASE( mtn->tr->get(mtn->tr, key, val) , rc,
			&mtn->data.mutex, &mtn->data.spin, lr);

		case TKVDB_MTN_WAITFREE_SPMC:
			rc = mtn->tr->get(mtn->tr, key, val);
			break;
	}

	return rc;
}

TKVDB_RES
tkvdb_mtn_del(tkvdb_mtn *mtn, const tkvdb_datum *key, int del_pfx)
{
	TKVDB_RES rc = TKVDB_CORRUPTED;
	int lr;

	switch (mtn->type) {
		LOCKED_CASE( mtn->tr->del(mtn->tr, key, del_pfx) , rc,
			&mtn->data.mutex, &mtn->data.spin, lr);

		case TKVDB_MTN_WAITFREE_SPMC:
			rc = mtn->tr->del(mtn->tr, key, del_pfx);
			break;
	}

	return rc;
}

/* cursors */
tkvdb_mtn_cursor *
tkvdb_mtn_cursor_create(tkvdb_mtn *mtn)
{
	tkvdb_mtn_cursor *c;

	c = malloc(sizeof(tkvdb_mtn_cursor));
	if (!c) {
		goto fail_alloc;
	}

	c->c = tkvdb_cursor_create(mtn->tr);
	if (!c->c) {
		goto fail_cursor_create;
	}

	c->mtn = mtn;

	return c;

fail_cursor_create:
	free(c);

fail_alloc:
	return NULL;
}

void *
tkvdb_mtn_cursor_key(tkvdb_mtn_cursor *c)
{
	return c->c->key(c->c);
}

size_t
tkvdb_mtn_cursor_keysize(tkvdb_mtn_cursor *c)
{
	return c->c->keysize(c->c);
}

void *
tkvdb_mtn_cursor_val(tkvdb_mtn_cursor *c)
{
	return c->c->val(c->c);
}

size_t
tkvdb_mtn_cursor_valsize(tkvdb_mtn_cursor *c)
{
	return c->c->valsize(c->c);
}

TKVDB_RES
tkvdb_mtn_cursor_seek(tkvdb_mtn_cursor *c, const tkvdb_datum *key,
	TKVDB_SEEK seek)
{
	return c->c->seek(c->c, key, seek);
}

TKVDB_RES
tkvdb_mtn_cursor_first(tkvdb_mtn_cursor *c)
{
	return c->c->first(c->c);
}

TKVDB_RES
tkvdb_mtn_cursor_last(tkvdb_mtn_cursor *c)
{
	return c->c->last(c->c);
}


TKVDB_RES
tkvdb_mtn_cursor_next(tkvdb_mtn_cursor *c)
{
	return c->c->next(c->c);
}

TKVDB_RES
tkvdb_mtn_cursor_prev(tkvdb_mtn_cursor *c)
{
	return c->c->prev(c->c);
}

void
tkvdb_mtn_cursor_free(tkvdb_mtn_cursor *c)
{
	c->c->free(c->c);
	free(c);
}

