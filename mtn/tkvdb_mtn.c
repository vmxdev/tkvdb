#include <stdlib.h>
#include <pthread.h>
#include <errno.h>

#include "tkvdb_mtn.h"

/* We're using 3 mwmr banks:
 * 1. for stalled readers
 * 2. active
 * 3. currently updated
 */
#define MWMR_BANKS 3
#define MWMR_BANK_STALLED  0
#define MWMR_BANK_ACTIVE   1
#define MWMR_BANK_UPDATED  2

struct mwmrdata
{
	tkvdb_tr *banks[MWMR_BANKS];
	unsigned int bank_ptr;

	mwmr_aggr aggr_func;

	int ns_sleep;

	size_t nwriters;
	tkvdb_mtn **writers;

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
		struct mwmrdata mwmr;
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

/* "wait-free" single writer, multiple readers */
tkvdb_mtn *
tkvdb_mtn_create_spmc(tkvdb_tr *tr1, tkvdb_tr *tr2)
{
	tkvdb_mtn *mtn;

	mtn = malloc(sizeof(tkvdb_mtn));
	if (!mtn) {
		goto fail_alloc;
	}

	mtn->type = TKVDB_MTN_WAITFREE_SWMR;

	mtn->data.spmc_banks[0] = tr1;
	mtn->data.spmc_banks[1] = tr2;

	mtn->tr = tr1;

	return mtn;

fail_alloc:
	return NULL;
}


/* "wait-free" multiple writers, multiple readers */
static void
tkvdb_mtn_mwmr_pendingadd(struct mwmrdata *mwmr)
{
	size_t i, j;

	pthread_mutex_lock(&mwmr->pending_mutex);

	for (i=0; i<mwmr->npendingadd; i++) {
		/* skip existing writers */
		int found = 0;
		tkvdb_mtn **tmpwr;

		for (j=0; j<mwmr->nwriters; j++) {
			if (mwmr->pendingadd[i] == mwmr->writers[j]) {
				found = 1;
			}
		}

		if (found) {
			/* nothing to add */
			continue;
		}

		tmpwr = realloc(mwmr->writers,
			sizeof(tkvdb_mtn *)
			* (mwmr->nwriters + 1));

		if (!tmpwr) {
			/* do nothing */
			continue;
		}
		mwmr->writers = tmpwr;
		mwmr->writers[mwmr->nwriters] = mwmr->pendingadd[i];
		mwmr->nwriters++;
	}

	pthread_mutex_unlock(&mwmr->pending_mutex);
}

/* mwmr merge thread */
static void *
tkvdb_mtn_mwmr_thread(void *arg)
{
	struct mwmrdata *mwmr;
	struct timespec delay;
	const int billion = 1000000000;

	mwmr = arg;
	delay.tv_sec  = mwmr->ns_sleep / billion;
	delay.tv_nsec = mwmr->ns_sleep % billion;

	while (!mwmr->stop) {
		size_t i;
		tkvdb_tr *tr;

		tr = mwmr->banks[(mwmr->bank_ptr + MWMR_BANK_UPDATED)
			% MWMR_BANKS];

		tr->rollback(tr);
		tr->begin(tr);

		for (i=0; i<mwmr->nwriters; i++) {
			tkvdb_mtn_cursor *c;
			TKVDB_RES rc;

			/* create cursor */
			c = tkvdb_mtn_cursor_create(mwmr->writers[i]);
			if (!c) {
				/* error */
				break;
			}

			rc = tkvdb_mtn_cursor_first(c);
			while (rc == TKVDB_OK) {
				tkvdb_datum dtk, dtv;

				dtk.data = tkvdb_mtn_cursor_key(c);
				dtk.size = tkvdb_mtn_cursor_keysize(c);

				rc = tr->get(tr, &dtk, &dtv);
				if (rc == TKVDB_OK) {
					/* apply aggregation function */
					(*mwmr->aggr_func)(dtv.data,
						tkvdb_mtn_cursor_val(c));
				} else {
					/* new k-v pair */
					dtv.data = tkvdb_mtn_cursor_val(c);
					dtv.size = tkvdb_mtn_cursor_valsize(c);
				}

				tr->put(tr, &dtk, &dtv);
				tkvdb_mtn_cursor_next(c);
			}

			tkvdb_mtn_cursor_free(c);
		}

		if (mwmr->ns_sleep) {
			clock_nanosleep(CLOCK_MONOTONIC, 0, &delay, NULL);
		}

		/* check for new writers */
		if (mwmr->npendingadd) {
			tkvdb_mtn_mwmr_pendingadd(mwmr);
		}

		/* switch banks */
		mwmr->bank_ptr++;
	}
	return NULL;
}

/* create multiple writers/multiple readers transaction
   and start merge thread */
tkvdb_mtn *
tkvdb_mtn_create_mwmr(tkvdb_tr *tr1, tkvdb_tr *tr2, tkvdb_tr *tr3,
	mwmr_aggr aggr_func, int ns_sleep)
{
	tkvdb_mtn *mtn;

	mtn = malloc(sizeof(tkvdb_mtn));
	if (!mtn) {
		goto fail_alloc;
	}

	if (pthread_mutex_init(&mtn->data.mwmr.pending_mutex, NULL) != 0) {
		goto fail_alloc;
	}

	mtn->type = TKVDB_MTN_WAITFREE_MWMR;

	mtn->tr = tr1;

	mtn->data.mwmr.banks[0] = tr1;
	mtn->data.mwmr.banks[1] = tr2;
	mtn->data.mwmr.banks[2] = tr3;

	mtn->data.mwmr.bank_ptr = 0;

	mtn->data.mwmr.ns_sleep = ns_sleep;

	mtn->data.mwmr.nwriters = 0;
	mtn->data.mwmr.writers = NULL;

	mtn->data.mwmr.nsubaggrs = 0;
	mtn->data.mwmr.subaggrs = NULL;

	mtn->data.mwmr.stop  = 0;

	mtn->data.mwmr.aggr_func  = aggr_func;

	/* start merge thread */
	if (pthread_create(&mtn->data.mwmr.tid, NULL,
		&tkvdb_mtn_mwmr_thread, mtn) != 0) {

		goto fail_thread;
	}

	return mtn;

fail_thread:
	free(mtn);

fail_alloc:
	return NULL;
}

int
tkvdb_mtn_mwmr_add_writer(tkvdb_mtn *mwmr, tkvdb_mtn *subaggr, tkvdb_mtn *p)
{
	tkvdb_mtn **tmppadd;

	if (mwmr->type != TKVDB_MTN_WAITFREE_MWMR) {
		return 0;
	}

	if (subaggr) {
	}

	pthread_mutex_lock(&mwmr->data.mwmr.pending_mutex);

	/* append writer to pending writers */
	tmppadd = realloc(mwmr->data.mwmr.pendingadd,
		sizeof(struct tkvdb_mtn *)
		* (mwmr->data.mwmr.npendingadd) + 1);

	if (!tmppadd) {
		return 0;
	}

	mwmr->data.mwmr.pendingadd = tmppadd;
	mwmr->data.mwmr.pendingadd[mwmr->data.mwmr.npendingadd] = p;
	mwmr->data.mwmr.npendingadd++;

	pthread_mutex_unlock(&mwmr->data.mwmr.pending_mutex);

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

		case TKVDB_MTN_WAITFREE_SWMR:
			break;

		case TKVDB_MTN_WAITFREE_MWMR:
			mtn->data.mwmr.stop = 1;
			pthread_join(mtn->data.mwmr.tid, NULL);
			break;
	}

	free(mtn);
}

int
tkvdb_mtn_lock(tkvdb_mtn *mtn)
{
	int rc;

	switch (mtn->type) {
		case TKVDB_MTN_MUTEX:
		case TKVDB_MTN_MUTEX_TRY:
			rc = pthread_mutex_lock(&mtn->data.mutex);
			break;

		case TKVDB_MTN_SPINLOCK:
		case TKVDB_MTN_SPINLOCK_TRY:
			rc = pthread_spin_lock(&mtn->data.spin);
			break;

		default:
			rc = 0;
			break;
	}

	return rc;
}

int
tkvdb_mtn_unlock(tkvdb_mtn *mtn)
{
	int rc;

	switch (mtn->type) {
		case TKVDB_MTN_MUTEX:
		case TKVDB_MTN_MUTEX_TRY:
			rc = pthread_mutex_unlock(&mtn->data.mutex);
			break;

		case TKVDB_MTN_SPINLOCK:
		case TKVDB_MTN_SPINLOCK_TRY:
			rc = pthread_spin_unlock(&mtn->data.spin);
			break;

		default:
			rc = 0;
			break;
	}

	return rc;
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

		case TKVDB_MTN_WAITFREE_SWMR:
			rc = mtn->tr->begin(mtn->tr);
			break;

		case TKVDB_MTN_WAITFREE_MWMR:
			/* return TKVDB_CORRUPTED */
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

		case TKVDB_MTN_WAITFREE_SWMR:
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

		case TKVDB_MTN_WAITFREE_MWMR:
			/* return TKVDB_CORRUPTED */
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

		case TKVDB_MTN_WAITFREE_SWMR:
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

		case TKVDB_MTN_WAITFREE_MWMR:
			/* return TKVDB_CORRUPTED */
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

		case TKVDB_MTN_WAITFREE_SWMR:
			rc = mtn->tr->put(mtn->tr, key, val);
			break;

		case TKVDB_MTN_WAITFREE_MWMR:
			/* return TKVDB_CORRUPTED */
			break;
	}

	return rc;
}

TKVDB_RES
tkvdb_mtn_get(tkvdb_mtn *mtn, const tkvdb_datum *key, tkvdb_datum *val)
{
	TKVDB_RES rc = TKVDB_CORRUPTED;
	int lr;
	tkvdb_tr *tr;
	struct mwmrdata *mwmr;

	switch (mtn->type) {
		LOCKED_CASE( mtn->tr->get(mtn->tr, key, val) , rc,
			&mtn->data.mutex, &mtn->data.spin, lr);

		case TKVDB_MTN_WAITFREE_SWMR:
			rc = mtn->tr->get(mtn->tr, key, val);
			break;

		case TKVDB_MTN_WAITFREE_MWMR:
			mwmr = &(mtn->data.mwmr);
			tr = mwmr->banks[(mwmr->bank_ptr + MWMR_BANK_UPDATED)
				% MWMR_BANKS];
			rc = tr->get(tr, key, val);
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

		case TKVDB_MTN_WAITFREE_SWMR:
			rc = mtn->tr->del(mtn->tr, key, del_pfx);
			break;

		case TKVDB_MTN_WAITFREE_MWMR:
			/* return TKVDB_CORRUPTED */
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

