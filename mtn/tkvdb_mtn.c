#include <stdlib.h>
#include <pthread.h>
#include <errno.h>

#include "tkvdb_mtn.h"


struct tkvdb_mtn
{
	TKVDB_MTN_TYPE type;

	union lock_t
	{
		pthread_mutex_t mutex;
		pthread_spinlock_t spin;
	} lock;

	tkvdb_tr *tr;
};

tkvdb_mtn *
tkvdb_mtn_create(tkvdb_tr *tr, TKVDB_MTN_TYPE type)
{
	tkvdb_mtn *mtn;

	mtn = malloc(sizeof(tkvdb_mtn));
	if (!mtn) {
		goto fail_alloc;
	}

	switch (type) {
		case TKVDB_MTN_MUTEX:
		case TKVDB_MTN_MUTEX_TRY:
			if (pthread_mutex_init(&mtn->lock.mutex, NULL) != 0) {
				goto fail_lock;
			}
			break;
		case TKVDB_MTN_SPINLOCK:
		case TKVDB_MTN_SPINLOCK_TRY:
			if (pthread_spin_init(&mtn->lock.spin, 0) != 0) {
				goto fail_lock;
			}
			break;
	}

	mtn->type = type;
	mtn->tr = tr;

	return mtn;

fail_lock:
	free(mtn);
fail_alloc:
	return NULL;
}


void
tkvdb_mtn_free(tkvdb_mtn *mtn)
{
	switch (mtn->type) {
		case TKVDB_MTN_MUTEX:
		case TKVDB_MTN_MUTEX_TRY:
			pthread_mutex_destroy(&mtn->lock.mutex);
			break;
		case TKVDB_MTN_SPINLOCK:
		case TKVDB_MTN_SPINLOCK_TRY:
			pthread_spin_destroy(&mtn->lock.spin);
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
	TKVDB_RES rc;
	int lr;

	switch (mtn->type) {
		LOCKED_CASE( mtn->tr->begin(mtn->tr) , rc,
			&mtn->lock.mutex, &mtn->lock.spin, lr);
/*
		case TKVDB_MTN_MUTEX:
			pthread_mutex_lock(&mtn->lock.mutex);
			rc = mtn->tr->begin(mtn->tr);
			pthread_mutex_unlock(&mtn->lock.mutex);
			break;

		case TKVDB_MTN_MUTEX_TRY:
			lr = pthread_mutex_trylock(&mtn->lock.mutex);
			if (lr == EBUSY) {
				return TKVDB_LOCKED;
			}
			rc = mtn->tr->begin(mtn->tr);
			pthread_mutex_unlock(&mtn->lock.mutex);
			break;

		case TKVDB_MTN_SPINLOCK:
			pthread_spin_lock(&mtn->lock.spin);
			rc = mtn->tr->begin(mtn->tr);
			pthread_spin_unlock(&mtn->lock.spin);
			break;

		case TKVDB_MTN_SPINLOCK_TRY:
			lr = pthread_spin_trylock(&mtn->lock.spin);
			if (lr == EBUSY) {
				return TKVDB_LOCKED;
			}
			rc = mtn->tr->begin(mtn->tr);
			pthread_spin_unlock(&mtn->lock.spin);
			break;
*/
	}

	return rc;
}

TKVDB_RES
tkvdb_mtn_commit(tkvdb_mtn *mtn)
{
	TKVDB_RES rc;
	int lr;

	switch (mtn->type) {
		LOCKED_CASE( mtn->tr->commit(mtn->tr) , rc,
			&mtn->lock.mutex, &mtn->lock.spin, lr);
	}

	return rc;
}

TKVDB_RES
tkvdb_mtn_rollback(tkvdb_mtn *mtn)
{
	TKVDB_RES rc;
	int lr;

	switch (mtn->type) {
		LOCKED_CASE( mtn->tr->rollback(mtn->tr) , rc,
			&mtn->lock.mutex, &mtn->lock.spin, lr);
	}

	return rc;
}

TKVDB_RES
tkvdb_mtn_put(tkvdb_mtn *mtn, const tkvdb_datum *key, const tkvdb_datum *val)
{
	TKVDB_RES rc;
	int lr;

	switch (mtn->type) {
		LOCKED_CASE( mtn->tr->put(mtn->tr, key, val) , rc,
			&mtn->lock.mutex, &mtn->lock.spin, lr);
	}

	return rc;
}

TKVDB_RES
tkvdb_mtn_get(tkvdb_mtn *mtn, const tkvdb_datum *key, tkvdb_datum *val)
{
	TKVDB_RES rc;
	int lr;

	switch (mtn->type) {
		LOCKED_CASE( mtn->tr->get(mtn->tr, key, val) , rc,
			&mtn->lock.mutex, &mtn->lock.spin, lr);
	}

	return rc;
}

TKVDB_RES
tkvdb_mtn_del(tkvdb_mtn *mtn, const tkvdb_datum *key, int del_pfx)
{
	TKVDB_RES rc;
	int lr;

	switch (mtn->type) {
		LOCKED_CASE( mtn->tr->del(mtn->tr, key, del_pfx) , rc,
			&mtn->lock.mutex, &mtn->lock.spin, lr);
	}

	return rc;
}

