#ifndef tkvdb_mtn_h_included
#define tkvdb_mtn_h_included

#include "tkvdb.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef enum TKVDB_MTN_TYPE
{
	TKVDB_MTN_MUTEX,
	TKVDB_MTN_MUTEX_TRY,
	TKVDB_MTN_SPINLOCK,
	TKVDB_MTN_SPINLOCK_TRY,
	TKVDB_MTN_WAITFREE_SWMR,
	TKVDB_MTN_WAITFREE_MWMR
} TKVDB_MTN_TYPE;

typedef struct tkvdb_mtn tkvdb_mtn;
typedef struct tkvdb_mtn_cursor tkvdb_mtn_cursor;

typedef void (*mwmr_aggr)(const void *a, const void *b);

tkvdb_mtn *tkvdb_mtn_create_locked(tkvdb_tr *tr, TKVDB_MTN_TYPE type);
tkvdb_mtn *tkvdb_mtn_create_spmc(tkvdb_tr *tr1, tkvdb_tr *tr2);
tkvdb_mtn *tkvdb_mtn_create_mwmr(tkvdb_tr *tr1, tkvdb_tr *tr2, tkvdb_tr *tr3,
	mwmr_aggr aggr_func, int ns_sleep);

void tkvdb_mtn_free(tkvdb_mtn *mtn);

TKVDB_RES tkvdb_mtn_begin(tkvdb_mtn *mtn);
TKVDB_RES tkvdb_mtn_commit(tkvdb_mtn *mtn);
TKVDB_RES tkvdb_mtn_rollback(tkvdb_mtn *mtn);

TKVDB_RES tkvdb_mtn_put(tkvdb_mtn *mtn,
	const tkvdb_datum *key, const tkvdb_datum *val);

TKVDB_RES tkvdb_mtn_get(tkvdb_mtn *mtn,
	const tkvdb_datum *key, tkvdb_datum *val);

TKVDB_RES tkvdb_mtn_del(tkvdb_mtn *mtn, const tkvdb_datum *key, int del_pfx);

/* lock and unlock transaction */
int tkvdb_mtn_lock(tkvdb_mtn *mtn);
int tkvdb_mtn_unlock(tkvdb_mtn *mtn);

/* cursors */
tkvdb_mtn_cursor *tkvdb_mtn_cursor_create(tkvdb_mtn *mtn);
void *tkvdb_mtn_cursor_key(tkvdb_mtn_cursor *c);
size_t tkvdb_mtn_cursor_keysize(tkvdb_mtn_cursor *c);

void *tkvdb_mtn_cursor_val(tkvdb_mtn_cursor *c);
size_t tkvdb_mtn_cursor_valsize(tkvdb_mtn_cursor *c);

TKVDB_RES tkvdb_mtn_cursor_seek(tkvdb_mtn_cursor *c, const tkvdb_datum *key,
	TKVDB_SEEK seek);
TKVDB_RES tkvdb_mtn_cursor_first(tkvdb_mtn_cursor *c);
TKVDB_RES tkvdb_mtn_cursor_last(tkvdb_mtn_cursor *c);

TKVDB_RES tkvdb_mtn_cursor_next(tkvdb_mtn_cursor *c);
TKVDB_RES tkvdb_mtn_cursor_prev(tkvdb_mtn_cursor *c);

void tkvdb_mtn_cursor_free(tkvdb_mtn_cursor *c);

/* multiple writers/multiple readers */
int tkvdb_mtn_mwmr_add(tkvdb_mtn *mwmr, tkvdb_mtn *mtn);

#ifdef __cplusplus
}
#endif

#endif

