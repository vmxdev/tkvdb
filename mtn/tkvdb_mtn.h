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
	TKVDB_MTN_WAITFREE_SPMC
} TKVDB_MTN_TYPE;

typedef struct tkvdb_mtn tkvdb_mtn;

tkvdb_mtn *tkvdb_mtn_create_locked(tkvdb_tr *tr, TKVDB_MTN_TYPE type);
tkvdb_mtn *tkvdb_mtn_create_spmc(tkvdb_tr *tr1, tkvdb_tr *tr2);
void tkvdb_mtn_free(tkvdb_mtn *mtn);

TKVDB_RES tkvdb_mtn_begin(tkvdb_mtn *mtn);
TKVDB_RES tkvdb_mtn_commit(tkvdb_mtn *mtn);
TKVDB_RES tkvdb_mtn_rollback(tkvdb_mtn *mtn);

TKVDB_RES tkvdb_mtn_put(tkvdb_mtn *mtn,
	const tkvdb_datum *key, const tkvdb_datum *val);

TKVDB_RES tkvdb_mtn_get(tkvdb_mtn *mtn,
	const tkvdb_datum *key, tkvdb_datum *val);

TKVDB_RES tkvdb_mtn_del(tkvdb_mtn *mtn, const tkvdb_datum *key, int del_pfx);


#ifdef __cplusplus
}
#endif

#endif

