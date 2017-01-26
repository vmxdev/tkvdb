#ifndef tkvdb_h_included
#define tkvdb_h_included

#include <limits.h>

typedef struct tkvdb tkvdb;
typedef struct tkvdb_params tkvdb_params;

typedef struct tkvdb_tr tkvdb_tr;
typedef struct tkvdb_cursor tkvdb_cursor;

typedef enum TKVDB_RES
{
	TKVDB_OK = 0,
	TKVDB_ERROR,
	TKVDB_EMPTY,
	TKVDB_ENOMEM,
	TKVDB_CORRUPTED
} TKVDB_RES;

typedef enum TKVDB_MNTYPE
{
	TKVDB_KEY,
	TKVDB_KEYVAL,
	TKVDB_REPLACED
} TKVDB_MNTYPE;


tkvdb    *tkvdb_open(const char *path, tkvdb_params *params);
TKVDB_RES tkvdb_close(tkvdb *db);

/* in-memory transaction */
tkvdb_tr *tkvdb_tr_create(tkvdb *db);
TKVDB_RES tkvdb_tr_free(tkvdb_tr *tr);

TKVDB_RES tkvdb_begin(tkvdb_tr *tr);
TKVDB_RES tkvdb_commit(tkvdb_tr *tr);
TKVDB_RES tkvdb_rollback(tkvdb_tr *tr);

TKVDB_RES tkvdb_put(tkvdb_tr *tr, const void *key, size_t klen,
	const void *val, size_t vlen);
TKVDB_RES tkvdb_del(tkvdb_tr *tr, const void *key, size_t klen);
TKVDB_RES tkvdb_get(tkvdb_tr *tr, const void *key, size_t klen,
	const void **val, size_t *vlen);

/* cursors manipulations */
tkvdb_cursor *tkvdb_cursor_create();
TKVDB_RES tkvdb_cursor_free(tkvdb_cursor *c);

void *tkvdb_cursor_key(tkvdb_cursor *c);
size_t tkvdb_cursor_keysize(tkvdb_cursor *c);

TKVDB_RES tkvdb_seek(tkvdb_cursor *c, tkvdb_tr *tr,
	const void *key, size_t klen);
TKVDB_RES tkvdb_first(tkvdb_cursor *c, tkvdb_tr *tr);
TKVDB_RES tkvdb_last(tkvdb_cursor *c, tkvdb_tr *tr);
TKVDB_RES tkvdb_next(tkvdb_cursor *c);
TKVDB_RES tkvdb_prev(tkvdb_cursor *c);

#endif

