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
	TKVDB_IO_ERROR,
	TKVDB_LOCKED,
	TKVDB_EMPTY,
	TKVDB_NOT_FOUND,
	TKVDB_ENOMEM,
	TKVDB_CORRUPTED,
	TKVDB_NOT_STARTED
} TKVDB_RES;

typedef enum TKVDB_SEEK
{
	TKVDB_SEEK_EQ,
	TKVDB_SEEK_LE,
	TKVDB_SEEK_GE
} TKVDB_SEEK;

#ifdef __cplusplus
extern "C" {
#endif

/* fill db params with default values */
void tkvdb_params_init(tkvdb_params *params);

tkvdb    *tkvdb_open(const char *path, tkvdb_params *params);
TKVDB_RES tkvdb_close(tkvdb *db);

/* in-memory transaction */
tkvdb_tr *tkvdb_tr_create(tkvdb *db);
/* create transaction with custom memory allocation parameters */
tkvdb_tr *tkvdb_tr_create_m(tkvdb *db, size_t limit, int dynalloc);
void tkvdb_tr_free(tkvdb_tr *tr);

TKVDB_RES tkvdb_begin(tkvdb_tr *tr);
TKVDB_RES tkvdb_commit(tkvdb_tr *tr);
TKVDB_RES tkvdb_rollback(tkvdb_tr *tr);

/* fsync() db file */
TKVDB_RES tkvdb_sync(tkvdb *db);

TKVDB_RES tkvdb_put(tkvdb_tr *tr, const void *key, size_t klen,
	const void *val, size_t vlen);
TKVDB_RES tkvdb_del(tkvdb_tr *tr, const void *key, size_t klen, int del_pfx);
TKVDB_RES tkvdb_get(tkvdb_tr *tr, const void *key, size_t klen,
	void **val, size_t *vlen);

/* cursors manipulations */
tkvdb_cursor *tkvdb_cursor_create(tkvdb_tr *tr);
TKVDB_RES tkvdb_cursor_free(tkvdb_cursor *c);

void *tkvdb_cursor_key(tkvdb_cursor *c);
size_t tkvdb_cursor_keysize(tkvdb_cursor *c);

void *tkvdb_cursor_val(tkvdb_cursor *c);
size_t tkvdb_cursor_valsize(tkvdb_cursor *c);

TKVDB_RES tkvdb_seek(tkvdb_cursor *c, const void *key, size_t klen,
	TKVDB_SEEK seek);
TKVDB_RES tkvdb_first(tkvdb_cursor *c);
TKVDB_RES tkvdb_last(tkvdb_cursor *c);

TKVDB_RES tkvdb_next(tkvdb_cursor *c);
TKVDB_RES tkvdb_prev(tkvdb_cursor *c);

#ifdef __cplusplus
}
#endif

#endif

