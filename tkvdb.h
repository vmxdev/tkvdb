#ifndef tkvdb_h_included
#define tkvdb_h_included

#include <limits.h>

typedef struct tkvdb tkvdb;
typedef struct tkvdb_params tkvdb_params;

typedef struct tkvdb_memtr tkvdb_memtr;
typedef struct tkvdb_cursor tkvdb_cursor;

typedef enum TKVDB_RES
{
	TKVDB_OK = 0,
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


/* open (or create) database */
tkvdb    *tkvdb_open(const char *path, tkvdb_params *params);
TKVDB_RES tkvdb_close(tkvdb *db);

/* begin (in-memory) transaction */
TKVDB_RES tkvdb_begin(tkvdb *db, tkvdb_memtr **tr);
TKVDB_RES tkvdb_commit(tkvdb_memtr *tr);
TKVDB_RES tkvdb_rollback(tkvdb_memtr *tr);
TKVDB_RES tkvdb_transaction_free(tkvdb_memtr *tr);

/* add key-value pair to memory transaction */
TKVDB_RES tkvdb_put(tkvdb_memtr *tr,
	const void *key, size_t klen, const void *val, size_t vlen);

/* cursors */
tkvdb_cursor *tkvdb_cursor_new();
void tkvdb_cursor_close(tkvdb_cursor *c);

void *tkvdb_cursor_key(tkvdb_cursor *c);
size_t tkvdb_cursor_keysize(tkvdb_cursor *c);

int tkvdb_first(tkvdb_cursor *c, tkvdb_memtr *tr);
int tkvdb_next(tkvdb_cursor *c);

#endif

