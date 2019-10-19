#ifndef tkvdb_h_included
#define tkvdb_h_included

#include <limits.h>
#include <stdint.h>
#include <stddef.h>

typedef struct tkvdb tkvdb;
typedef struct tkvdb_params tkvdb_params;
typedef struct tkvdb_triggers tkvdb_triggers;

typedef enum TKVDB_RES
{
	TKVDB_OK = 0,
	TKVDB_IO_ERROR,
	TKVDB_LOCKED,
	TKVDB_EMPTY,
	TKVDB_NOT_FOUND,
	TKVDB_ENOMEM,
	TKVDB_CORRUPTED,
	TKVDB_NOT_STARTED,
	TKVDB_MODIFIED
} TKVDB_RES;

typedef enum TKVDB_SEEK
{
	TKVDB_SEEK_EQ,
	TKVDB_SEEK_LE,
	TKVDB_SEEK_GE
} TKVDB_SEEK;

/* database (or transaction) parameters */
typedef enum TKVDB_PARAM
{
	/* dynamically allocate space for nodes (using system malloc() for
	   each node) */
	TKVDB_PARAM_TR_DYNALLOC,

	/* transaction size limit, default SIZE_MAX, e.g. no limit */
	TKVDB_PARAM_TR_LIMIT,

	/* align value in memory (0 or 1 for none), must be power of two */
	TKVDB_PARAM_ALIGNVAL,

	/* automatically start transaction after commit() and rollback(),
	   transaction created in started state, begin() is ignored */
	TKVDB_PARAM_AUTOBEGIN,

	/* dynamically allocate space for cursors stacks, default 1 */
	TKVDB_PARAM_CURSOR_STACK_DYNALLOC,

	/* cursors stacks size limit, default SIZE_MAX */
	TKVDB_PARAM_CURSOR_STACK_LIMIT,

	/* dynamically allocate space for cursor key, default 1 */
	TKVDB_PARAM_CURSOR_KEY_DYNALLOC,

	/* cursor key size limit, default SIZE_MAX */
	TKVDB_PARAM_CURSOR_KEY_LIMIT,

	/* flags passed to open() function */
	TKVDB_PARAM_DBFILE_OPEN_FLAGS
} TKVDB_PARAM;

typedef struct tkvdb_datum
{
	void *data;
	size_t size;
} tkvdb_datum;

typedef struct tkvdb_tr tkvdb_tr;

struct tkvdb_tr
{
	TKVDB_RES (*begin)(tkvdb_tr *tr);
	TKVDB_RES (*commit)(tkvdb_tr *tr);
	TKVDB_RES (*rollback)(tkvdb_tr *tr);

	TKVDB_RES (*put)(tkvdb_tr *tr,
		const tkvdb_datum *key, const tkvdb_datum *val);
	TKVDB_RES (*get)(tkvdb_tr *tr,
		const tkvdb_datum *key, tkvdb_datum *val);
	TKVDB_RES (*del)(tkvdb_tr *tr, const tkvdb_datum *key, int del_pfx);

	/* get size of memory used by transaction */
	size_t (*mem)(tkvdb_tr *tr);

	void (*free)(tkvdb_tr *tr);

	void *data;

	TKVDB_RES (*putx)(tkvdb_tr *tr,
		const tkvdb_datum *key, const tkvdb_datum *val,
		tkvdb_triggers *triggers);
	TKVDB_RES (*getx)(tkvdb_tr *tr,
		const tkvdb_datum *key, tkvdb_datum *val,
		const tkvdb_triggers *triggers);
};

typedef struct tkvdb_cursor tkvdb_cursor;
struct tkvdb_cursor
{
	void *(*key)(tkvdb_cursor *c);
	size_t (*keysize)(tkvdb_cursor *c);

	void *(*val)(tkvdb_cursor *c);
	size_t (*valsize)(tkvdb_cursor *c);

	TKVDB_RES (*seek)(tkvdb_cursor *c,
		const tkvdb_datum *key, TKVDB_SEEK seek);
	TKVDB_RES (*first)(tkvdb_cursor *c);
	TKVDB_RES (*last)(tkvdb_cursor *c);

	TKVDB_RES (*next)(tkvdb_cursor *c);
	TKVDB_RES (*prev)(tkvdb_cursor *c);

	void (*free)(tkvdb_cursor *c);

	void *data;
};

/* triggers */
typedef struct tkvdb_trigger_stack
{
	size_t size;
	struct valmeta
	{
		tkvdb_datum val, meta;
	} *valmeta;
} tkvdb_trigger_stack;

typedef TKVDB_RES (*tkvdb_trigger_func)(tkvdb_tr *tr, const tkvdb_datum *key,
	const tkvdb_trigger_stack *stack, void *userdata);

typedef size_t (*tkvdb_trigger_size_func)(const tkvdb_datum *key,
	const tkvdb_datum *val, void *userdata);

typedef struct tkvdb_trigger_set
{
	tkvdb_trigger_func before_insert;
	tkvdb_trigger_func before_update;

	tkvdb_trigger_size_func meta_size;
} tkvdb_trigger_set;


#ifdef __cplusplus
extern "C" {
#endif

/* allocate and fill db params with default values */
tkvdb_params *tkvdb_params_create(void);
/* change parameter */
void tkvdb_param_set(tkvdb_params *params, TKVDB_PARAM p, int64_t val);
/* free */
void tkvdb_params_free(tkvdb_params *params);

/* database */
tkvdb    *tkvdb_open(const char *path, tkvdb_params *params);
TKVDB_RES tkvdb_close(tkvdb *db);
/* fsync() db file */
TKVDB_RES tkvdb_sync(tkvdb *db);

/* in-memory transaction */
tkvdb_tr *tkvdb_tr_create(tkvdb *db, tkvdb_params *params);

/* cursors */
tkvdb_cursor *tkvdb_cursor_create(tkvdb_tr *tr);

/* vacuum */
TKVDB_RES tkvdb_vacuum(tkvdb_tr *tr, tkvdb_tr *vac, tkvdb_tr *tres,
	tkvdb_cursor *c);
/* get database file information */
TKVDB_RES tkvdb_dbinfo(tkvdb *db, uint64_t *root_off,
	uint64_t *gap_begin, uint64_t *gap_end);


/* triggers */
tkvdb_triggers *tkvdb_triggers_create(size_t stack_size, void *userdata);
void tkvdb_triggers_free(tkvdb_triggers *triggers);

int tkvdb_triggers_add_set(tkvdb_triggers *triggers,
	const tkvdb_trigger_set *trigger_set);

#ifdef __cplusplus
}
#endif

#endif

