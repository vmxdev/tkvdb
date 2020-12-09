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

	/* triggers and metadata */
	TKVDB_RES (*putx)(tkvdb_tr *tr,
		const tkvdb_datum *key, const tkvdb_datum *val,
		tkvdb_triggers *triggers);

	TKVDB_RES (*delx)(tkvdb_tr *tr, const tkvdb_datum *key, int del_pfx,
		tkvdb_triggers *triggers);

	TKVDB_RES (*subnode)(tkvdb_tr *tr, void *node, int n, void **subnode,
		tkvdb_datum *prefix, tkvdb_datum *val, tkvdb_datum *meta);
};

typedef struct tkvdb_cursor tkvdb_cursor;
struct tkvdb_cursor
{
	void *(*key)(tkvdb_cursor *c);
	size_t (*keysize)(tkvdb_cursor *c);

	void *(*val)(tkvdb_cursor *c);
	size_t (*valsize)(tkvdb_cursor *c);

	/* alternate way to get key/value */
	tkvdb_datum (*key_datum)(tkvdb_cursor *c);
	tkvdb_datum (*val_datum)(tkvdb_cursor *c);

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

/* types of modification */
enum TKVDB_TRIGGER_MOD_TYPE
{
	TKVDB_TRIGGER_UPDATE,
	TKVDB_TRIGGER_INSERT_NEWROOT,
	TKVDB_TRIGGER_INSERT_SUBKEY,
	TKVDB_TRIGGER_INSERT_NEWNODE,
	TKVDB_TRIGGER_INSERT_SHORTER,
	TKVDB_TRIGGER_INSERT_LONGER,
	TKVDB_TRIGGER_INSERT_SPLIT,
	TKVDB_TRIGGER_DELETE_ROOT,
	TKVDB_TRIGGER_DELETE_PREFIX,
	TKVDB_TRIGGER_DELETE_LEAF,
	TKVDB_TRIGGER_DELETE_INTNODE
};

typedef struct tkvdb_trigger_stack
{
	size_t size, limit;
	void **meta;
} tkvdb_trigger_stack;

typedef struct tkvdb_trigger_info
{
	tkvdb_trigger_stack *stack;

	enum TKVDB_TRIGGER_MOD_TYPE type;
	void *newroot, *subnode1, *subnode2;

	void *userdata;
} tkvdb_trigger_info;


typedef TKVDB_RES (*tkvdb_trigger_func)(tkvdb_trigger_info *info);

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
tkvdb_triggers *tkvdb_triggers_create(size_t stack_limit);
void tkvdb_triggers_free(tkvdb_triggers *triggers);

TKVDB_RES tkvdb_triggers_add(tkvdb_triggers *triggers, tkvdb_trigger_func t,
	size_t meta_size, void *userdata);

#ifdef __cplusplus
}
#endif

#endif

