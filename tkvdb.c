/*
 * tkvdb
 *
 * Copyright (c) 2016-2018, Vladimir Misyurov
 *
 * Permission to use, copy, modify, and/or distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH
 * REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY
 * AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT,
 * INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM
 * LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE
 * OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR
 * PERFORMANCE OF THIS SOFTWARE.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

#include "tkvdb.h"

#define TKVDB_SIGNATURE    "tkvdb003"

/* at the begin of each on-disk block there is a byte with type.
 * footer marked as removed is used in vacuum procedure */
#define TKVDB_BLOCKTYPE_TRANSACTION  0
#define TKVDB_BLOCKTYPE_FOOTER       1
#define TKVDB_BLOCKTYPE_RM_FOOTER    2

/* node properties */
#define TKVDB_NODE_VAL  (1 << 0)
#define TKVDB_NODE_META (1 << 1)
#define TKVDB_NODE_LEAF (1 << 2)

/* max number of subnodes we store as [symbols array] => [offsets array]
 * if number of subnodes is more than TKVDB_SUBNODES_THR, they stored on disk
 * as array of 256 offsets */
#define TKVDB_SUBNODES_THR (256 - 256 / sizeof(uint64_t))

/* read block size */
#define TKVDB_READ_SIZE 4096

/* FIXME: allocate stack dynamically */
#define TKVDB_STACK_MAX_DEPTH 128

/* helper macro for executing functions which returns TKVDB_RES */
#define TKVDB_EXEC(FUNC)                   \
do {                                       \
	TKVDB_RES r = FUNC;                \
	if (r != TKVDB_OK) {               \
		return r;                  \
	}                                  \
} while (0)

/* skip replaced nodes */
#define TKVDB_SKIP_RNODES(NODE)            \
while (NODE->c.replaced_by) {              \
	NODE = NODE->c.replaced_by;        \
} while (0)

/* replace node with updated one */
/* FIXME: (optional) memory barrier? */
#define TKVDB_REPLACE_NODE(NODE, NEWNODE)  \
do {                                       \
	NODE->c.replaced_by = NEWNODE;     \
} while (0)

struct tkvdb_params
{
	int flags;              /* db file flags (as passed to open()) */
	mode_t mode;            /* db file mode */

	size_t write_buf_limit; /* size of database write buffer */
	int write_buf_dynalloc; /* realloc buffer when needed */

	size_t tr_buf_limit;    /* size of transaction buffer */
	int tr_buf_dynalloc;    /* realloc transaction buffer when needed */

	int alignval;           /* val alignment */
};

/* packed structures */
#ifdef _WIN32
#pragma pack (push, packing)
#pragma pack (1)
#define PACKED
#else
#define PACKED __attribute__((packed))
#endif

/* on-disk transaction header */
struct tkvdb_tr_header
{
	uint8_t type;
	uint64_t footer_off;       /* pointer to footer */
} PACKED;

/* on-disk transaction footer */
struct tkvdb_tr_footer
{
	uint8_t type;
	uint8_t signature[8];
	uint64_t root_off;         /* offset of root node */
	uint64_t transaction_size; /* transaction size */
	uint64_t transaction_id;   /* transaction number */

	uint64_t gap_begin;
	uint64_t gap_end;
} PACKED;

#define TKVDB_TR_FTRSIZE (sizeof(struct tkvdb_tr_footer))

/* on-disk node */
struct tkvdb_disknode
{
	uint32_t size;        /* node size */
	uint8_t type;         /* type (has value or metadata) */
	uint16_t nsubnodes;   /* number of subnodes */
	uint32_t prefix_size; /* prefix size */

	uint8_t data[1];      /* variable size data */
} PACKED;

#ifdef _WIN32
#pragma pack(pop, packing)
#else
#undef PACKED
#endif

/* database file information */
struct tkvdb_db_info
{
	struct tkvdb_tr_footer footer;

	uint64_t filesize;
};

/* database */
struct tkvdb
{
	int fd;                     /* database file handle */
	struct tkvdb_db_info info;

	tkvdb_params params;        /* database params */

	uint8_t *write_buf;
	size_t write_buf_allocated;
};

/* transaction in memory */
typedef struct tkvdb_tr_data
{
	tkvdb *db;
	tkvdb_params params;

	void *root;

	int started;

	uint8_t *tr_buf;                /* transaction buffer */
	size_t tr_buf_allocated;
	uint8_t *tr_buf_ptr;
} tkvdb_tr_data;

struct tkvdb_visit_helper
{
	void *node;             /* pointer to memnode */
	int off;                /* index of subnode in node */
};

/* database cursor */
typedef struct tkvdb_cursor_data
{
	size_t stack_size;
	struct tkvdb_visit_helper stack[TKVDB_STACK_MAX_DEPTH];

	size_t prefix_size;
	unsigned char *prefix;

	size_t val_size;
	uint8_t *val;

	tkvdb_tr *tr;
} tkvdb_cursor_data;

/* get next subnode (or load from disk) */
#define TKVDB_SUBNODE_NEXT(TR, NODE, NEXT, OFF)                           \
do {                                                                      \
	tkvdb_tr_data *trd = TR->data;                                    \
	if (NODE->c.type & TKVDB_NODE_LEAF) {                             \
		break;                                                    \
	}                                                                 \
	if (NODE->next[OFF]) {                                            \
		NEXT = node->next[OFF];                                   \
	} else if (trd->db && NODE->fnext[OFF]) {                         \
		TKVDB_MEMNODE_TYPE *tmp;                                  \
		TKVDB_EXEC( TKVDB_IMPL_NODE_READ(TR, NODE->fnext[OFF],    \
			&tmp) );                                          \
		NODE->next[OFF] = tmp;                                    \
		NEXT = tmp;                                               \
	}                                                                 \
} while (0)

#define TKVDB_SUBNODE_SEARCH(TR, NODE, NEXT, OFF, INCR)                   \
do {                                                                      \
	int lim, step;                                                    \
	NEXT = NULL;                                                      \
	if (NODE->c.type & TKVDB_NODE_LEAF) {                             \
		break;                                                    \
	}                                                                 \
	if (INCR) {                                                       \
		lim = 256;                                                \
		step = 1;                                                 \
	} else {                                                          \
		lim = -1;                                                 \
		step = -1;                                                \
	}                                                                 \
	for (; OFF!=lim; OFF+=step) {                                     \
		TKVDB_SUBNODE_NEXT(TR, NODE, NEXT, OFF);                  \
		if (next) {                                               \
			break;                                            \
		}                                                         \
	}                                                                 \
} while (0)


static TKVDB_RES
tkvdb_info_read(const int fd, struct tkvdb_db_info *info)
{
	struct stat st;
	off_t footer_pos; /* position of footer in database file */
	ssize_t io_res;

	/* get file size */
	if (fstat(fd, &st) != 0) {
		return TKVDB_IO_ERROR;
	}

	info->filesize = st.st_size;

	if (info->filesize == 0) {
		/* empty file */
		return TKVDB_OK;
	}

	if (info->filesize <= (off_t)TKVDB_TR_FTRSIZE) {
		/* file is too small */
		return TKVDB_CORRUPTED;
	}

	/* seek to the end of file (e.g. footer of last transaction) */
	footer_pos = info->filesize - TKVDB_TR_FTRSIZE;
	if (lseek(fd, footer_pos, SEEK_SET) != footer_pos) {
		return TKVDB_IO_ERROR;
	}

	io_res = read(fd, &info->footer, TKVDB_TR_FTRSIZE);
	if (io_res < (ssize_t)TKVDB_TR_FTRSIZE) {
		/* read less than footer, assuming it's error */
		return TKVDB_IO_ERROR;
	}

	/* check signature */
	if ((memcmp(info->footer.signature, TKVDB_SIGNATURE,
		sizeof(TKVDB_SIGNATURE) - 1)) != 0) {

		return TKVDB_CORRUPTED;
	}

	if (info->footer.transaction_size > (uint64_t)footer_pos) {
		return TKVDB_CORRUPTED;
	}

	return TKVDB_OK;
}

/* fill tkvdb_params with default values */
void
tkvdb_params_init(tkvdb_params *params)
{
	params->write_buf_dynalloc = 1;
	params->write_buf_limit = SIZE_MAX;

	params->tr_buf_dynalloc = 1;
	params->tr_buf_limit = SIZE_MAX;

	params->mode = S_IRUSR | S_IWUSR;
#ifndef _WIN32
	params->flags = O_RDWR | O_CREAT;
#else
	params->flags = O_RDWR | O_CREAT | O_BINARY;
#endif

	params->alignval = 0;
}

/* open database file */
tkvdb *
tkvdb_open(const char *path, tkvdb_params *user_params)
{
	tkvdb *db;
	TKVDB_RES r;

	db = malloc(sizeof(tkvdb));
	if (!db) {
		goto fail;
	}

	if (user_params) {
		db->params = *user_params;
	} else {
		tkvdb_params_init(&db->params);
	}

	db->fd = open(path, db->params.flags, db->params.mode);
	if (db->fd < 0) {
		goto fail_free;
	}

	r = tkvdb_info_read(db->fd, &(db->info));
	if (r != TKVDB_OK) {
		/* error */
		goto fail_close;
	}

	/* init params */
	if (db->params.write_buf_dynalloc) {
		db->write_buf = NULL;
		db->write_buf_allocated = 0;
	} else {
		db->write_buf = malloc(db->params.write_buf_limit);
		if (!db->write_buf) {
			goto fail_close;
		}
		db->write_buf_allocated = db->params.write_buf_limit;
	}

	return db;

fail_close:
	close(db->fd);
fail_free:
	free(db);
fail:
	return NULL;
}

/* close database and free data */
TKVDB_RES
tkvdb_close(tkvdb *db)
{
	TKVDB_RES r = TKVDB_OK;

	if (!db) {
		return TKVDB_OK;
	}

	if (close(db->fd) < 0) {
		r = TKVDB_IO_ERROR;
	}
	if (db->write_buf) {
		free(db->write_buf);
	}

	free(db);
	return r;
}


tkvdb_params *
tkvdb_params_create(void)
{
	tkvdb_params *params;

	params = malloc(sizeof(tkvdb_params));
	if (!params) {
		return NULL;
	}

	tkvdb_params_init(params);

	return params;
}

void
tkvdb_params_free(tkvdb_params *params)
{
	free(params);
}

void
tkvdb_param_set(tkvdb_params *params, TKVDB_PARAM p, int64_t val)
{
	switch (p) {
		case TKVDB_PARAM_TR_DYNALLOC:
			params->tr_buf_dynalloc = (int)val;
			break;
		case TKVDB_PARAM_TR_LIMIT:
			params->tr_buf_limit = (size_t)val;
			break;
		case TKVDB_PARAM_ALIGNVAL:
			params->alignval = (int)val;
			break;
		default:
			break;
	}
}


/* cursors */
static void
tkvdb_cursor_free(tkvdb_cursor *c)
{
	tkvdb_cursor_data *cdata;

	cdata = c->data;
	if (cdata->prefix) {
		free(cdata->prefix);
		cdata->prefix = NULL;
	}
	cdata->prefix_size = 0;

	cdata->val_size = 0;
	cdata->val = NULL;

	cdata->stack_size = 0;

	free(cdata);
	c->data = NULL;
	free(c);
}


static void *
tkvdb_cursor_key(tkvdb_cursor *c)
{
	tkvdb_cursor_data *cdata = c->data;
	return cdata->prefix;
}

static size_t
tkvdb_cursor_keysize(tkvdb_cursor *c)
{
	tkvdb_cursor_data *cdata = c->data;
	return cdata->prefix_size;
}

static void *
tkvdb_cursor_val(tkvdb_cursor *c)
{
	tkvdb_cursor_data *cdata = c->data;
	return cdata->val;
}

static size_t
tkvdb_cursor_valsize(tkvdb_cursor *c)
{
	tkvdb_cursor_data *cdata = c->data;
	return cdata->val_size;
}

/* todo: rewrite */
static int
tkvdb_cursor_expand_prefix(tkvdb_cursor *c, int n)
{
	unsigned char *tmp_pfx;
	tkvdb_cursor_data *cdata = c->data;

	if (n == 0) {
		/* underflow */
		if (cdata->prefix) {
			free(cdata->prefix);
			cdata->prefix = NULL;
		}
		return TKVDB_OK;
	}

	/* empty key is ok */
	if ((cdata->prefix_size + n) == 0) {
		free(cdata->prefix);
		cdata->prefix = NULL;
		return TKVDB_OK;
	}

	tmp_pfx = realloc(cdata->prefix, cdata->prefix_size + n);
	if (!tmp_pfx) {
		free(cdata->prefix);
		cdata->prefix = NULL;
		return TKVDB_ENOMEM;
	}
	cdata->prefix = tmp_pfx;

	return TKVDB_OK;
}

static void
tkvdb_cursor_reset(tkvdb_cursor *c)
{
	tkvdb_cursor_data *cdata = c->data;

	cdata->stack_size = 0;
	if (cdata->prefix_size && cdata->prefix) {
		free(cdata->prefix);
		cdata->prefix = NULL;
	}
	cdata->prefix_size = 0;

	cdata->val_size = 0;
	cdata->val = NULL;
}

/* reallocate transaction write buffer */
static TKVDB_RES
tkvdb_writebuf_realloc(tkvdb *db, size_t new_size)
{
	if (new_size > db->params.write_buf_limit) {
		/* not enough space for node in buffer */
		return TKVDB_ENOMEM;
	}
	if (new_size > db->write_buf_allocated) {
		uint8_t *tmp;

		if (!db->params.write_buf_dynalloc) {
			return TKVDB_ENOMEM;
		}

		tmp = realloc(db->write_buf, new_size);
		if (!tmp) {
			return TKVDB_ENOMEM;
		}

		db->write_buf = tmp;
		db->write_buf_allocated = new_size;
	}

	return TKVDB_OK;
}

/* generated implementation of tkvdb_* functions () */
#include "tkvdb_generated.inc"

TKVDB_RES
tkvdb_dbinfo(tkvdb *db, uint64_t *root_off,
	uint64_t *gap_begin, uint64_t *gap_end)
{
	struct tkvdb_db_info info;

	TKVDB_EXEC( tkvdb_info_read(db->fd, &info) );

	*root_off = info.footer.root_off;

	*gap_begin = info.footer.gap_begin;
	*gap_end = info.footer.gap_end;

	return TKVDB_OK;
}

static TKVDB_RES
tkvdb_begin(tkvdb_tr *trns)
{
	tkvdb_tr_data *tr = trns->data;

	if (tr->started) {
		/* ignore if transaction is already started */
		return TKVDB_OK;
	}

	if (!tr->db) {
		/* no underlying database file */
		tr->started = 1;
		return TKVDB_OK;
	}

	/* read database info to find root node */
	TKVDB_EXEC( tkvdb_info_read(tr->db->fd, &(tr->db->info)) );

	if (tr->db->info.filesize == 0) {
		memset(&(tr->db->info.footer),
			0, sizeof(struct tkvdb_tr_footer));
	} else {
		/* increase transaction number */
		tr->db->info.footer.transaction_id += 1;
	}

	tr->started = 1;

	return TKVDB_OK;
}

static size_t
tkvdb_tr_mem(tkvdb_tr *trns)
{
	tkvdb_tr_data *tr = trns->data;
	return tr->tr_buf_allocated;
}

tkvdb_tr *
tkvdb_tr_create(tkvdb *db, tkvdb_params *user_params)
{
	tkvdb_tr *tr;
	tkvdb_tr_data *trdata;

	tr = malloc(sizeof(tkvdb_tr));
	if (!tr) {
		goto fail_tr;
	}

	trdata = malloc(sizeof(tkvdb_tr_data));
	if (!tr) {
		goto fail_trdata;
	}

	tr->data = trdata;

	trdata->db = db;
	trdata->root = NULL;

	trdata->started = 0;

	/* setup params */
	if (user_params) {
		trdata->params = *user_params;
	} else {
		if (db) {
			trdata->params = db->params;
		} else {
			tkvdb_params_init(&(trdata->params));
		}
	}

	if (!trdata->params.tr_buf_dynalloc) {
		trdata->tr_buf = malloc(trdata->params.tr_buf_limit);
		if (!trdata->tr_buf) {
			goto fail_buf;
		}
		trdata->tr_buf_ptr = trdata->tr_buf;
	} else {
		trdata->tr_buf = NULL;
		trdata->tr_buf_ptr = NULL;
	}
	trdata->tr_buf_allocated = 0;

	/* setup functions */
	tr->begin = &tkvdb_begin;
	tr->mem = &tkvdb_tr_mem;

	if (trdata->params.alignval) {
		tr->commit = &tkvdb_commit_alignval;
		tr->rollback = &tkvdb_rollback_alignval;

		tr->put = &tkvdb_put_alignval;
		tr->get = &tkvdb_get_alignval;
		tr->del = &tkvdb_del_alignval;

		tr->free = &tkvdb_tr_free_alignval;
	} else {
		tr->commit = &tkvdb_commit_generic;
		tr->rollback = &tkvdb_rollback_generic;

		tr->put = &tkvdb_put_generic;
		tr->get = &tkvdb_get_generic;
		tr->del = &tkvdb_del_generic;

		tr->free = &tkvdb_tr_free_generic;
	}

	return tr;

	/* errors */
fail_buf:
	free(trdata);
fail_trdata:
	free(tr);
fail_tr:
	return NULL;
}

tkvdb_cursor *
tkvdb_cursor_create(tkvdb_tr *tr)
{
	tkvdb_cursor *c;
	tkvdb_cursor_data *cdata;
	tkvdb_tr_data *trdata = tr->data;

	c = malloc(sizeof(tkvdb_cursor));
	if (!c) {
		return NULL;
	}

	cdata = malloc(sizeof(tkvdb_cursor_data));
	if (!cdata) {
		free(c);
		return NULL;
	}

	c->data = cdata;

	cdata->stack_size = 0;

	cdata->prefix_size = 0;
	cdata->prefix = NULL;

	cdata->val_size = 0;
	cdata->val = NULL;

	cdata->tr = tr;

	/* cursor functions */
	c->key = &tkvdb_cursor_key;
	c->keysize = &tkvdb_cursor_keysize;

	c->val = &tkvdb_cursor_val;
	c->valsize = &tkvdb_cursor_valsize;

	c->free = &tkvdb_cursor_free;

	if (trdata->params.alignval) {
		c->seek = &tkvdb_seek_alignval;
		c->first = &tkvdb_first_alignval;
		c->last = &tkvdb_last_alignval;

		c->next = &tkvdb_next_alignval;
		c->prev = &tkvdb_prev_alignval;
	} else {
		c->seek = &tkvdb_seek_generic;
		c->first = &tkvdb_first_generic;
		c->last = &tkvdb_last_generic;

		c->next = &tkvdb_next_generic;
		c->prev = &tkvdb_prev_generic;
	}


	return c;
}

