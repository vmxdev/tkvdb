/*
 * tkvdb
 *
 * Copyright (c) 2016-2017, Vladimir Misyurov

 * Permission to use, copy, modify, and/or distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.

 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH
 * REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY
 * AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT,
 * INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM
 * LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE
 * OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR
 * PERFORMANCE OF THIS SOFTWARE.
 */

#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

#include "tkvdb.h"

#define TKVDB_STRVER "001"

#define TKVDB_SIGNATURE    "tkvdb" TKVDB_STRVER
#define TKVDB_TR_SIGNATURE "tkvtr" TKVDB_STRVER

/* node properties */
#define TKVDB_NODE_VAL  (1 << 0)
#define TKVDB_NODE_META (1 << 1)
#define TKVDB_NODE_REPLACED (1 << 2)

/* max number of subnodes we store as [symbols array] => [offsets array]
 * if number of subnodes is more than TKVDB_SUBNODES_THR, they stored on disk
 * as array of 256 offsets */
#define TKVDB_SUBNODES_THR (256 - 256 / sizeof(uint64_t))

struct tkvdb_params
{
	size_t tr_limit;        /* transaction max size */
	int tr_dynalloc;

	int flags;              /* db file flags (as passed to open()) */
	mode_t mode;            /* db file mode */

	size_t write_buf_limit; /* size of database write buffer */
	int write_buf_dynalloc; /* realloc buffer when needed */

	uint64_t part_size;     /* partition size (TODO: TBW) */
};

/* database */
struct tkvdb
{
	int fd;               /* database file handle */

	tkvdb_params params;  /* database params */

	uint8_t *write_buf;
	size_t write_buf_allocated;
};

/* on-disk database header */
struct tkvdb_header
{
	char signature[8];
	uint64_t root;

	uint64_t gap_begin;
	uint64_t gap_end;

	/* number of currently running transactions
	 * increased in tkvdb_begin(), decreased after commit/rollback */
	uint32_t running_transactions;
	uint32_t locked;                 /* something is commited right now */
} __attribute__((packed));

/* on-disk node */
struct tkvdb_disknode
{
	uint32_t size;        /* node size */
	uint8_t type;         /* type (has value or metadata) */
	uint8_t nsubnodes;    /* number of subnodes */
	uint32_t prefix_size; /* prefix size */

	uint8_t data[1];      /* variable size data */
} __attribute__((packed));

/* node in memory */
typedef struct tkvdb_memnode
{
	int type;
	size_t prefix_size;
	size_t val_size;
	size_t meta_size;

	uint64_t disk_size;               /* size of node on disk */
	uint64_t disk_off;                /* offset of node on disk */
	unsigned int nsubnodes;           /* number of subnodes */

	struct tkvdb_memnode *next[256];  /* subnodes in memory */
	uint64_t fnext[256];              /* positions of subnodes in file */

	unsigned char prefix_val_meta[1]; /* prefix, value and metadata */
} tkvdb_memnode;

/* transaction in memory */
struct tkvdb_tr
{
	tkvdb *db;
	tkvdb_memnode *root;
};

typedef struct tkvdb_visit_helper
{
	tkvdb_memnode *node;
	int off;                /* index of subnode in node */

} tkvdb_visit_helper;

/* database cursor */
struct tkvdb_cursor
{
	size_t stack_size;
	tkvdb_visit_helper *stack;

	size_t prefix_size;
	unsigned char *prefix;
};

/* fill tkvdb_params with default values */
void
tkvdb_params_init(tkvdb_params *params)
{
	params->write_buf_dynalloc = 1;
	params->write_buf_limit = SIZE_MAX;

	params->tr_dynalloc = 1;
	params->tr_limit = SIZE_MAX;

	params->flags = O_RDWR | O_CREAT;
	params->mode = S_IRUSR | S_IWUSR;
}

/* open database file */
tkvdb *
tkvdb_open(const char *path, tkvdb_params *user_params)
{
	tkvdb *db;
	struct tkvdb_header db_header;
	ssize_t io_res;

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

	io_res = read(db->fd, &db_header, sizeof(struct tkvdb_header));
	if (io_res == 0) {
		/* new file? */
		memset(&db_header, 0, sizeof(struct tkvdb_header));
		memcpy(&db_header.signature, TKVDB_SIGNATURE,
			sizeof(TKVDB_SIGNATURE) - 1);

		io_res = write(db->fd, &db_header,
			sizeof(struct tkvdb_header));
	}

	/* ok, assuming it's error */
	if (io_res < (ssize_t)sizeof(struct tkvdb_header)) {
		goto fail_close;
	}

	if (db->params.write_buf_dynalloc) {
		db->write_buf = NULL;
		db->write_buf_allocated = 0;
	} else {
		/* FIXME: calloc? */
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
	TKVDB_RES res = TKVDB_OK;

	if (close(db->fd) < 0) {
		res = TKVDB_ERROR;
	}

	free(db);
	return res;
}

/* create new node and append prefix and value */
static tkvdb_memnode *
tkvdb_node_new(int type, size_t prefix_size,
	const void *prefix, size_t vlen, const void *val)
{
	tkvdb_memnode *node;

	node = malloc(sizeof(tkvdb_memnode) + prefix_size + vlen);
	if (!node) {
		return NULL;
	}

	node->type = type;
	node->prefix_size = prefix_size;
	node->val_size = vlen;
	node->meta_size = 0;
	if (node->prefix_size > 0) {
		memcpy(node->prefix_val_meta, prefix, node->prefix_size);
	}
	if (node->val_size > 0) {
		memcpy(node->prefix_val_meta + node->prefix_size,
			val, node->val_size);
	}

	memset(node->next, 0, sizeof(tkvdb_memnode *) * 256);
	memset(node->fnext, 0, sizeof(uint64_t) * 256);

	node->disk_size = 0;
	node->disk_off = 0;

	return node;
}

static void
clone_subnodes(tkvdb_memnode *dst, tkvdb_memnode *src)
{
	memcpy(dst->next,  src->next, sizeof(tkvdb_memnode *) * 256);
	memcpy(dst->fnext, src->fnext, sizeof(uint64_t) * 256);
}

/* add key-value pair to memory transaction */
TKVDB_RES
tkvdb_put(tkvdb_tr *tr,
	const void *key, size_t klen, const void *val, size_t vlen)
{
	const unsigned char *sym;  /* pointer to current symbol in key */
	tkvdb_memnode *curr;       /* current node */
	size_t pi;                 /* prefix index */

	/* new root */
	if (tr->root == NULL) {
		tr->root = tkvdb_node_new(TKVDB_NODE_VAL, klen, key,
			vlen, val);
		if (!tr->root) {
			return TKVDB_ENOMEM;
		}

		return TKVDB_OK;
	}

	sym = key;
	curr = tr->root;

next_node:
	if (curr->type & TKVDB_NODE_REPLACED) {
		curr = curr->next[0];
		goto next_node;
	}
	pi = 0;

next_byte:

/* end of key
  ere we have two cases:
  [p][r][e][f][i][x] - prefix
  [p][r][e] - new key

  or exact match:
  [p][r][e][f][i][x] - prefix
  [p][r][e][f][i][x] - new key
*/
	if (sym >= ((unsigned char *)key + klen)) {
		tkvdb_memnode *newroot, *subnode_rest;

		if (pi == curr->prefix_size) {
			/* exact match */
			if ((curr->val_size == vlen) && (vlen != 0)) {
				/* same value size, so copy new value and
					return */
				memcpy(curr->prefix_val_meta
					+ curr->prefix_size, val, vlen);
				return TKVDB_OK;
			}

			newroot = tkvdb_node_new(TKVDB_NODE_VAL,
				pi, curr->prefix_val_meta, vlen, val);
			if (!newroot) return TKVDB_ENOMEM;

			clone_subnodes(newroot, curr);

			curr->type = TKVDB_NODE_REPLACED;
			curr->next[0] = newroot;

			return TKVDB_OK;
		}

/* split node with prefix
  [p][r][e][f][i][x] - prefix
  [p][r][e] - new key

  becomes
  [p][r][e] - new root
  next['f'] => [i][x] - tail
*/
		newroot = tkvdb_node_new(TKVDB_NODE_VAL, pi,
			curr->prefix_val_meta,
			vlen, val);
		if (!newroot) return TKVDB_ENOMEM;

		subnode_rest = tkvdb_node_new(curr->type,
			curr->prefix_size - pi - 1,
			curr->prefix_val_meta + pi + 1,
			curr->val_size,
			curr->prefix_val_meta + curr->prefix_size);

		if (!subnode_rest) {
			free(newroot);
			return TKVDB_ENOMEM;
		}
		clone_subnodes(subnode_rest, curr);

		newroot->next[curr->prefix_val_meta[pi]] = subnode_rest;
		curr->type = TKVDB_NODE_REPLACED;
		curr->next[0] = newroot;
		return TKVDB_OK;
	}

/* end of prefix
  [p][r][e][f][i][x] - old prefix
  [p][r][e][f][i][x][n][e][w]- new prefix

  so we hold old node and change only pointer to next
  [p][r][e][f][i][x]
  next['n'] => [e][w] - tail
*/
	if (pi >= curr->prefix_size) {
		if (curr->next[*sym] != NULL) {
			/* continue with next node */
			curr = curr->next[*sym];
			sym++;
			goto next_node;
		} else {
			tkvdb_memnode *tmp;

			/* allocate tail */
			tmp = tkvdb_node_new(TKVDB_NODE_VAL,
				klen - (sym - (unsigned char *)key) - 1,
				sym + 1,
				vlen, val);
			if (!tmp) return TKVDB_ENOMEM;

			curr->next[*sym] = tmp;
			return TKVDB_OK;
		}
	}

/* node prefix don't match with corresponding part of key
  [p][r][e][f][i][x] - old prefix
  [p][r][e][p][a][r][e]- new prefix

  [p][r][e] - new root
  next['f'] => [i][x] - tail from old prefix
  next['p'] => [a][r][e] - tail from new prefix
*/
	if (curr->prefix_val_meta[pi] != *sym) {
		tkvdb_memnode *newroot, *subnode_rest, *subnode_key;

		/* split current node into 3 subnodes */
		newroot = tkvdb_node_new(0, pi,
			curr->prefix_val_meta, 0, NULL);
		if (!newroot) return TKVDB_ENOMEM;

		/* rest of prefix (skip current symbol) */
		subnode_rest = tkvdb_node_new(curr->type,
			curr->prefix_size - pi - 1,
			curr->prefix_val_meta + pi + 1,
			curr->val_size,
			curr->prefix_val_meta + curr->prefix_size);
		if (!subnode_rest) {
			free(newroot);
			return TKVDB_ENOMEM;
		}
		clone_subnodes(subnode_rest, curr);

		/* rest of key */
		subnode_key = tkvdb_node_new(TKVDB_NODE_VAL,
			klen - (sym - (unsigned char *)key) - 1,
			sym + 1,
			vlen, val);
		if (!subnode_key) {
			free(subnode_rest);
			free(newroot);
			return TKVDB_ENOMEM;
		}

		newroot->next[curr->prefix_val_meta[pi]] = subnode_rest;
		newroot->next[*sym] = subnode_key;

		curr->type = TKVDB_NODE_REPLACED;
		curr->next[0] = newroot;
		return TKVDB_OK;
	}

	sym++;
	pi++;
	goto next_byte;

	return TKVDB_OK;
}

/* cursors */

tkvdb_cursor *
tkvdb_cursor_create()
{
	tkvdb_cursor *c;

	c = malloc(sizeof(tkvdb_cursor));
	if (!c) {
		return NULL;
	}

	c->stack_size = 0;
	c->stack = NULL;

	c->prefix_size = 0;
	c->prefix = NULL;

	return c;
}

TKVDB_RES
tkvdb_cursor_free(tkvdb_cursor *c)
{
	if (c->prefix) {
		free(c->prefix);
		c->prefix = NULL;
	}
	c->prefix_size = 0;

	if (c->stack) {
		free(c->stack);
		c->stack = NULL;
	}
	c->stack_size = 0;

	return TKVDB_OK;
}

static int
tkvdb_cursor_expand_prefix(tkvdb_cursor *c, int n)
{
	unsigned char *tmp_pfx;

	if (n == 0) {
		/* underflow */
		if (c->prefix) {
			free(c->prefix);
			c->prefix = NULL;
		}
		return TKVDB_OK;
	}

	/* empty key is ok */
	if ((c->prefix_size + n) == 0) {
		free(c->prefix);
		c->prefix = NULL;
		return TKVDB_OK;
	}

	tmp_pfx = realloc(c->prefix, c->prefix_size + n);
	if (!tmp_pfx) {
		free(c->prefix);
		c->prefix = NULL;
		return TKVDB_ENOMEM;
	}
	c->prefix = tmp_pfx;

	return TKVDB_OK;
}

/* add (push) node */
static int
tkvdb_cursor_push(tkvdb_cursor *c, tkvdb_memnode *node, int off)
{
	tkvdb_visit_helper *tmp_stack;

	tmp_stack = realloc(c->stack,
		(c->stack_size + 1) * sizeof(tkvdb_visit_helper));
	if (!tmp_stack) {
		free(c->stack);
		c->stack = NULL;
		return TKVDB_ENOMEM;
	}
	c->stack = tmp_stack;
	c->stack[c->stack_size].node = node;
	c->stack[c->stack_size].off = off;
	c->stack_size++;

	return TKVDB_OK;
}

/* pop node */
static int
tkvdb_cursor_pop(tkvdb_cursor *c)
{
	tkvdb_visit_helper *tmp_stack;
	int r;
	tkvdb_memnode *node;

	if (c->stack_size <= 1) {
		return TKVDB_EMPTY;
	}

	node = c->stack[c->stack_size - 1].node;

	/* erase prefix */
	if ((r = tkvdb_cursor_expand_prefix(c, -(node->prefix_size + 1)))
		!= TKVDB_OK) {
		return r;
	}
	c->prefix_size -= node->prefix_size + 1;

	tmp_stack = realloc(c->stack,
		(c->stack_size - 1) * sizeof(tkvdb_visit_helper));
	if (!tmp_stack) {
		free(c->stack);
		c->stack = NULL;
		return TKVDB_ENOMEM;
	}
	c->stack = tmp_stack;
	c->stack_size--;

	return TKVDB_OK;
}


#define TKVDB_EXEC(FUNC) \
do {\
	TKVDB_RES r = FUNC;\
	if (r != TKVDB_OK) {\
		return r;\
	}\
} while (0)

#define TKVDB_SKIP_RNODES(NODE) \
	while (NODE->type & TKVDB_NODE_REPLACED) { \
		NODE = NODE->next[0]; \
	}

static int
tkvdb_smallest(tkvdb_cursor *c, tkvdb_memnode *node)
{
	int off = 0;
	tkvdb_memnode *next;

	for (;;) {
		/* skip replaced nodes */
		TKVDB_SKIP_RNODES(node);

		/* if node has prefix, append it to cursor */
		if (node->prefix_size > 0) {
			TKVDB_EXEC( tkvdb_cursor_expand_prefix(c,
				node->prefix_size) );

			/* append prefix */
			memcpy(c->prefix + c->prefix_size,
				node->prefix_val_meta,
				node->prefix_size);
			c->prefix_size += node->prefix_size;
		}

		/* stop search at key-value node */
		if (node->type & TKVDB_NODE_VAL) {
			TKVDB_EXEC( tkvdb_cursor_push(c, node, off) );
			break;
		}

		/* if current node is key without value, search in subnodes */
		next = NULL;
		for (off=0; off<256; off++) {
			if (node->next[off]) {
				/* found next subnode */
				next = node->next[off];
				break;
			}
		}

		if (!next) {
			/* key node and no subnodes, return error */
			return TKVDB_CORRUPTED;
		}

		TKVDB_EXEC( tkvdb_cursor_expand_prefix(c, 1) );

		c->prefix[c->prefix_size] = off;
		c->prefix_size++;

		/* push node */
		TKVDB_EXEC( tkvdb_cursor_push(c, node, off) );

		node = next;
	}

	return TKVDB_OK;
}

TKVDB_RES
tkvdb_first(tkvdb_cursor *c, tkvdb_tr *tr)
{
	int r;

	r = tkvdb_smallest(c, tr->root);
	return r;
}

TKVDB_RES
tkvdb_next(tkvdb_cursor *c)
{
	int *off;
	tkvdb_memnode *node, *next;
	TKVDB_RES r;

next_node:
	if (c->stack_size < 1) {
		return TKVDB_EMPTY;
	}

	/* get node from stack's top */
	node = c->stack[c->stack_size - 1].node;
	off = &(c->stack[c->stack_size - 1].off);
	(*off)++;

	/* search in subnodes */
	next = NULL;
	for (; *off<256; (*off)++) {
		if (node->next[*off]) {
			next = node->next[*off];
			break;
		}
	}

	/* found */
	if (next) {
		/* expand cursor key */
		TKVDB_EXEC( tkvdb_cursor_expand_prefix(c, 1) );
		c->prefix[c->prefix_size] = *off;
		c->prefix_size++;

		r = tkvdb_smallest(c, next);
		return r;
	}

	/* pop */
	TKVDB_EXEC( tkvdb_cursor_pop(c) );

	goto next_node;

	return TKVDB_OK;
}

void *
tkvdb_cursor_key(tkvdb_cursor *c)
{
	return c->prefix;
}

size_t
tkvdb_cursor_keysize(tkvdb_cursor *c)
{
	return c->prefix_size;
}

tkvdb_tr *
tkvdb_tr_create(tkvdb *db)
{
	tkvdb_tr *tr;

	tr = malloc(sizeof (tkvdb_tr));
	if (!tr) {
		return NULL;
	}

	tr->db = db;
	tr->root = NULL;
	return tr;
}

void
tkvdb_tr_free(tkvdb_tr *tr)
{
	free(tr);
}

static void
tkvdb_tr_reset(tkvdb_tr *tr)
{
	(void)tr;
}

TKVDB_RES
tkvdb_begin(tkvdb_tr *tr)
{
	tkvdb_tr_reset(tr);

	return TKVDB_OK;
}

TKVDB_RES
tkvdb_rollback(tkvdb_tr *tr)
{
	tkvdb_tr_reset(tr);

	return TKVDB_OK;
}

/* compact node and put it to write buffer */
static TKVDB_RES
tkvdb_node_to_buf(tkvdb *db, tkvdb_memnode *node, uint64_t transaction_off)
{
	struct tkvdb_disknode *disknode;
	uint8_t *ptr;
	uint64_t iobuf_off;

	iobuf_off = node->disk_off - transaction_off;

	if ((iobuf_off + node->disk_size) > db->params.write_buf_limit) {
		/* not enough space for node in buffer */
		return TKVDB_ENOMEM;
	}
	if ((iobuf_off + node->disk_size) > db->write_buf_allocated) {
		uint8_t *tmp;

		if (!db->params.write_buf_dynalloc) {
			return TKVDB_ENOMEM;
		}

		tmp = realloc(db->write_buf, iobuf_off + node->disk_size);
		if (!tmp) {
			return TKVDB_ENOMEM;
		}

		db->write_buf = tmp;
		db->write_buf_allocated = iobuf_off + node->disk_size;
	}

	disknode = (struct tkvdb_disknode *)(db->write_buf + iobuf_off);

	disknode->size = node->disk_size;
	disknode->type = node->type;

	disknode->nsubnodes = node->nsubnodes;

	ptr = disknode->data;

	if (node->type & TKVDB_NODE_VAL) {
		*((uint32_t *)ptr) = node->val_size;
		ptr += sizeof(uint32_t);
	}
	if (node->type & TKVDB_NODE_META) {
		*((uint32_t *)ptr) = node->meta_size;
		ptr += sizeof(uint32_t);
	}

	if (node->nsubnodes > TKVDB_SUBNODES_THR) {
		memcpy(ptr, node->fnext, sizeof(uint64_t) * 256);
		ptr += sizeof(uint64_t) * 256;
	} else {
		int i;
		uint8_t *symbols;

		/* array of next symbols */
		symbols = ptr;
		ptr += node->nsubnodes * sizeof(uint8_t);
		for (i=0; i<256; i++) {
			if (node->fnext[i]) {
				*symbols = i;
				symbols++;

				*((uint64_t *)ptr) = node->fnext[i];
				ptr += sizeof(uint64_t);
			}
		}
	}

	memcpy(ptr, node->prefix_val_meta, node->prefix_size + node->val_size
		+ node->meta_size);

	return TKVDB_OK;
}

/* calculate size of node on disk */
static void
tkvdb_node_calc_disksize(tkvdb_memnode *node)
{
	unsigned int i;

	node->nsubnodes = 0;

	for (i=0; i<256; i++) {
		if (node->next[i] || node->fnext[i]) {
			node->nsubnodes++;
		}
	}

	node->disk_size = sizeof(struct tkvdb_disknode) - 1;

	/* if node has value add 4 bytes for value size */
	if (node->type & TKVDB_NODE_VAL) {
		node->disk_size += sizeof(uint32_t);
	}
	/* 4 bytes for metadata size */
	if (node->type & TKVDB_NODE_META) {
		node->disk_size += sizeof(uint32_t);
	}

	/* subnodes */
	if (node->nsubnodes > TKVDB_SUBNODES_THR) {
		node->disk_size += node->nsubnodes * sizeof(uint8_t);
		node->disk_size += 256 * sizeof(uint64_t);
	} else {
		node->disk_size += node->nsubnodes
			+ node->nsubnodes * sizeof(uint64_t);
	}

	/* prefix + value + metadata */
	node->disk_size += node->prefix_size + node->val_size
		+ node->meta_size;
}

TKVDB_RES
tkvdb_commit(tkvdb_tr *tr)
{
	size_t stack_depth = 0;
	tkvdb_visit_helper *stack = NULL;

	struct tkvdb_header db_header;
	/* file offsets */
	uint64_t transaction_off, node_off;
	struct stat st;

	tkvdb_memnode *node;
	int off = 0;
	TKVDB_RES r = TKVDB_OK;

	if (!tr->db) {
		tkvdb_tr_reset(tr);
		return TKVDB_OK;
	}

	/* try to set lock */
	if (lseek(tr->db->fd, 0, SEEK_SET) != 0) {
		return TKVDB_ERROR;
	}

	if (read(tr->db->fd, &db_header, sizeof(struct tkvdb_header))
		!= sizeof(struct tkvdb_header)) {
		return TKVDB_ERROR;
	}

	if (db_header.locked) {
		return TKVDB_LOCKED;
	}

	db_header.locked = 1;

	if (lseek(tr->db->fd, 0, SEEK_SET) != 0) {
		return TKVDB_ERROR;
	}

	if (write(tr->db->fd, &db_header, sizeof(struct tkvdb_header))
		!= sizeof(struct tkvdb_header)) {
		return TKVDB_ERROR;
	}

	/* get file size */
	if (fstat(tr->db->fd, &st) < 0) {
		return TKVDB_ERROR;
	}

	/* append transaction to the end of file */
	transaction_off = st.st_size;
	/* first node offset */
	node_off = transaction_off;

	/* now iterate through nodes in transaction */
	node = tr->root;

	for (;;) {
		tkvdb_memnode *next;

		TKVDB_SKIP_RNODES(node);

		if (node->disk_size == 0) {
			tkvdb_node_calc_disksize(node);

			node->disk_off = node_off;
		}

		next = NULL;
		for (; off<256; off++) {
			if (node->next[off]) {
				/* found next subnode */
				next = node->next[off];
				break;
			}
		}

		if (next) {
			node->fnext[off] = node_off;
			node_off += node->disk_size;

			/* push node and position to stack */
			stack = realloc(stack, sizeof(tkvdb_visit_helper)
				* (stack_depth + 1));

			stack[stack_depth].node = node;
			stack[stack_depth].off = off;
			stack_depth++;

			node = next;
			off = 0;
		} else {
			/* no more subnodes, copy node to buffer */
			r = tkvdb_node_to_buf(tr->db, node, transaction_off);

			/* pop */
			if (stack_depth == 0) {
				break;
			}

			node = stack[stack_depth - 1].node;
			off  = stack[stack_depth - 1].off + 1;
			stack = realloc(stack, (stack_depth - 1)
				* sizeof(tkvdb_visit_helper));

			if (stack_depth == 1) {
				stack = NULL;
			}
			stack_depth--;
		}
	}

	/* write transaction to disk */
	/* XXX: check! */
	lseek(tr->db->fd, transaction_off, SEEK_SET);
	write(tr->db->fd, tr->db->write_buf, node_off);

	/* unlock database */
	db_header.locked = 0;
	db_header.root = transaction_off;

	if (lseek(tr->db->fd, 0, SEEK_SET) != 0) {
		return TKVDB_ERROR;
	}

	if (write(tr->db->fd, &db_header, sizeof(struct tkvdb_header))
		!= sizeof(struct tkvdb_header)) {
		return TKVDB_ERROR;
	}

	free(stack);
	return r;
}

