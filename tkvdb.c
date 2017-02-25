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

/* read block size */
#define TKVDB_READ_SIZE 4096

/* max depth of search stack, assuming there will not 2**64 keys */
#define TKVDB_STACK_MAX_DEPTH 64

/* helper macro for executing functions which returns TKVDB_RES */
#define TKVDB_EXEC(FUNC) \
do {\
	TKVDB_RES r = FUNC;\
	if (r != TKVDB_OK) {\
		return r;\
	}\
} while (0)

/* skip replaced nodes */
#define TKVDB_SKIP_RNODES(NODE) \
while (NODE->type & TKVDB_NODE_REPLACED) { \
	NODE = NODE->replaced_by; \
}

struct tkvdb_params
{
	int flags;              /* db file flags (as passed to open()) */
	mode_t mode;            /* db file mode */

	size_t write_buf_limit; /* size of database write buffer */
	int write_buf_dynalloc; /* realloc buffer when needed */

	size_t tr_buf_limit;    /* size of transaction buffer */
	int tr_buf_dynalloc;    /* realloc transaction buffer when needed */

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
	uint8_t signature[8];
	uint64_t root_off;     /* offset of last transaction root */

	uint64_t gap_begin;
	uint64_t gap_end;

	/* number of currently running transactions
	 * increased in tkvdb_begin(), decreased after commit/rollback */
	uint32_t running_transactions;
	uint32_t locked;       /* something is commited right now */
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

/* on-disk transaction */
struct tkvdb_tr_header
{
	uint8_t signature[8];
	uint64_t size;

	uint8_t data[1];
} __attribute__((packed));

#define TKVDB_TR_HDRSIZE (sizeof(struct tkvdb_tr_header) - 1)

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

	struct tkvdb_memnode *replaced_by;

	struct tkvdb_memnode *next[256];  /* subnodes in memory */
	uint64_t fnext[256];              /* positions of subnodes in file */

	unsigned char prefix_val_meta[1]; /* prefix, value and metadata */
} tkvdb_memnode;

/* transaction in memory */
struct tkvdb_tr
{
	tkvdb *db;
	tkvdb_memnode *root;

	int started;
	uint64_t root_off;       /* offset of root node on disk */

	uint8_t *tr_buf;         /* transaction buffer */
	size_t tr_buf_allocated;
	uint8_t *tr_buf_ptr;

	size_t tr_buf_limit;     /* size of transaction buffer */
	int tr_buf_dynalloc;     /* realloc transaction buffer when needed */
};

struct tkvdb_visit_helper
{
	tkvdb_memnode *node;
	int off;                /* index of subnode in node */

};

/* database cursor */
struct tkvdb_cursor
{
	size_t stack_size;
	struct tkvdb_visit_helper stack[TKVDB_STACK_MAX_DEPTH];

	size_t prefix_size;
	unsigned char *prefix;

	tkvdb_tr *tr;
};

/* get next subnode (or load from disk) */
#define TKVDB_SUBNODE_NEXT(TR, NODE, NEXT, OFF)\
do {\
	if (NODE->next[OFF]) {\
		NEXT = node->next[OFF];\
	} else if (TR->db && NODE->fnext[OFF]) {\
		tkvdb_memnode *tmp;\
		TKVDB_EXEC( tkvdb_node_read(TR, NODE->fnext[OFF], &tmp) );\
		NODE->next[OFF] = tmp;\
		NEXT = tmp;\
	}\
} while (0)

#define TKVDB_SUBNODE_SEARCH(TR, NODE, NEXT, OFF, INCR)\
do {\
	int lim, step;\
	NEXT = NULL;\
	if (INCR) {\
		lim = 256;\
		step = 1;\
	} else {\
		lim = -1;\
		step = -1;\
	}\
	for (; OFF!=lim; OFF+=step) {\
		TKVDB_SUBNODE_NEXT(TR, NODE, NEXT, OFF);\
		if (next) {\
			break;\
		}\
	}\
} while (0)

/* fill tkvdb_params with default values */
void
tkvdb_params_init(tkvdb_params *params)
{
	params->write_buf_dynalloc = 1;
	params->write_buf_limit = SIZE_MAX;

	params->tr_buf_dynalloc = 1;
	params->tr_buf_limit = SIZE_MAX;

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

	if (io_res < (ssize_t)sizeof(struct tkvdb_header)) {
		/* ok, assuming it's error */
		goto fail_close;
	}

	/* check signature */
	if ((memcmp(db_header.signature, TKVDB_SIGNATURE,
		sizeof(TKVDB_SIGNATURE) - 1)) != 0) {
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

	if (!db) {
		return TKVDB_OK;
	}

	if (close(db->fd) < 0) {
		res = TKVDB_IO_ERROR;
	}
	if (db->write_buf) {
		free(db->write_buf);
	}

	free(db);
	return res;
}

/* get memory for node
 * memory block is taken from system using malloc()
 * when 'tr->tr_buf_dynalloc' is true or from preallocated buffer
 * buffer is allocated in tkvdb_tr_create_m() */
static tkvdb_memnode *
tkvdb_node_alloc(tkvdb_tr *tr, size_t node_size)
{
	tkvdb_memnode *node;

	if ((tr->tr_buf_allocated + node_size) > tr->tr_buf_limit) {
		/* memory limit exceeded */
		return NULL;
	}

	if (tr->tr_buf_dynalloc) {
		node = malloc(node_size);
		if (!node) {
			return NULL;
		}
	} else {
		node = (tkvdb_memnode *)tr->tr_buf_ptr;
		tr->tr_buf_ptr += node_size;
	}

	tr->tr_buf_allocated += node_size;
	return node;
}

/* create new node and append prefix and value */
static tkvdb_memnode *
tkvdb_node_new(tkvdb_tr *tr, int type, size_t prefix_size,
	const void *prefix, size_t vlen, const void *val)
{
	tkvdb_memnode *node;
	size_t node_size;

	node_size = sizeof(tkvdb_memnode) + prefix_size + vlen;
	node = tkvdb_node_alloc(tr, node_size);
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
tkvdb_clone_subnodes(tkvdb_memnode *dst, tkvdb_memnode *src)
{
	memcpy(dst->next,  src->next, sizeof(tkvdb_memnode *) * 256);
	memcpy(dst->fnext, src->fnext, sizeof(uint64_t) * 256);
}

/* read node from disk */
static TKVDB_RES
tkvdb_node_read(tkvdb_tr *tr, uint64_t off, tkvdb_memnode **node_ptr)
{
	uint8_t buf[TKVDB_READ_SIZE];
	ssize_t read_res;
	struct tkvdb_disknode *disknode;
	size_t prefix_val_meta_size;
	uint8_t *ptr;
	int fd;

	fd = tr->db->fd;

	if (lseek(fd, off, SEEK_SET) != (off_t)off) {
		return TKVDB_IO_ERROR;
	}

	read_res = read(fd, buf, TKVDB_READ_SIZE);
	if (read_res < 0) {
		return TKVDB_IO_ERROR;
	}

	disknode = (struct tkvdb_disknode *)buf;

	/* calculate size of prefix + value + metadata */
	prefix_val_meta_size = disknode->size - sizeof(struct tkvdb_disknode)
		+ sizeof(uint8_t) * 1;

	if (disknode->type & TKVDB_NODE_VAL) {
		prefix_val_meta_size -= sizeof(uint32_t);
	}
	if (disknode->type & TKVDB_NODE_META) {
		prefix_val_meta_size -= sizeof(uint32_t);
	}

	if (disknode->nsubnodes > TKVDB_SUBNODES_THR) {
		prefix_val_meta_size -= 256 * sizeof(uint64_t);
	} else {
		prefix_val_meta_size -= disknode->nsubnodes * sizeof(uint8_t);
		prefix_val_meta_size -= disknode->nsubnodes * sizeof(uint64_t);
	}

	/* allocate memnode */
	*node_ptr = tkvdb_node_alloc(tr, sizeof(tkvdb_memnode)
		+ prefix_val_meta_size);

	if (!(*node_ptr)) {
		return TKVDB_ENOMEM;
	}

	/* now fill memnode with values from disk node */
	(*node_ptr)->type = disknode->type;
	(*node_ptr)->prefix_size = disknode->prefix_size;

	(*node_ptr)->disk_size = 0;
	(*node_ptr)->disk_off = 0;

	ptr = disknode->data;

	(*node_ptr)->val_size = (*node_ptr)->meta_size = 0;
	if (disknode->type & TKVDB_NODE_VAL) {
		(*node_ptr)->val_size = *((uint32_t *)ptr);
		ptr += sizeof(uint32_t);
	}
	if (disknode->type & TKVDB_NODE_META) {
		(*node_ptr)->meta_size = *((uint32_t *)ptr);
		ptr += sizeof(uint32_t);
	}

	if (disknode->nsubnodes > TKVDB_SUBNODES_THR) {
		memcpy((*node_ptr)->fnext, ptr, 256 * sizeof(uint64_t));
		ptr += 256 * sizeof(uint64_t);
	} else {
		int i;
		uint64_t *offptr;

		offptr = (uint64_t *)(ptr
			+ disknode->nsubnodes * sizeof(uint8_t));

		memset((*node_ptr)->next, 0, sizeof(tkvdb_memnode *) * 256);
		memset((*node_ptr)->fnext, 0, sizeof(uint64_t) * 256);

		for (i=0; i<disknode->nsubnodes; i++) {
			(*node_ptr)->fnext[*ptr] = *offptr;
			ptr++;
			offptr++;
		}
		ptr += disknode->nsubnodes * sizeof(uint64_t);
	}

	if (disknode->size > TKVDB_READ_SIZE) {
		/* prefix + value + metadata bigger than read block */
		size_t blk_tail = disknode->size - prefix_val_meta_size;

		memcpy((*node_ptr)->prefix_val_meta, ptr,
			TKVDB_READ_SIZE - blk_tail);
		ptr += TKVDB_READ_SIZE - blk_tail;
		read(fd, ptr, disknode->size - (TKVDB_READ_SIZE - blk_tail));
	} else {
		memcpy((*node_ptr)->prefix_val_meta, ptr,
			prefix_val_meta_size);
	}

	return TKVDB_OK;
}

/* free node and subnodes */
static void
tkvdb_node_free(tkvdb_memnode *node)
{
	size_t stack_size = 0;
	struct tkvdb_visit_helper stack[TKVDB_STACK_MAX_DEPTH];

	tkvdb_memnode *next;
	int off = 0;

	for (;;) {
		if (node->type & TKVDB_NODE_REPLACED) {
			next = node->replaced_by;
			free(node);
			node = next;
			continue;
		}

		/* search in subnodes */
		next = NULL;
		for (; off<256; off++) {
			if (node->next[off]) {
				next = node->next[off];
				break;
			}
		}

		if (next) {
			/* push */
			stack[stack_size].node = node;
			stack[stack_size].off = off;
			stack_size++;

			node = next;
			off = 0;
		} else {
			/* no more subnodes */
			if (stack_size < 1) {
				break;
			}

			free(node);
			/* get node from stack's top */
			stack_size--;
			node = stack[stack_size].node;
			off = stack[stack_size].off;
			off++;
		}
	}
	free(node);
}

/* add key-value pair to memory transaction */
TKVDB_RES
tkvdb_put(tkvdb_tr *tr,
	const void *key, size_t klen, const void *val, size_t vlen)
{
	const unsigned char *sym;  /* pointer to current symbol in key */
	tkvdb_memnode *node;       /* current node */
	size_t pi;                 /* prefix index */

	if (!tr->started) {
		return TKVDB_NOT_STARTED;
	}

	/* new root */
	if (tr->root == NULL) {
		if (tr->db && (tr->root_off > TKVDB_TR_HDRSIZE)) {
			/* we have underlying non-empty db file */
			TKVDB_EXEC( tkvdb_node_read(tr, tr->root_off,
				&(tr->root)) );
		} else {
			tr->root = tkvdb_node_new(tr, TKVDB_NODE_VAL,
				klen, key, vlen, val);
			if (!tr->root) {
				return TKVDB_ENOMEM;
			}
			return TKVDB_OK;
		}
	}

	sym = key;
	node = tr->root;

next_node:
	TKVDB_SKIP_RNODES(node);
	pi = 0;

next_byte:

/* end of key
  Here we have two cases:
  [p][r][e][f][i][x] - prefix
  [p][r][e] - new key

  or exact match:
  [p][r][e][f][i][x] - prefix
  [p][r][e][f][i][x] - new key
*/
	if (sym >= ((unsigned char *)key + klen)) {
		tkvdb_memnode *newroot, *subnode_rest;

		if (pi == node->prefix_size) {
			/* exact match */
			if ((node->val_size == vlen) && (vlen != 0)) {
				/* same value size, so copy new value and
					return */
				memcpy(node->prefix_val_meta
					+ node->prefix_size, val, vlen);
				return TKVDB_OK;
			}

			newroot = tkvdb_node_new(tr, TKVDB_NODE_VAL,
				pi, node->prefix_val_meta, vlen, val);
			if (!newroot) return TKVDB_ENOMEM;

			tkvdb_clone_subnodes(newroot, node);

			node->type = TKVDB_NODE_REPLACED;
			node->replaced_by = newroot;

			return TKVDB_OK;
		}

/* split node with prefix
  [p][r][e][f][i][x] - prefix
  [p][r][e] - new key

  becomes
  [p][r][e] - new root
  next['f'] => [i][x] - tail
*/
		newroot = tkvdb_node_new(tr, TKVDB_NODE_VAL, pi,
			node->prefix_val_meta,
			vlen, val);
		if (!newroot) return TKVDB_ENOMEM;

		subnode_rest = tkvdb_node_new(tr, node->type,
			node->prefix_size - pi - 1,
			node->prefix_val_meta + pi + 1,
			node->val_size,
			node->prefix_val_meta + node->prefix_size);

		if (!subnode_rest) {
			free(newroot);
			return TKVDB_ENOMEM;
		}
		tkvdb_clone_subnodes(subnode_rest, node);

		newroot->next[node->prefix_val_meta[pi]] = subnode_rest;

		node->type = TKVDB_NODE_REPLACED;
		node->replaced_by = newroot;

		return TKVDB_OK;
	}

/* end of prefix
  [p][r][e][f][i][x] - old prefix
  [p][r][e][f][i][x][n][e][w]- new prefix

  so we hold old node and change only pointer to next
  [p][r][e][f][i][x]
  next['n'] => [e][w] - tail
*/
	if (pi >= node->prefix_size) {
		if (node->next[*sym] != NULL) {
			/* continue with next node */
			node = node->next[*sym];
			sym++;
			goto next_node;
		} else if (tr->db && (node->fnext[*sym] != 0)) {
			tkvdb_memnode *tmp;

			/* load subnode from disk */
			TKVDB_EXEC( tkvdb_node_read(tr, node->fnext[*sym],
				&tmp) );

			node->next[*sym] = tmp;
			node = tmp;
			sym++;
			goto next_node;
		} else {
			tkvdb_memnode *tmp;

			/* allocate tail */
			tmp = tkvdb_node_new(tr, TKVDB_NODE_VAL,
				klen - (sym - (unsigned char *)key) - 1,
				sym + 1,
				vlen, val);
			if (!tmp) return TKVDB_ENOMEM;

			node->next[*sym] = tmp;
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
	if (node->prefix_val_meta[pi] != *sym) {
		tkvdb_memnode *newroot, *subnode_rest, *subnode_key;

		/* split current node into 3 subnodes */
		newroot = tkvdb_node_new(tr, 0, pi,
			node->prefix_val_meta, 0, NULL);
		if (!newroot) return TKVDB_ENOMEM;

		/* rest of prefix (skip current symbol) */
		subnode_rest = tkvdb_node_new(tr, node->type,
			node->prefix_size - pi - 1,
			node->prefix_val_meta + pi + 1,
			node->val_size,
			node->prefix_val_meta + node->prefix_size);
		if (!subnode_rest) {
			free(newroot);
			return TKVDB_ENOMEM;
		}
		tkvdb_clone_subnodes(subnode_rest, node);

		/* rest of key */
		subnode_key = tkvdb_node_new(tr, TKVDB_NODE_VAL,
			klen - (sym - (unsigned char *)key) - 1,
			sym + 1,
			vlen, val);
		if (!subnode_key) {
			free(subnode_rest);
			free(newroot);
			return TKVDB_ENOMEM;
		}

		newroot->next[node->prefix_val_meta[pi]] = subnode_rest;
		newroot->next[*sym] = subnode_key;

		node->type = TKVDB_NODE_REPLACED;
		node->replaced_by = newroot;

		return TKVDB_OK;
	}

	sym++;
	pi++;
	goto next_byte;

	return TKVDB_OK;
}

/* cursors */

tkvdb_cursor *
tkvdb_cursor_create(tkvdb_tr *tr)
{
	tkvdb_cursor *c;

	c = malloc(sizeof(tkvdb_cursor));
	if (!c) {
		return NULL;
	}

	c->stack_size = 0;

	c->prefix_size = 0;
	c->prefix = NULL;

	c->tr = tr;

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

	c->stack_size = 0;

	free(c);

	return TKVDB_OK;
}

/* todo: rewrite */
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

/* add (push) node to cursor */
static int
tkvdb_cursor_push(tkvdb_cursor *c, tkvdb_memnode *node, int off)
{
	c->stack[c->stack_size].node = node;
	c->stack[c->stack_size].off = off;
	c->stack_size++;

	return TKVDB_OK;
}

/* pop node from cursor */
static int
tkvdb_cursor_pop(tkvdb_cursor *c)
{
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

	c->stack_size--;

	return TKVDB_OK;
}

static TKVDB_RES
tkvdb_smallest_or_biggest(tkvdb_cursor *c, tkvdb_memnode *node, int smallest)
{
	int off;
	tkvdb_memnode *next;

	off = smallest ? 0 : 255;

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
		off = smallest ? 0 : 255;

		TKVDB_SUBNODE_SEARCH(c->tr, node, next, off, smallest);
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

static TKVDB_RES
tkvdb_cursor_load_root(tkvdb_cursor *c)
{
	if (!c->tr->root) {
		/* empty root node */
		if (!c->tr->db) {
			/* and no database file */
			return TKVDB_EMPTY;
		}

		if (c->tr->root_off < sizeof(struct tkvdb_header)) {
			/* database is empty */
			return TKVDB_EMPTY;
		}
		/* try to read root node */
		TKVDB_EXEC( tkvdb_node_read(c->tr, c->tr->root_off,
			&(c->tr->root)) );
	}

	return TKVDB_OK;
}

TKVDB_RES
tkvdb_first(tkvdb_cursor *c)
{
	TKVDB_EXEC( tkvdb_cursor_load_root(c) );
	return tkvdb_smallest_or_biggest(c, c->tr->root, 1);
}

TKVDB_RES
tkvdb_last(tkvdb_cursor *c)
{
	TKVDB_EXEC( tkvdb_cursor_load_root(c) );
	return tkvdb_smallest_or_biggest(c, c->tr->root, 0);
}


TKVDB_RES
tkvdb_seek(tkvdb_cursor *c, const void *key, size_t klen, TKVDB_SEEK seek)
{
	tkvdb_memnode *node, *next;

	const uint8_t *sym;
	size_t pi;

	TKVDB_EXEC( tkvdb_cursor_load_root(c) );
	node = c->tr->root;

	sym = key;

next_node:
	TKVDB_SKIP_RNODES(node);
	pi = 0;

next_byte:
	if (sym >= ((uint8_t *)key + klen)) {
		/* end of key */
		if (pi == node->prefix_size) {
			/* exact match */
			return TKVDB_OK;
		}

		if (seek != TKVDB_SEEK_GE) {
			return TKVDB_EMPTY;
		}
		TKVDB_EXEC( tkvdb_cursor_expand_prefix(c,
			node->prefix_size - pi) );

		/* append prefix */
		memcpy(c->prefix + c->prefix_size,
			node->prefix_val_meta,
			node->prefix_size - pi);
		c->prefix_size += node->prefix_size - pi;
		if (node->type & TKVDB_NODE_VAL) {
			return TKVDB_OK;
		}
		return tkvdb_smallest_or_biggest(c, node, seek==TKVDB_SEEK_LE);
	}

	if (pi >= node->prefix_size) {
		/* end of prefix (but not the key) */
		next = NULL;
		TKVDB_SUBNODE_NEXT(c->tr, node, next, *sym);
		if (next) {
			/* found next matching node, continue with it */
			TKVDB_EXEC( tkvdb_cursor_expand_prefix(c, 1) );
			c->prefix[c->prefix_size] = *sym;
			c->prefix_size++;

			sym++;
			goto next_node;
		} else {
			int off;
			if (seek == TKVDB_SEEK_EQ) {
				return TKVDB_EMPTY;
			}

			off = *sym;
			TKVDB_SUBNODE_SEARCH(c->tr, node, next, off,
				seek==TKVDB_SEEK_LE);

			if (next) {
				TKVDB_EXEC( tkvdb_cursor_expand_prefix(c, 1) );
				c->prefix[c->prefix_size] = *sym;
				c->prefix_size++;

				return tkvdb_smallest_or_biggest(c, next,
					seek==TKVDB_SEEK_LE);
			}
			/* unreachable? */
			return TKVDB_CORRUPTED;
		}
	}

	if (node->prefix_val_meta[pi] != *sym) {
		if (seek == TKVDB_SEEK_EQ) {
			return TKVDB_EMPTY;
		}

		if ((seek == TKVDB_SEEK_LE)
			&& (node->prefix_val_meta[pi] > *sym)) {
			return TKVDB_EMPTY;
		}
		if ((seek == TKVDB_SEEK_GE)
			&& (node->prefix_val_meta[pi] < *sym)) {
			return TKVDB_EMPTY;
		}

		TKVDB_EXEC( tkvdb_cursor_expand_prefix(c,
			node->prefix_size - pi) );

		/* append prefix */
		memcpy(c->prefix + c->prefix_size,
			node->prefix_val_meta,
			node->prefix_size - pi);
		c->prefix_size += node->prefix_size - pi;
		if (node->type & TKVDB_NODE_VAL) {
			return TKVDB_OK;
		}
		return tkvdb_smallest_or_biggest(c, node,
			seek == TKVDB_SEEK_LE);
	}


	sym++;
	pi++;
	goto next_byte;

	/* unreachable */
	return TKVDB_OK;
}

static TKVDB_RES
tkvdb_next_or_prev(tkvdb_cursor *c, int incr)
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
	incr ? (*off)++ : (*off)--;

	TKVDB_SUBNODE_SEARCH(c->tr, node, next, *off, incr);

	if (next) {
		/* expand cursor key */
		TKVDB_EXEC( tkvdb_cursor_expand_prefix(c, 1) );
		c->prefix[c->prefix_size] = *off;
		c->prefix_size++;

		r = tkvdb_smallest_or_biggest(c, next, incr);
		return r;
	}

	/* pop */
	TKVDB_EXEC( tkvdb_cursor_pop(c) );

	goto next_node;

	return TKVDB_OK;
}

TKVDB_RES
tkvdb_next(tkvdb_cursor *c)
{
	return tkvdb_next_or_prev(c, 1);
}

TKVDB_RES
tkvdb_prev(tkvdb_cursor *c)
{
	return tkvdb_next_or_prev(c, 0);
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
tkvdb_tr_create_m(tkvdb *db, size_t limit, int dynalloc)
{
	tkvdb_tr *tr;

	tr = malloc(sizeof(tkvdb_tr));
	if (!tr) {
		return NULL;
	}

	tr->db = db;
	tr->root = NULL;

	tr->started = 0;
	tr->root_off = 0;

	tr->tr_buf_dynalloc = dynalloc;
	tr->tr_buf_limit = limit;

	if (!tr->tr_buf_dynalloc) {
		tr->tr_buf = malloc(tr->tr_buf_limit);
		if (tr->tr_buf) {
			tr->tr_buf_ptr = tr->tr_buf;
		} else {
			free(tr);
			tr = NULL;
		}
	} else {
		tr->tr_buf = NULL;
		tr->tr_buf_ptr = NULL;
	}
	tr->tr_buf_allocated = 0;

	return tr;
}

tkvdb_tr *
tkvdb_tr_create(tkvdb *db)
{
	tkvdb_tr *tr;

	if (db) {
		/* inherit from database parameters */
		tr = tkvdb_tr_create_m(db,
			db->params.tr_buf_limit, db->params.tr_buf_dynalloc);
	} else {
		/* if no db, use dynamic allocation without limits */
		tr = tkvdb_tr_create_m(db, SIZE_MAX, 1);
	}

	return tr;
}


/* reset transaction to initial state */
static void
tkvdb_tr_reset(tkvdb_tr *tr)
{
	if (tr->tr_buf_dynalloc) {
		if (tr->root) {
			tkvdb_node_free(tr->root);
			tr->root = NULL;
		}
	} else {
		tr->tr_buf_ptr = tr->tr_buf;
	}

	tr->tr_buf_allocated = 0;
	tr->started = 0;
}

void
tkvdb_tr_free(tkvdb_tr *tr)
{
	if (tr->tr_buf_dynalloc) {
		tkvdb_tr_reset(tr);
	} else {
		free(tr->tr_buf);
	}

	free(tr);
}


TKVDB_RES
tkvdb_begin(tkvdb_tr *tr)
{
	struct tkvdb_header db_header;

	if (tr->started) {
		/* ignore if transaction is already started */
		return TKVDB_OK;
	}

	if (!tr->db) {
		tr->started = 1;
		return TKVDB_OK;
	}

	/* read database header to find root node */
	if (lseek(tr->db->fd, 0, SEEK_SET) != 0) {
		return TKVDB_IO_ERROR;
	}

	if (read(tr->db->fd, &db_header, sizeof(struct tkvdb_header))
		!= sizeof(struct tkvdb_header)) {
		return TKVDB_IO_ERROR;
	}

	/* TODO: increment number of running transactions in header */

	tr->root_off = db_header.root_off + TKVDB_TR_HDRSIZE;

	tr->started = 1;

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
	disknode->prefix_size = node->prefix_size;

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
	struct tkvdb_visit_helper stack[TKVDB_STACK_MAX_DEPTH];

	struct tkvdb_header db_header;
	struct tkvdb_tr_header *tr_header;
	/* offset of whole transaction in file */
	uint64_t transaction_off;
	/* offset of next node in file */
	uint64_t node_off;
	/* size of last accessed node, will be added to node_off */
	uint64_t last_node_size;
	struct stat st;

	tkvdb_memnode *node;
	int off = 0;
	TKVDB_RES r = TKVDB_OK;

	if (!tr->started) {
		return TKVDB_NOT_STARTED;
	}

	if (!tr->db) {
		tkvdb_tr_reset(tr);
		return TKVDB_OK;
	}

	/* try to set lock */
	if (lseek(tr->db->fd, 0, SEEK_SET) != 0) {
		return TKVDB_IO_ERROR;
	}

	if (read(tr->db->fd, &db_header, sizeof(struct tkvdb_header))
		!= sizeof(struct tkvdb_header)) {
		return TKVDB_IO_ERROR;
	}

	if (db_header.locked) {
		return TKVDB_LOCKED;
	}

	db_header.locked = 1;

	if (lseek(tr->db->fd, 0, SEEK_SET) != 0) {
		return TKVDB_IO_ERROR;
	}

	if (write(tr->db->fd, &db_header, sizeof(struct tkvdb_header))
		!= sizeof(struct tkvdb_header)) {
		return TKVDB_IO_ERROR;
	}

	/* get file size */
	if (fstat(tr->db->fd, &st) < 0) {
		return TKVDB_IO_ERROR;
	}

	/* append transaction to the end of file */
	transaction_off = st.st_size;
	/* first node offset */
	node_off = transaction_off + TKVDB_TR_HDRSIZE;

	last_node_size = 0;

	/* now iterate through nodes in transaction */
	node = tr->root;

	for (;;) {
		tkvdb_memnode *next;

		TKVDB_SKIP_RNODES(node);

		if (node->disk_size == 0) {
			tkvdb_node_calc_disksize(node);

			node->disk_off = node_off;
			last_node_size = node->disk_size;
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
			TKVDB_SKIP_RNODES(next);

			node_off += last_node_size;
			node->fnext[off] = node_off;

			/* push node and position to stack */
			stack[stack_depth].node = node;
			stack[stack_depth].off = off;
			stack_depth++;

			node = next;
			off = 0;
		} else {
			/* no more subnodes, serialize node to memory buffer */
			r = tkvdb_node_to_buf(tr->db, node, transaction_off);
			if (r != TKVDB_OK) {
				goto fail_unlock;
			}

			/* pop */
			if (stack_depth == 0) {
				break;
			}

			stack_depth--;
			node = stack[stack_depth].node;
			off  = stack[stack_depth].off + 1;
		}
	}

	/* fill transaction header */
	tr_header = (struct tkvdb_tr_header *)tr->db->write_buf;
	memcpy(tr_header->signature, TKVDB_TR_SIGNATURE,
		sizeof(TKVDB_TR_SIGNATURE) - 1);
	tr_header->size = node_off - transaction_off;

	/* write transaction to disk */
	if (lseek(tr->db->fd, transaction_off, SEEK_SET)
		!= (off_t)transaction_off) {
		return TKVDB_IO_ERROR;
	}
	node_off += last_node_size;
	if (write(tr->db->fd, tr->db->write_buf, node_off - transaction_off)
		!= (ssize_t)(node_off - transaction_off)) {
		return TKVDB_IO_ERROR;
	}

fail_unlock:
	/* unlock database */
	db_header.locked = 0;
	db_header.root_off = transaction_off;

	if (lseek(tr->db->fd, 0, SEEK_SET) != 0) {
		return TKVDB_IO_ERROR;
	}

	if (write(tr->db->fd, &db_header, sizeof(struct tkvdb_header))
		!= sizeof(struct tkvdb_header)) {
		return TKVDB_IO_ERROR;
	}

	if (fsync(tr->db->fd) < 0) {
		return TKVDB_IO_ERROR;
	}

	tkvdb_tr_reset(tr);

	return r;
}

