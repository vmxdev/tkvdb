#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include "tkvdb.h"

struct tkvdb_params
{
	size_t mmap_len;
	/* TODO: partitioning */
};

/* database */
struct tkvdb
{
	/* database file handle */
	int fd;

	/* database params */
	tkvdb_params params;
};

/* node in memory */
typedef struct tkvdb_memnode
{
	enum TKVDB_MNTYPE type;
	size_t prefix_size;
	size_t val_size;

	struct tkvdb_memnode *next[256];
	uint64_t fnext[256];

	unsigned char prefix_val_meta[1];
} tkvdb_memnode;

/* transaction in memory */
struct tkvdb_memtr
{
	tkvdb *db;
	tkvdb_memnode *root;
};

/* index of subnode in node */
typedef struct tkvdb_nodepos
{
	tkvdb_memnode *node;
	int off;
} tkvdb_nodepos;

/* database cursor */
struct tkvdb_cursor
{
	size_t stack_size;
	tkvdb_nodepos *stack;

	size_t prefix_size;
	unsigned char *prefix;
};


/* allocate node and append prefix and value */
tkvdb_memnode *
tkvdb_node_alloc(enum TKVDB_MNTYPE type, size_t prefix_size,
	const void *prefix, size_t vlen, const void *val)
{
	tkvdb_memnode *r;

	r = malloc(sizeof(tkvdb_memnode) + prefix_size + vlen);
	if (!r) {
		return NULL;
	}

	r->type = type;
	r->prefix_size = prefix_size;
	r->val_size = vlen;
	if (r->prefix_size > 0) {
		memcpy(r->prefix_val_meta, prefix, r->prefix_size);
	}
	if (r->val_size > 0) {
		memcpy(r->prefix_val_meta + r->prefix_size, val, r->val_size);
	}

	memset(r->next, 0, sizeof(tkvdb_memnode *) * 256);
	memset(r->fnext, 0, sizeof(uint64_t) * 256);

	return r;
}

static void
clone_subnodes(tkvdb_memnode *dst, tkvdb_memnode *src)
{
	memcpy(dst->next,  src->next, sizeof(tkvdb_memnode *) * 256);
	memcpy(dst->fnext, src->fnext, sizeof(uint64_t) * 256);
}

/* add key-value pair to memory transaction */
TKVDB_RES
tkvdb_put(tkvdb_memtr *tr,
	const void *key, size_t klen, const void *val, size_t vlen)
{
	const unsigned char *sym;  /* pointer to current symbol in key */
	tkvdb_memnode *curr;       /* current node */
	size_t pi;                 /* prefix index */

	/* new root */
	if (tr->root == NULL) {
		tr->root = tkvdb_node_alloc(TKVDB_KEYVAL, klen, key,
			vlen, val);
		if (!tr->root) {
			return TKVDB_ENOMEM;
		}

		return TKVDB_OK;
	}

	sym = key;
	curr = tr->root;

next_node:
	if (curr->type == TKVDB_REPLACED) {
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

			newroot = tkvdb_node_alloc(TKVDB_KEYVAL,
				pi, curr->prefix_val_meta, vlen, val);
			if (!newroot) return TKVDB_ENOMEM;

			clone_subnodes(newroot, curr);

			curr->type = TKVDB_REPLACED;
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
		newroot = tkvdb_node_alloc(TKVDB_KEYVAL, pi,
			curr->prefix_val_meta,
			vlen, val);
		if (!newroot) return TKVDB_ENOMEM;

		subnode_rest = tkvdb_node_alloc(curr->type,
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
		curr->type = TKVDB_REPLACED;
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
			tmp = tkvdb_node_alloc(TKVDB_KEYVAL,
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
#ifdef TRACE
		printf("splitting key at %lu, '%c'(%d) != '%c'(%d)\n",
			pi, curr->prefix_and_val[pi], curr->prefix_and_val[pi],
			*sym, *sym);
#endif

		/* split current node into 3 subnodes */
		newroot = tkvdb_node_alloc(TKVDB_KEY, pi,
			curr->prefix_val_meta, 0, NULL);
		if (!newroot) return TKVDB_ENOMEM;

		/* rest of prefix (skip current symbol) */
		subnode_rest = tkvdb_node_alloc(curr->type,
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
		subnode_key = tkvdb_node_alloc(TKVDB_KEYVAL,
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

		curr->type = TKVDB_REPLACED;
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
tkvdb_cursor_new()
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

void
tkvdb_cursor_close(tkvdb_cursor *c)
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
}

static int
tkvdb_cursor_expand_prefix(tkvdb_cursor *c, int n)
{
	unsigned char *tmp_pfx;

	if (n == 0) {
		/* underflow */
		printf("underflow?\n");
		if (c->prefix) {
			free(c->prefix);
			c->prefix = NULL;
		}
		return /*TKVDB_ENOMEM*/TKVDB_OK;
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
	tkvdb_nodepos *tmp_stack;

	tmp_stack = realloc(c->stack,
		(c->stack_size + 1) * sizeof(tkvdb_nodepos));
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
	tkvdb_nodepos *tmp_stack;
	int r;
	tkvdb_memnode *node;

	if (c->stack_size <= 1) {
		printf("stack underflow!\n");
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
		(c->stack_size - 1) * sizeof(tkvdb_nodepos));
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
	int r = FUNC;\
	if (r != TKVDB_OK) {\
		return r;\
	}\
} while (0)

#define TKVDB_SKIP_RNODES(NODE) \
	while (NODE->type == TKVDB_REPLACED) { \
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
		if (node->type == TKVDB_KEYVAL) {
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

int
tkvdb_first(tkvdb_cursor *c, tkvdb_memtr *tr)
{
	int r;

	r = tkvdb_smallest(c, tr->root);
	return r;
}

int
tkvdb_next(tkvdb_cursor *c)
{
	int r, *off;
	tkvdb_memnode *node, *next;

next_node:
	if (c->stack_size < 1) {
		printf("???\n");
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

TKVDB_RES
tkvdb_begin(tkvdb *db, tkvdb_memtr **tr)
{
	/* requested new transaction */
	if (!(*tr)) {
		*tr = malloc(sizeof (tkvdb_memtr));
		if (*tr == NULL) {
			return TKVDB_ENOMEM;
		}
	}

	(*tr)->db = db;

	return TKVDB_OK;
}

