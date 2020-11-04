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


/* cursors */

#ifdef TKVDB_PARAMS_ALIGN_VAL

#define CURSOR_UPDATE_VAL()                                                  \
do {                                                                         \
	c->val_size = node->c.val_size;                                      \
	if (node->c.type & TKVDB_NODE_LEAF) {                                \
		c->val = ((TKVDB_MEMNODE_TYPE_LEAF *)node)->prefix_val_meta  \
			+ node->c.prefix_size + node->c.val_pad;             \
	} else {                                                             \
		c->val = node->prefix_val_meta                               \
			+ node->c.prefix_size + node->c.val_pad;             \
	}                                                                    \
} while (0)

#else

#define CURSOR_UPDATE_VAL()                                                  \
do {                                                                         \
	c->val_size = node->c.val_size;                                      \
	if (node->c.type & TKVDB_NODE_LEAF) {                                \
		c->val = ((TKVDB_MEMNODE_TYPE_LEAF *)node)->prefix_val_meta  \
			+ node->c.prefix_size;                               \
	} else {                                                             \
		c->val = node->prefix_val_meta                               \
			+ node->c.prefix_size;                               \
	}                                                                    \
} while (0)


#endif

/* add (push) node to cursor */
static int
TKVDB_IMPL_CURSOR_PUSH(tkvdb_cursor *cr, TKVDB_MEMNODE_TYPE *node, int off)
{
	tkvdb_cursor_data *c = cr->data;

	if ((c->stack_size + 1) > c->stack_allocated) {
		/* stack is too small */
		struct tkvdb_visit_helper *tmpstack;

		if (!c->stack_dynalloc) {
			/* dynamic reallocation is not allowed */
			return TKVDB_ENOMEM;
		}

		tmpstack = realloc(c->stack, (c->stack_size + 1)
			* sizeof(struct tkvdb_visit_helper));
		if (!tmpstack) {
			return TKVDB_ENOMEM;
		}
		c->stack = tmpstack;
		c->stack_allocated = c->stack_size + 1;
	}
	c->stack[c->stack_size].node = node;
	c->stack[c->stack_size].off = off;
	c->stack_size++;

	CURSOR_UPDATE_VAL();

	return TKVDB_OK;
}

/* pop node from cursor */
static int
TKVDB_IMPL_CURSOR_POP(tkvdb_cursor *cr)
{
	int r;
	TKVDB_MEMNODE_TYPE *node;
	tkvdb_cursor_data *c = cr->data;

	if (c->stack_size <= 1) {
		return TKVDB_NOT_FOUND;
	}

	node = c->stack[c->stack_size - 1].node;
	/* erase prefix */
	if ((r = tkvdb_cursor_resize_prefix(cr, node->c.prefix_size + 1, 0))
		!= TKVDB_OK) {
		return r;
	}
	c->prefix_size -= node->c.prefix_size + 1;

	c->stack_size--;

	CURSOR_UPDATE_VAL();

	return TKVDB_OK;
}


static TKVDB_RES
TKVDB_IMPL_CURSOR_APPEND(tkvdb_cursor *cr, uint8_t *str, size_t n)
{
	tkvdb_cursor_data *c = cr->data;

	if (n > 0) {
		TKVDB_EXEC( tkvdb_cursor_resize_prefix(cr, n, 1) );
		memcpy(c->prefix + c->prefix_size, str, n);
		c->prefix_size += n;
	}

	return TKVDB_OK;
}

static TKVDB_RES
TKVDB_IMPL_CURSOR_APPEND_SYM(tkvdb_cursor *cr, int sym)
{
	tkvdb_cursor_data *c = cr->data;

	TKVDB_EXEC( tkvdb_cursor_resize_prefix(cr, 1, 1) );
	c->prefix[c->prefix_size] = sym;
	c->prefix_size++;

	return TKVDB_OK;
}

static TKVDB_RES
TKVDB_IMPL_SMALLEST(tkvdb_cursor *cr, TKVDB_MEMNODE_TYPE *node)
{
	int off;
	TKVDB_MEMNODE_TYPE *next;
	tkvdb_cursor_data *c = cr->data;

	for (;;) {
		/* skip replaced nodes */
		TKVDB_SKIP_RNODES(node);

		/* if node has prefix, append it to cursor */
		if (node->c.prefix_size > 0) {
			TKVDB_EXEC( tkvdb_cursor_resize_prefix(cr,
				node->c.prefix_size, 1) );

			/* append prefix */
			if (node->c.type & TKVDB_NODE_LEAF) {
				TKVDB_MEMNODE_TYPE_LEAF *node_leaf;
				node_leaf = (TKVDB_MEMNODE_TYPE_LEAF *)node;
				memcpy(c->prefix + c->prefix_size,
					node_leaf->prefix_val_meta,
					node->c.prefix_size);
			} else {
				memcpy(c->prefix + c->prefix_size,
					node->prefix_val_meta,
					node->c.prefix_size);
			}
			c->prefix_size += node->c.prefix_size;
		}

		/* stop search at key-value node */
		if (node->c.type & TKVDB_NODE_VAL) {
			TKVDB_EXEC(
				TKVDB_IMPL_CURSOR_PUSH(cr, node, /*off*/-1)
			);
			break;
		}

		/* if current node is key without value, search in subnodes */
		off = 0;

		TKVDB_SUBNODE_SEARCH(c->tr, node, next, off, 1);
		if (!next) {
			/* key node and no subnodes, return error */
			return TKVDB_CORRUPTED;
		}

		TKVDB_EXEC( tkvdb_cursor_resize_prefix(cr, 1, 1) );

		c->prefix[c->prefix_size] = off;
		c->prefix_size++;

		/* push node */
		TKVDB_EXEC( TKVDB_IMPL_CURSOR_PUSH(cr, node, off) );

		node = next;
	}

	return TKVDB_OK;
}

static TKVDB_RES
TKVDB_IMPL_BIGGEST(tkvdb_cursor *cr, TKVDB_MEMNODE_TYPE *node)
{
	int off;
	TKVDB_MEMNODE_TYPE *next;
	tkvdb_cursor_data *c = cr->data;

	for (;;) {
		TKVDB_SKIP_RNODES(node);

		/* if node has prefix, append it to cursor */
		if (node->c.prefix_size > 0) {
			TKVDB_EXEC( tkvdb_cursor_resize_prefix(cr,
				node->c.prefix_size, 1) );

			/* append prefix */
			if (node->c.type & TKVDB_NODE_LEAF) {
				TKVDB_MEMNODE_TYPE_LEAF *node_leaf;
				node_leaf = (TKVDB_MEMNODE_TYPE_LEAF *)node;
				memcpy(c->prefix + c->prefix_size,
					node_leaf->prefix_val_meta,
					node->c.prefix_size);
			} else {
				memcpy(c->prefix + c->prefix_size,
					node->prefix_val_meta,
					node->c.prefix_size);
			}
			c->prefix_size += node->c.prefix_size;
		}

		/* if current node is key without value, search in subnodes */
		off = 255;
		TKVDB_SUBNODE_SEARCH(c->tr, node, next, off, 0);

		if (!next) {
			if (node->c.type & TKVDB_NODE_VAL) {
				TKVDB_EXEC(
					TKVDB_IMPL_CURSOR_PUSH(cr, node, -1)
				);
				break;
			} else {
				return TKVDB_CORRUPTED;
			}
		}

		TKVDB_EXEC( tkvdb_cursor_resize_prefix(cr, 1, 1) );

		c->prefix[c->prefix_size] = off;
		c->prefix_size++;

		TKVDB_EXEC( TKVDB_IMPL_CURSOR_PUSH(cr, node, off) );

		node = next;
	}

	return TKVDB_OK;
}

static TKVDB_RES
TKVDB_IMPL_CURSOR_LOAD_ROOT(tkvdb_cursor *cr)
{
	tkvdb_cursor_data *c = cr->data;
	tkvdb_tr_data *tr = c->tr->data;

	if (!tr->root) {
#ifndef TKVDB_PARAMS_NODBFILE
		/* empty root node */
		if (!tr->db) {
			/* and no database file */
			return TKVDB_EMPTY;
		}

		if (tr->db->info.filesize == 0) {
			/* database is empty */
			return TKVDB_EMPTY;
		}
		/* try to read root node */
		TKVDB_EXEC( TKVDB_IMPL_NODE_READ(c->tr,
			tr->db->info.footer.root_off,
			(TKVDB_MEMNODE_TYPE **)&(tr->root)) );
#else
		return TKVDB_EMPTY;
#endif
	}

	return TKVDB_OK;
}

static TKVDB_RES
TKVDB_IMPL_FIRST(tkvdb_cursor *cr)
{
	tkvdb_cursor_data *c = cr->data;
	tkvdb_tr_data *tr = c->tr->data;

	tkvdb_cursor_reset(cr);
	TKVDB_EXEC( TKVDB_IMPL_CURSOR_LOAD_ROOT(cr) );
	return TKVDB_IMPL_SMALLEST(cr, tr->root);
}

static TKVDB_RES
TKVDB_IMPL_LAST(tkvdb_cursor *cr)
{
	tkvdb_cursor_data *c = cr->data;
	tkvdb_tr_data *tr = c->tr->data;

	tkvdb_cursor_reset(cr);
	TKVDB_EXEC( TKVDB_IMPL_CURSOR_LOAD_ROOT(cr) );
	return TKVDB_IMPL_BIGGEST(cr, tr->root);
}

static TKVDB_RES
TKVDB_IMPL_NEXT(tkvdb_cursor *cr)
{
	int *off;
	TKVDB_MEMNODE_TYPE *node, *next;
	tkvdb_cursor_data *c = cr->data;

	for (;;) {
		if (c->stack_size < 1) {
			break;
		}

		/* get node from stack's top */
		node = c->stack[c->stack_size - 1].node;
		off = &(c->stack[c->stack_size - 1].off);
		(*off)++;

		if (*off > 255) {
			TKVDB_EXEC( TKVDB_IMPL_CURSOR_POP(cr) );
			continue;
		}

		TKVDB_SUBNODE_SEARCH(c->tr, node, next, *off, 1);

		if (next) {
			/* expand cursor key */
			TKVDB_EXEC( tkvdb_cursor_resize_prefix(cr, 1, 1) );
			c->prefix[c->prefix_size] = *off;
			c->prefix_size++;

			return TKVDB_IMPL_SMALLEST(cr, next);
		}

		/* pop */
		TKVDB_EXEC( TKVDB_IMPL_CURSOR_POP(cr) );
	}

	return TKVDB_NOT_FOUND;
}

static TKVDB_RES
TKVDB_IMPL_PREV(tkvdb_cursor *cr)
{
	int *off;
	TKVDB_MEMNODE_TYPE *node, *next = NULL;
	tkvdb_cursor_data *c = cr->data;

	for (;;) {
		if (c->stack_size < 1) {
			return TKVDB_NOT_FOUND;
		}

		node = c->stack[c->stack_size - 1].node;
		off = &(c->stack[c->stack_size - 1].off);
		(*off)--;

		/* special case? */
		if ((*off == -1) && (node->c.type & TKVDB_NODE_VAL)) {
			break;
		}

		if (*off < 0) {
			TKVDB_EXEC( TKVDB_IMPL_CURSOR_POP(cr) );
			continue;
		}

		TKVDB_SUBNODE_SEARCH(c->tr, node, next, *off, 0);

		if (next) {
			TKVDB_EXEC( tkvdb_cursor_resize_prefix(cr, 1, 1) );
			c->prefix[c->prefix_size] = *off;
			c->prefix_size++;

			return TKVDB_IMPL_BIGGEST(cr, next);
		}

		if (node->c.type & TKVDB_NODE_VAL) {
			break;
		}

		TKVDB_EXEC( TKVDB_IMPL_CURSOR_POP(cr) );
	}

	CURSOR_UPDATE_VAL();

	return TKVDB_OK;
}

/* seek to key (or to nearest key, less or greater) */
static TKVDB_RES
TKVDB_IMPL_SEEK(tkvdb_cursor *cr, const tkvdb_datum *key, TKVDB_SEEK seek)
{
	TKVDB_MEMNODE_TYPE *node, *next;

	const uint8_t *sym;
	size_t pi;
	int off = 0;
	unsigned char *prefix_val_meta;
	tkvdb_cursor_data *c = cr->data;
	tkvdb_tr_data *tr = c->tr->data;

	TKVDB_EXEC( TKVDB_IMPL_CURSOR_LOAD_ROOT(cr) );
	tkvdb_cursor_reset(cr);

	node = tr->root;
	sym = key->data;

next_node:
	TKVDB_SKIP_RNODES(node);

	pi = 0;
	if (node->c.type & TKVDB_NODE_LEAF) {
		prefix_val_meta =
			((TKVDB_MEMNODE_TYPE_LEAF *)node)->prefix_val_meta;
	} else {
		prefix_val_meta = node->prefix_val_meta;
	}

next_byte:

	if (sym >= ((uint8_t *)key->data + key->size)) {
		/* end of key */
		if ((pi == node->c.prefix_size)
			&& (node->c.type & TKVDB_NODE_VAL)) {
			TKVDB_EXEC ( TKVDB_IMPL_CURSOR_APPEND(cr,
				prefix_val_meta, node->c.prefix_size) );
			TKVDB_EXEC ( TKVDB_IMPL_CURSOR_PUSH(cr, node, -1) );

			return TKVDB_OK;
		}

		if (seek == TKVDB_SEEK_EQ) {
			tkvdb_cursor_reset(cr);
			return TKVDB_NOT_FOUND;
		}

		TKVDB_EXEC ( TKVDB_IMPL_SMALLEST(cr, node) );
		if (seek == TKVDB_SEEK_LE) {
			return TKVDB_IMPL_PREV(cr);
		}

		return TKVDB_OK;
	}


	if (pi >= node->c.prefix_size) {
		/* end of prefix (but not the key) */
		next = NULL;
		TKVDB_SUBNODE_NEXT(c->tr, node, next, *sym);
		if (next) {
			TKVDB_EXEC ( TKVDB_IMPL_CURSOR_APPEND(cr,
				prefix_val_meta, node->c.prefix_size) );
			TKVDB_EXEC ( TKVDB_IMPL_CURSOR_APPEND_SYM(cr, *sym) );
			TKVDB_EXEC ( TKVDB_IMPL_CURSOR_PUSH(cr, node, *sym) );

			node = next;
			sym++;
			goto next_node;
		}

		if (seek == TKVDB_SEEK_EQ) {
			tkvdb_cursor_reset(cr);
			return TKVDB_NOT_FOUND;
		}

		off = *sym;
		if (seek == TKVDB_SEEK_LE) {
			TKVDB_SUBNODE_SEARCH(c->tr, node, next, off, 0);
			if (next) {
				TKVDB_EXEC (
					TKVDB_IMPL_CURSOR_APPEND(cr,
						prefix_val_meta,
						node->c.prefix_size)
				);
				TKVDB_EXEC (
					TKVDB_IMPL_CURSOR_APPEND_SYM(cr, off)
				);
				TKVDB_EXEC (
					TKVDB_IMPL_CURSOR_PUSH(cr, node, off)
				);
				return TKVDB_IMPL_BIGGEST(cr, next);
			}
			if (node->c.type & TKVDB_NODE_VAL) {
				TKVDB_EXEC (
					TKVDB_IMPL_CURSOR_APPEND(cr,
						prefix_val_meta,
						node->c.prefix_size)
				);
				TKVDB_EXEC (
					TKVDB_IMPL_CURSOR_PUSH(cr, node, *sym)
				);

				return TKVDB_OK;
			}
			TKVDB_EXEC ( TKVDB_IMPL_SMALLEST(cr, node) );
			return TKVDB_IMPL_PREV(cr);
		} else {
			/* greater */
			TKVDB_SUBNODE_SEARCH(c->tr, node, next, off, 1);
			if (next) {
				TKVDB_EXEC (
					TKVDB_IMPL_CURSOR_APPEND(cr,
						prefix_val_meta,
						node->c.prefix_size)
				);
				TKVDB_EXEC (
					TKVDB_IMPL_CURSOR_APPEND_SYM(cr, off)
				);
				TKVDB_EXEC (
					TKVDB_IMPL_CURSOR_PUSH(cr, node, off)
				);
				return TKVDB_IMPL_SMALLEST(cr, next);
			}

			TKVDB_EXEC ( TKVDB_IMPL_BIGGEST(cr, node) );
			return TKVDB_IMPL_NEXT(cr);
		}
	}


	if (prefix_val_meta[pi] != *sym) {
		if (seek == TKVDB_SEEK_EQ) {
			tkvdb_cursor_reset(cr);
			return TKVDB_NOT_FOUND;
		}

		if (seek == TKVDB_SEEK_LE) {
			if (prefix_val_meta[pi] < *sym) {
				/* symbol in prefix is lesser than in key */
				return TKVDB_IMPL_BIGGEST(cr, node);
			}
			/* not optimal, we push node and pop it in prev() */

			TKVDB_EXEC (TKVDB_IMPL_CURSOR_APPEND(cr,
				prefix_val_meta, node->c.prefix_size) );

			TKVDB_EXEC ( TKVDB_IMPL_CURSOR_PUSH(cr, node, -1) );

			return TKVDB_IMPL_PREV(cr);
		} else {
			/* greater */
			if (prefix_val_meta[pi] > *sym) {
				return TKVDB_IMPL_SMALLEST(cr, node);
			}

			TKVDB_EXEC (TKVDB_IMPL_CURSOR_APPEND(cr,
				prefix_val_meta, node->c.prefix_size) );

			TKVDB_EXEC ( TKVDB_IMPL_CURSOR_PUSH(cr, node, 255) );

			return TKVDB_IMPL_NEXT(cr);
		}
	}

	sym++;
	pi++;
	goto next_byte;

	/* unreachable */
	return TKVDB_OK;
}

#undef CURSOR_UPDATE_VAL

