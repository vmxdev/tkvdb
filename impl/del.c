/*
 * tkvdb
 *
 * Copyright (c) 2016-2020, Vladimir Misyurov
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

#include "impl/trigger.h"

#ifdef TKVDB_TRIGGER

#define TKVDB_TRIGGERS_DELROOT(T)                                           \
do {                                                                        \
	T->info.type = TKVDB_TRIGGER_DELETE_ROOT;                           \
	TKVDB_CALL_ALL_TRIGGER_FUNCTIONS(T);                                \
} while (0)

#define TKVDB_TRIGGERS_DELPREFIX(T, P, N)                                   \
do {                                                                        \
	T->info.type = TKVDB_TRIGGER_DELETE_PREFIX;                         \
	T->info.newroot = TKVDB_META_ADDR_NONLEAF(P);                             \
	T->info.subnode1 = TKVDB_META_ADDR(N);                                    \
	TKVDB_CALL_ALL_TRIGGER_FUNCTIONS(T);                                \
} while (0)

#define TKVDB_TRIGGERS_DELINTNODE(T, P, N)                                  \
do {                                                                        \
	T->info.type = TKVDB_TRIGGER_DELETE_INTNODE;                        \
	T->info.newroot = TKVDB_META_ADDR_NONLEAF(P);                             \
	T->info.subnode1 = TKVDB_META_ADDR(N);                                    \
	TKVDB_CALL_ALL_TRIGGER_FUNCTIONS(T);                                \
} while (0)

#define TKVDB_TRIGGERS_DELLEAF(T, P, N)                                     \
do {                                                                        \
	T->info.type = TKVDB_TRIGGER_DELETE_LEAF;                           \
	T->info.newroot = TKVDB_META_ADDR_NONLEAF(P);                             \
	T->info.subnode1 = TKVDB_META_ADDR(N);                                    \
	TKVDB_CALL_ALL_TRIGGER_FUNCTIONS(T);                                \
} while (0)

#else

#define TKVDB_TRIGGERS_DELROOT(T)
#define TKVDB_TRIGGERS_DELPREFIX(T, P, N)
#define TKVDB_TRIGGERS_DELINTNODE(T, P, N)
#define TKVDB_TRIGGERS_DELLEAF(T, P, N)

#endif

static TKVDB_RES
#ifdef TKVDB_TRIGGER
TKVDB_IMPL_DO_DEL(tkvdb_tr *trns, TKVDB_MEMNODE_TYPE *node,
	TKVDB_MEMNODE_TYPE *prev, int prev_off, int del_pfx,
	tkvdb_triggers *triggers)
#else
TKVDB_IMPL_DO_DEL(tkvdb_tr *trns, TKVDB_MEMNODE_TYPE *node,
	TKVDB_MEMNODE_TYPE *prev, int prev_off, int del_pfx)
#endif
{
	tkvdb_tr_data *tr = trns->data;

	if (!prev) {
		/* remove root node */
		TKVDB_TRIGGERS_DELROOT(triggers);

		TKVDB_IMPL_NODE_FREE(tr, node);
		node = TKVDB_IMPL_NODE_NEW(trns, 0, 0, NULL, 0, NULL, 0, NULL);
		if (!node) {
			return TKVDB_ENOMEM;
		}
		tr->root = node;

		return TKVDB_OK;
	}

	if (del_pfx) {
		TKVDB_TRIGGERS_DELPREFIX(triggers, prev, node);

		prev->next[prev_off] = NULL;
#ifndef TKVDB_PARAMS_NODBFILE
		prev->fnext[prev_off] = 0;
#endif
		TKVDB_IMPL_NODE_FREE(tr, node);
		return TKVDB_OK;
	} else if (node->c.type & TKVDB_NODE_VAL) {
		if (node->c.nsubnodes != 0) {
			TKVDB_TRIGGERS_DELINTNODE(triggers, prev, node);

			/* we have subnodes, so just clear value bit */
			node->c.type &= ~TKVDB_NODE_VAL;
		} else {
			TKVDB_TRIGGERS_DELLEAF(triggers, prev, node);

			/* no subnodes, delete node */
			prev->next[prev_off] = NULL;
#ifndef TKVDB_PARAMS_NODBFILE
			prev->fnext[prev_off] = 0;
#endif

			prev->c.nsubnodes -= 1; /* XXX: not atomic */

			TKVDB_IMPL_NODE_FREE(tr, node);
		}
	} else {
		return TKVDB_NOT_FOUND;
	}

	return TKVDB_OK;
}

static TKVDB_RES
#ifdef TKVDB_TRIGGER
TKVDB_IMPL_DEL(tkvdb_tr *trns, const tkvdb_datum *key, int del_pfx,
		tkvdb_triggers *triggers)
#else
TKVDB_IMPL_DEL(tkvdb_tr *trns, const tkvdb_datum *key, int del_pfx)
#endif
{
	const unsigned char *sym;
	TKVDB_MEMNODE_TYPE *node, *prev;
	size_t pi;
	unsigned char *prefix_val_meta;
	int prev_off = 0;
	tkvdb_tr_data *tr = trns->data;

	if (!tr->started) {
		return TKVDB_NOT_STARTED;
	}

#ifdef TKVDB_TRIGGER
	/* resets triggers stack to initial state */
	triggers->stack.size = 0;
#endif

	/* check root */
	if (tr->root == NULL) {
#ifndef TKVDB_PARAMS_NODBFILE
		if (tr->db && (tr->db->info.filesize > 0)) {
			/* we have underlying non-empty db file */
			TKVDB_EXEC( TKVDB_IMPL_NODE_READ(trns,
				tr->db->info.footer.root_off,
				(TKVDB_MEMNODE_TYPE **)&(tr->root)) );
		} else
#endif
		{
			return TKVDB_EMPTY;
		}
	}

	sym = key->data;
	node = tr->root;
	prev = NULL;

next_node:
	TKVDB_SKIP_RNODES(node);

	pi = 0;
	if (node->c.type & TKVDB_NODE_LEAF) {
		prefix_val_meta =
			((TKVDB_MEMNODE_TYPE_LEAF *)node)->prefix_val_meta;
	} else {
		prefix_val_meta = node->prefix_val_meta;
	}

	TKVDB_TRIGGER_NODE_PUSH(triggers, node, prefix_val_meta);

next_byte:

	if (sym >= ((unsigned char *)key->data + key->size)) {
		/* end of key */
		if ((pi == node->c.prefix_size) || (del_pfx)) {
			/* exact match or we should delete by prefix */
#ifdef TKVDB_TRIGGER
			return TKVDB_IMPL_DO_DEL(trns, node, prev, prev_off,
				del_pfx, triggers);
#else
			return TKVDB_IMPL_DO_DEL(trns, node, prev, prev_off,
				del_pfx);
#endif
		}
	}

	if (pi >= node->c.prefix_size) {
		/* end of prefix */
		if (node->next[*sym] != NULL) {
			/* continue with next node */
			prev = node;
			prev_off = *sym;

			node = node->next[*sym];
			sym++;
			goto next_node;
		}
#ifndef TKVDB_PARAMS_NODBFILE
		else if (tr->db && (node->fnext[*sym] != 0)) {
			TKVDB_MEMNODE_TYPE *tmp;

			/* load subnode from disk */
			TKVDB_EXEC( TKVDB_IMPL_NODE_READ(trns,
				node->fnext[*sym], &tmp) );

			prev = node;
			prev_off = *sym;

			node->next[*sym] = tmp;
			node = tmp;
			sym++;
			goto next_node;
		}
#endif
		else {
			return TKVDB_NOT_FOUND;
		}
	}

	if (prefix_val_meta[pi] != *sym) {
		return TKVDB_NOT_FOUND;
	}

	sym++;
	pi++;
	goto next_byte;

	return TKVDB_OK;
}

#undef TKVDB_TRIGGERS_DELROOT
#undef TKVDB_TRIGGERS_DELPREFIX
#undef TKVDB_TRIGGERS_DELINTNODE
#undef TKVDB_TRIGGERS_DELLEAF

#undef TKVDB_META_ADDR_LEAF
#undef TKVDB_META_ADDR_NONLEAF
#undef TKVDB_META_ADDR
#undef TKVDB_INC_VOID_PTR
#undef TKVDB_CALL_ALL_TRIGGER_FUNCTIONS

#undef TKVDB_TRIGGER_NODE_PUSH

#undef TKVDB_VAL_ALIGN_PAD
