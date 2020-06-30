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

#define TKVDB_TRIGGERS_META_SIZE(T) (T->meta_size)

#define TKVDB_TRIGGERS_UPDATE(T)                                            \
do {                                                                        \
	T->info.type = TKVDB_TRIGGER_UPDATE;                                \
	TKVDB_CALL_ALL_TRIGGER_FUNCTIONS(T);                                \
} while (0)


#define TKVDB_TRIGGERS_NEWROOT(T, N)                                        \
do {                                                                        \
	T->info.type = TKVDB_TRIGGER_INSERT_NEWROOT;                        \
	T->info.newroot = TKVDB_META_ADDR_LEAF(N);                          \
	TKVDB_CALL_ALL_TRIGGER_FUNCTIONS(T);                                \
} while (0)

#define TKVDB_TRIGGERS_SUBKEY(T, N)                                         \
do {                                                                        \
	T->info.type = TKVDB_TRIGGER_INSERT_SUBKEY;                         \
	T->info.newroot = TKVDB_META_ADDR(N);                               \
	TKVDB_CALL_ALL_TRIGGER_FUNCTIONS(T);                                \
} while (0)

#define TKVDB_TRIGGERS_SHORTER(T, N, R)                                     \
do {                                                                        \
	T->info.type = TKVDB_TRIGGER_INSERT_SHORTER;                        \
	T->info.newroot = TKVDB_META_ADDR_NONLEAF(N);                       \
	T->info.subnode1 = TKVDB_META_ADDR(R);                              \
	TKVDB_CALL_ALL_TRIGGER_FUNCTIONS(T);                                \
} while (0)

#define TKVDB_TRIGGERS_LONGER(T, N, R)                                      \
do {                                                                        \
	T->info.type = TKVDB_TRIGGER_INSERT_LONGER;                         \
	T->info.newroot = TKVDB_META_ADDR_NONLEAF(N);                       \
	T->info.subnode1 = TKVDB_META_ADDR_LEAF(R);                         \
	TKVDB_CALL_ALL_TRIGGER_FUNCTIONS(T);                                \
} while (0)

#define TKVDB_TRIGGERS_NEWNODE(T, N, R)                                     \
do {                                                                        \
	T->info.type = TKVDB_TRIGGER_INSERT_NEWNODE;                        \
	T->info.newroot = TKVDB_META_ADDR_NONLEAF(N);                       \
	T->info.subnode1 = TKVDB_META_ADDR(R);                              \
	TKVDB_CALL_ALL_TRIGGER_FUNCTIONS(T);                                \
} while (0)

#define TKVDB_TRIGGERS_SPLIT(T, N, R1, R2)                                  \
do {                                                                        \
	T->info.type = TKVDB_TRIGGER_INSERT_SPLIT;                          \
	T->info.newroot = TKVDB_META_ADDR_NONLEAF(N);                       \
	T->info.subnode1 = TKVDB_META_ADDR(R1);                             \
	T->info.subnode2 = TKVDB_META_ADDR_LEAF(R2);                        \
	TKVDB_CALL_ALL_TRIGGER_FUNCTIONS(T);                                \
} while (0)

#else

#define TKVDB_TRIGGERS_META_SIZE(T) 0

#define TKVDB_TRIGGERS_UPDATE(T)
#define TKVDB_TRIGGERS_NEWROOT(T, N)
#define TKVDB_TRIGGERS_SUBKEY(T, N)
#define TKVDB_TRIGGERS_SHORTER(T, N, R)
#define TKVDB_TRIGGERS_LONGER(T, N, R)
#define TKVDB_TRIGGERS_NEWNODE(T, N, R)
#define TKVDB_TRIGGERS_SPLIT(T, N, R1, R2)

#endif


/* add key-value pair to memory transaction */
static TKVDB_RES
#ifdef TKVDB_TRIGGER
TKVDB_IMPL_PUT(tkvdb_tr *trns, const tkvdb_datum *key, const tkvdb_datum *val,
	tkvdb_triggers *triggers)
#else
TKVDB_IMPL_PUT(tkvdb_tr *trns, const tkvdb_datum *key, const tkvdb_datum *val)
#endif
{
	const unsigned char *sym;  /* pointer to current symbol in key */
	TKVDB_MEMNODE_TYPE *node;  /* current node */
	size_t pi;                 /* prefix index */
	/* replaced nodes chain start */
	TKVDB_MEMNODE_TYPE *rnodes_chain = NULL;

	/* pointer to data of node(prefix, value, metadata)
	   it can be different for leaf and ordinary nodes */
	uint8_t *prefix_val_meta;

	tkvdb_tr_data *tr = trns->data;

#ifdef TKVDB_TRIGGER
	/* resets triggers stack to initial state */
	triggers->stack.size = 0;
#endif


	if (!tr->started) {
		return TKVDB_NOT_STARTED;
	}

	/* new root */
	if (tr->root == NULL) {
		TKVDB_MEMNODE_TYPE *new_root;
#ifndef TKVDB_PARAMS_NODBFILE
		if (tr->db && (tr->db->info.filesize > 0)) {
			/* we have underlying non-empty db file */
			TKVDB_EXEC( TKVDB_IMPL_NODE_READ(trns,
				tr->db->info.footer.root_off, &new_root) );

			tr->root = new_root;
		} else 
#endif
		{
			new_root = TKVDB_IMPL_NODE_NEW(trns,
				TKVDB_NODE_VAL | TKVDB_NODE_LEAF,
				key->size, key->data, val->size, val->data,
				TKVDB_TRIGGERS_META_SIZE(triggers), NULL);
			if (!new_root) {
				return TKVDB_ENOMEM;
			}

			TKVDB_TRIGGERS_NEWROOT(triggers, new_root);

			tr->root = new_root;
			return TKVDB_OK;
		}
	}

	sym = key->data;
	node = tr->root;

next_node:
	rnodes_chain = node;
	TKVDB_SKIP_RNODES(node);

	if (node->c.type & TKVDB_NODE_LEAF) {
		prefix_val_meta =
			((TKVDB_MEMNODE_TYPE_LEAF *)node)->prefix_val_meta;
	} else {
		prefix_val_meta = node->prefix_val_meta;
	}

	TKVDB_TRIGGER_NODE_PUSH(triggers, node, prefix_val_meta);

	pi = 0;

next_byte:

/* end of key
  Here we have two cases:
  [1][2][3][4][5][6] - prefix
  [1][2][3] - new key

  or exact match:
  [1][2][3][4][5][6] - prefix
  [1][2][3][4][5][6] - new key
*/
	if (sym >= ((unsigned char *)key->data + key->size)) {
		TKVDB_MEMNODE_TYPE *newroot, *subnode_rest;

		if (pi == node->c.prefix_size) {
			/* exact match */
			if ((node->c.type & TKVDB_NODE_VAL)
				&& (node->c.val_size == val->size)) {

				/* same value size, so copy new value
					and return */
				TKVDB_TRIGGERS_UPDATE(triggers);

				memcpy(prefix_val_meta
					+ node->c.prefix_size
					+ TKVDB_VAL_ALIGN_PAD(node),
					val->data, val->size);
				return TKVDB_OK;
			}

			/* value with different size or non-value node,
				create new node */
			newroot = TKVDB_IMPL_NODE_NEW(trns,
				node->c.type | TKVDB_NODE_VAL,
				pi, prefix_val_meta,
				val->size, val->data,
				node->c.meta_size,
				prefix_val_meta + node->c.prefix_size
					+ TKVDB_VAL_ALIGN_PAD(node)
					+ node->c.val_size);
			if (!newroot) return TKVDB_ENOMEM;

			TKVDB_IMPL_CLONE_SUBNODES(newroot, node);

			if (node->c.type & TKVDB_NODE_VAL) {
				TKVDB_TRIGGERS_UPDATE(triggers);
			} else {
				TKVDB_TRIGGERS_SUBKEY(triggers, newroot);
			}

			TKVDB_REPLACE_NODE(!tr->params.tr_buf_dynalloc,
				rnodes_chain, node, newroot);

			return TKVDB_OK;
		}

/* split node with prefix
  [1][2][3][4][5][6] - prefix
  [1][2][3] - new key

  becomes
  [1][2][3] - new root
  next['4'] => [5][6] - tail
*/
		newroot = TKVDB_IMPL_NODE_NEW(trns, TKVDB_NODE_VAL, pi,
			prefix_val_meta,
			val->size, val->data,
			TKVDB_TRIGGERS_META_SIZE(triggers), NULL);
		if (!newroot) return TKVDB_ENOMEM;

		subnode_rest = TKVDB_IMPL_NODE_NEW(trns,
			node->c.type,
			node->c.prefix_size - pi - 1,
			prefix_val_meta + pi + 1,
			node->c.val_size,
			prefix_val_meta + TKVDB_VAL_ALIGN_PAD(node)
				+ node->c.prefix_size,
			node->c.meta_size,
			prefix_val_meta + node->c.prefix_size
				+ TKVDB_VAL_ALIGN_PAD(node)
				+ node->c.val_size);
		if (!subnode_rest) {
			if (tr->params.tr_buf_dynalloc) {
				free(newroot);
			}
			return TKVDB_ENOMEM;
		}
		TKVDB_IMPL_CLONE_SUBNODES(subnode_rest, node);

		newroot->next[prefix_val_meta[pi]] = subnode_rest;
		newroot->c.nsubnodes += 1;

		TKVDB_TRIGGERS_SHORTER(triggers, newroot, subnode_rest);

		TKVDB_REPLACE_NODE(!tr->params.tr_buf_dynalloc,
			rnodes_chain, node, newroot);

		return TKVDB_OK;
	}

/* end of prefix
  [1][2][3][4][5][6] - old prefix
  [1][2][3][4][5][6][7][8][9]- new prefix

  so we hold old node and change only pointer to next
  [1][2][3][4][5][6]
  next['7'] => [8][9] - tail
*/
	if (pi >= node->c.prefix_size) {
		if (node->c.type & TKVDB_NODE_LEAF) {
			/* create 2 nodes */
			TKVDB_MEMNODE_TYPE *newroot, *subnode_rest;

			newroot = TKVDB_IMPL_NODE_NEW(trns,
				node->c.type & (~TKVDB_NODE_LEAF),
				node->c.prefix_size,
				prefix_val_meta,
				node->c.val_size,
				prefix_val_meta
					+ TKVDB_VAL_ALIGN_PAD(node)
					+ node->c.prefix_size,
				node->c.meta_size,
				prefix_val_meta + node->c.prefix_size
					+ TKVDB_VAL_ALIGN_PAD(node)
					+ node->c.val_size);
			if (!newroot) {
				if (tr->params.tr_buf_dynalloc) {
					free(newroot);
				}
				return TKVDB_ENOMEM;
			}

			subnode_rest = TKVDB_IMPL_NODE_NEW(trns,
				TKVDB_NODE_VAL | TKVDB_NODE_LEAF,
				key->size -
					(sym - (unsigned char *)key->data) - 1,
				sym + 1,
				val->size, val->data,
				TKVDB_TRIGGERS_META_SIZE(triggers), NULL);
			if (!subnode_rest) return TKVDB_ENOMEM;

			newroot->c.nsubnodes += 1;
			newroot->next[*sym] = subnode_rest;

			TKVDB_TRIGGERS_LONGER(triggers, newroot, subnode_rest);

			TKVDB_REPLACE_NODE(!tr->params.tr_buf_dynalloc,
				rnodes_chain, node, newroot);

			return TKVDB_OK;
		} else if (node->next[*sym] != NULL) {
			/* continue with next node */
			node = node->next[*sym];
			sym++;
			goto next_node;
		}
#ifndef TKVDB_PARAMS_NODBFILE
		/* only if we have underlying db file */
		else if (tr->db && (node->fnext[*sym] != 0)) {
			TKVDB_MEMNODE_TYPE *tmp;

			/* load subnode from disk */
			TKVDB_EXEC( TKVDB_IMPL_NODE_READ(trns,
				node->fnext[*sym], &tmp) );

			node->next[*sym] = tmp;
			node = tmp;
			sym++;
			goto next_node;
		}
#endif
		else {
			/* non-leaf node */
			TKVDB_MEMNODE_TYPE *tmp;

			/* allocate tail */
			tmp = TKVDB_IMPL_NODE_NEW(trns,
				TKVDB_NODE_VAL | TKVDB_NODE_LEAF,
				key->size -
					(sym - (unsigned char *)key->data) - 1,
				sym + 1,
				val->size, val->data,
				TKVDB_TRIGGERS_META_SIZE(triggers), NULL);
			if (!tmp) return TKVDB_ENOMEM;

			TKVDB_TRIGGERS_NEWNODE(triggers, node, tmp);

			node->next[*sym] = tmp;
			node->c.nsubnodes += 1; /* XXX: not atomic */
			return TKVDB_OK;
		}
	}

/* node prefix don't match with corresponding part of key
  [1][2][3][4][5][6] - old prefix
  [1][2][3][7][8][9][0]- new prefix

  [1][2][3] - new root
  next['4'] => [5][6] - tail from old prefix
  next['7'] => [8][9][0] - tail from new prefix
*/
	if (prefix_val_meta[pi] != *sym) {
		TKVDB_MEMNODE_TYPE *newroot, *subnode_rest, *subnode_key;

		/* split current node into 3 subnodes */
		newroot = TKVDB_IMPL_NODE_NEW(trns, 0, pi,
			prefix_val_meta, 0, NULL,
			node->c.meta_size,
			prefix_val_meta + node->c.prefix_size
				+ TKVDB_VAL_ALIGN_PAD(node)
				+ node->c.val_size);
		if (!newroot) return TKVDB_ENOMEM;

		/* rest of prefix (skip current symbol) */
		subnode_rest = TKVDB_IMPL_NODE_NEW(trns,
			node->c.type,
			node->c.prefix_size - pi - 1,
			prefix_val_meta + pi + 1,
			node->c.val_size,
			prefix_val_meta + TKVDB_VAL_ALIGN_PAD(node)
				+ node->c.prefix_size,
			node->c.meta_size,
			prefix_val_meta + node->c.prefix_size
				+ TKVDB_VAL_ALIGN_PAD(node)
				+ node->c.val_size);
		if (!subnode_rest) {
			if (tr->params.tr_buf_dynalloc) {
				free(newroot);
			}
			return TKVDB_ENOMEM;
		}
		TKVDB_IMPL_CLONE_SUBNODES(subnode_rest, node);

		/* rest of key */
		subnode_key = TKVDB_IMPL_NODE_NEW(trns,
			TKVDB_NODE_VAL | TKVDB_NODE_LEAF,
			key->size -
				(sym - (unsigned char *)key->data) - 1,
			sym + 1,
			val->size, val->data,
			TKVDB_TRIGGERS_META_SIZE(triggers), NULL);
		if (!subnode_key) {
			if (tr->params.tr_buf_dynalloc) {
				free(subnode_rest);
				free(newroot);
			}
			return TKVDB_ENOMEM;
		}

		newroot->next[prefix_val_meta[pi]] = subnode_rest;
		newroot->next[*sym] = subnode_key;
		newroot->c.nsubnodes += 2;

		TKVDB_TRIGGERS_SPLIT(triggers, newroot,
			subnode_rest, subnode_key);

		TKVDB_REPLACE_NODE(!tr->params.tr_buf_dynalloc,
			rnodes_chain, node, newroot);

		return TKVDB_OK;
	}

	sym++;
	pi++;
	goto next_byte;

	return TKVDB_OK;
}

#undef TKVDB_TRIGGERS_META_SIZE

#undef TKVDB_TRIGGER_NODE_PUSH

#undef TKVDB_TRIGGERS_UPDATE
#undef TKVDB_TRIGGERS_NEWROOT
#undef TKVDB_TRIGGERS_SUBKEY
#undef TKVDB_TRIGGERS_SHORTER
#undef TKVDB_TRIGGERS_LONGER
#undef TKVDB_TRIGGERS_NEWNODE
#undef TKVDB_TRIGGERS_SPLIT

#undef TKVDB_META_ADDR_LEAF
#undef TKVDB_META_ADDR_NONLEAF
#undef TKVDB_META_ADDR
#undef TKVDB_INC_VOID_PTR
#undef TKVDB_CALL_ALL_TRIGGER_FUNCTIONS

#undef TKVDB_VAL_ALIGN_PAD
