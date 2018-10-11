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

/* add key-value pair to memory transaction */
static TKVDB_RES
TKVDB_IMPL_PUT(tkvdb_tr *trns, const tkvdb_datum *key, const tkvdb_datum *val)
{
	const unsigned char *sym;  /* pointer to current symbol in key */
	TKVDB_MEMNODE_TYPE *node;  /* current node */
	size_t pi;                 /* prefix index */

	/* pointer to data of node(prefix, value, metadata)
	   it can be different for leaf and ordinary nodes */
	uint8_t *prefix_val_meta;

	tkvdb_tr_data *tr = trns->data;

#ifdef TKVDB_PARAMS_ALIGN_VAL
#define VAL_ALIGN_PAD (node->c.val_pad)
#else
#define VAL_ALIGN_PAD 0
#endif

	if (!tr->started) {
		return TKVDB_NOT_STARTED;
	}

	/* new root */
	if (tr->root == NULL) {
#ifndef TKVDB_PARAMS_NODBFILE
		if (tr->db && (tr->db->info.filesize > 0)) {
			/* we have underlying non-empty db file */
			TKVDB_EXEC( TKVDB_IMPL_NODE_READ(trns,
				tr->db->info.footer.root_off,
				((TKVDB_MEMNODE_TYPE **)&(tr->root))) );
		} else 
#endif
		{
			tr->root = TKVDB_IMPL_NODE_NEW(trns,
				TKVDB_NODE_VAL | TKVDB_NODE_LEAF,
				key->size, key->data, val->size, val->data);
			if (!tr->root) {
				return TKVDB_ENOMEM;
			}
			return TKVDB_OK;
		}
	}

	sym = key->data;
	node = tr->root;

next_node:
	TKVDB_SKIP_RNODES(node);

	if (node->c.type & TKVDB_NODE_LEAF) {
		prefix_val_meta =
			((TKVDB_MEMNODE_TYPE_LEAF *)node)->prefix_val_meta;
	} else {
		prefix_val_meta = node->prefix_val_meta;
	}

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
	if (sym >= ((unsigned char *)key->data + key->size)) {
		TKVDB_MEMNODE_TYPE *newroot, *subnode_rest;

		if (pi == node->c.prefix_size) {
			/* exact match */
			if ((node->c.val_size == val->size)
				&& (val->size != 0)) {
				/* same value size, so copy new value and
					return */
				memcpy(prefix_val_meta
					+ node->c.prefix_size
					+ VAL_ALIGN_PAD,
					val->data, val->size);
				return TKVDB_OK;
			}

			newroot = TKVDB_IMPL_NODE_NEW(trns,
				node->c.type | TKVDB_NODE_VAL,
				pi, prefix_val_meta,
				val->size, val->data);
			if (!newroot) return TKVDB_ENOMEM;

			TKVDB_IMPL_CLONE_SUBNODES(newroot, node);

			TKVDB_REPLACE_NODE(node, newroot);

			return TKVDB_OK;
		}

/* split node with prefix
  [p][r][e][f][i][x] - prefix
  [p][r][e] - new key

  becomes
  [p][r][e] - new root
  next['f'] => [i][x] - tail
*/
		newroot = TKVDB_IMPL_NODE_NEW(trns, TKVDB_NODE_VAL, pi,
			prefix_val_meta,
			val->size, val->data);
		if (!newroot) return TKVDB_ENOMEM;

		subnode_rest = TKVDB_IMPL_NODE_NEW(trns,
			node->c.type & (~TKVDB_NODE_LEAF),
			node->c.prefix_size - pi - 1,
			prefix_val_meta + pi + 1,
			node->c.val_size,
			prefix_val_meta + VAL_ALIGN_PAD + node->c.prefix_size);
		if (!subnode_rest) {
			if (tr->params.tr_buf_dynalloc) {
				free(newroot);
			}
			return TKVDB_ENOMEM;
		}
		TKVDB_IMPL_CLONE_SUBNODES(subnode_rest, node);

		newroot->next[prefix_val_meta[pi]] = subnode_rest;

		TKVDB_REPLACE_NODE(node, newroot);

		return TKVDB_OK;
	}

/* end of prefix
  [p][r][e][f][i][x] - old prefix
  [p][r][e][f][i][x][n][e][w]- new prefix

  so we hold old node and change only pointer to next
  [p][r][e][f][i][x]
  next['n'] => [e][w] - tail
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
					+ VAL_ALIGN_PAD
					+ node->c.prefix_size);
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
				val->size, val->data);
			if (!subnode_rest) return TKVDB_ENOMEM;

			newroot->next[*sym] = subnode_rest;

			TKVDB_REPLACE_NODE(node, newroot);

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
			TKVDB_MEMNODE_TYPE *tmp;

			/* allocate tail */
			tmp = TKVDB_IMPL_NODE_NEW(trns,
				TKVDB_NODE_VAL | TKVDB_NODE_LEAF,
				key->size -
					(sym - (unsigned char *)key->data) - 1,
				sym + 1,
				val->size, val->data);
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
	if (prefix_val_meta[pi] != *sym) {
		TKVDB_MEMNODE_TYPE *newroot, *subnode_rest, *subnode_key;

		/* split current node into 3 subnodes */
		newroot = TKVDB_IMPL_NODE_NEW(trns, 0, pi,
			prefix_val_meta, 0, NULL);
		if (!newroot) return TKVDB_ENOMEM;

		/* rest of prefix (skip current symbol) */
		subnode_rest = TKVDB_IMPL_NODE_NEW(trns,
			node->c.type & (~TKVDB_NODE_LEAF),
			node->c.prefix_size - pi - 1,
			prefix_val_meta + pi + 1,
			node->c.val_size,
			prefix_val_meta + VAL_ALIGN_PAD + node->c.prefix_size);
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
			val->size, val->data);
		if (!subnode_key) {
			if (tr->params.tr_buf_dynalloc) {
				free(subnode_rest);
				free(newroot);
			}
			return TKVDB_ENOMEM;
		}

		newroot->next[prefix_val_meta[pi]] = subnode_rest;
		newroot->next[*sym] = subnode_key;

		TKVDB_REPLACE_NODE(node, newroot);

		return TKVDB_OK;
	}

	sym++;
	pi++;
	goto next_byte;

	return TKVDB_OK;
#undef VAL_ALIGN_PAD
}

