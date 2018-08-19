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


static TKVDB_RES
TKVDB_IMPL_DO_DEL(tkvdb_tr *trns, TKVDB_MEMNODE_TYPE *node,
	TKVDB_MEMNODE_TYPE *prev, int prev_off, int del_pfx)
{
	int i, n_subnodes = 0, concat_sym = -1;
	TKVDB_MEMNODE_TYPE *new_node, *old_node;
	tkvdb_tr_data *tr = trns->data;

	if (!prev) {
		/* remove root node */
		TKVDB_IMPL_NODE_FREE(node);
		node = TKVDB_IMPL_NODE_NEW(trns, 0, 0, NULL, 0, NULL);
		if (!node) {
			return TKVDB_ENOMEM;
		}
		tr->root = node;

		return TKVDB_OK;
	}

	if (del_pfx) {
		prev->next[prev_off] = NULL;
		prev->fnext[prev_off] = 0;
		TKVDB_IMPL_NODE_FREE(node);
		return TKVDB_OK;
	} else if (node->c.type & TKVDB_NODE_VAL) {
		/* check if we have at least 1 subnode */
		if (!(node->c.type & TKVDB_NODE_LEAF)) {
			for (i=0; i<256; i++) {
				if (node->next[i] || node->fnext[i]) {
					n_subnodes = 1;
					break;
				}
			}
		}

		if (!n_subnodes) {
			/* no subnodes, delete node */
			prev->next[prev_off] = NULL;
			prev->fnext[prev_off] = 0;
			TKVDB_IMPL_NODE_FREE(node);
			return TKVDB_OK;
		}
		/* we have subnodes, so just clear value bit */
		node->c.type &= ~TKVDB_NODE_VAL;
		return TKVDB_OK;
	} else {
		return TKVDB_NOT_FOUND;
	}

	if (prev->c.type & TKVDB_NODE_VAL) {
		return TKVDB_OK;
	}

	/* calculate number of subnodes in parent (prev) */
	for (i=0; i<256; i++) {
		if (prev->next[i] || prev->fnext[i]) {
			n_subnodes++;
			if (n_subnodes > 1) {
				/* more than one subnode */
				return TKVDB_OK;
			}
			concat_sym = i;
		}
	}

	if (n_subnodes == 0) {
		return TKVDB_CORRUPTED;
	}


	/* we have parent node with just one subnode */
	old_node = prev->next[concat_sym];
	if (!old_node) {
		TKVDB_EXEC( TKVDB_IMPL_NODE_READ(trns, prev->fnext[concat_sym],
			&old_node) );
	}
	/* allocate new (concatenated) node */
	new_node = TKVDB_IMPL_NODE_ALLOC(trns, sizeof(TKVDB_MEMNODE_TYPE)
		+ prev->c.prefix_size + 1
		+ old_node->c.prefix_size
		+ old_node->c.val_size + old_node->c.meta_size);
	if (!new_node) {
		return TKVDB_ENOMEM;
	}

	new_node->c.type = old_node->c.type;
	new_node->c.prefix_size = prev->c.prefix_size
		+ 1 + old_node->c.prefix_size;
	new_node->c.val_size = old_node->c.val_size;
	new_node->c.meta_size = old_node->c.meta_size;

	if (prev->c.prefix_size > 0) {
		memcpy(new_node->prefix_val_meta, prev->prefix_val_meta,
			prev->c.prefix_size);
	}
	new_node->prefix_val_meta[prev->c.prefix_size] = concat_sym;
	if (old_node->c.prefix_size > 0) {
		memcpy(new_node->prefix_val_meta + prev->c.prefix_size + 1,
			old_node->prefix_val_meta,
			old_node->c.prefix_size);
	}

	if (old_node->c.val_size > 0) {
		memcpy(new_node->prefix_val_meta + new_node->c.prefix_size,
			old_node->prefix_val_meta + old_node->c.prefix_size,
			old_node->c.val_size);
	}
	memcpy(new_node->next, old_node->next,
		sizeof(TKVDB_MEMNODE_TYPE *) * 256);
	memcpy(new_node->fnext, old_node->fnext, sizeof(uint64_t) * 256);

	new_node->c.disk_size = 0;
	new_node->c.disk_off = 0;

	TKVDB_REPLACE_NODE(prev, new_node);

	return TKVDB_OK;
}

static TKVDB_RES
TKVDB_IMPL_DEL(tkvdb_tr *trns, const tkvdb_datum *key, int del_pfx)
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

	/* check root */
	if (tr->root == NULL) {
		if (tr->db && (tr->db->info.filesize > 0)) {
			/* we have underlying non-empty db file */
			TKVDB_EXEC( TKVDB_IMPL_NODE_READ(trns,
				tr->db->info.footer.root_off,
				(TKVDB_MEMNODE_TYPE **)&(tr->root)) );
		} else {
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

next_byte:

	if (sym >= ((unsigned char *)key->data + key->size)) {
		/* end of key */
		if (pi == node->c.prefix_size) {
			/* exact match */
			return TKVDB_IMPL_DO_DEL(trns, node, prev, prev_off,
				del_pfx);
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
		} else if (tr->db && (node->fnext[*sym] != 0)) {
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
		} else {
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



