/*
 * tkvdb
 *
 * Copyright (c) 2016-2019, Vladimir Misyurov
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
	tkvdb_tr_data *tr = trns->data;

	if (!prev) {
		/* remove root node */
		TKVDB_IMPL_NODE_FREE(tr, node);
		node = TKVDB_IMPL_NODE_NEW(trns, 0, 0, NULL, 0, NULL, 0, NULL);
		if (!node) {
			return TKVDB_ENOMEM;
		}
		tr->root = node;

		return TKVDB_OK;
	}

	if (del_pfx) {
		prev->next[prev_off] = NULL;
#ifndef TKVDB_PARAMS_NODBFILE
		prev->fnext[prev_off] = 0;
#endif
		TKVDB_IMPL_NODE_FREE(tr, node);
		return TKVDB_OK;
	} else if (node->c.type & TKVDB_NODE_VAL) {
		if (node->c.nsubnodes != 0) {
			/* we have subnodes, so just clear value bit */
			node->c.type &= ~TKVDB_NODE_VAL;
		} else {
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


