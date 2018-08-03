/*
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

/* get value for given key */

static TKVDB_RES
TKVDB_IMPL_GET(tkvdb_tr *trns, const tkvdb_datum *key, tkvdb_datum *val)
{
	const unsigned char *sym;
	size_t pi;
	TKVDB_MEMNODE_TYPE *node = NULL;
	uint64_t off;
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
	off = tr->db->info.footer.root_off;

next_node:
	TKVDB_SKIP_RNODES(node);
	pi = 0;

next_byte:

	if (sym >= ((unsigned char *)key->data + key->size)) {
		/* end of key */
		if ((pi == node->prefix_size)
			&& (node->type & TKVDB_NODE_VAL)) {
			/* exact match and node with value */
			val->size = node->val_size;
			val->data = node->prefix_val_meta + node->prefix_size;
			return TKVDB_OK;
		} else {
			return TKVDB_NOT_FOUND;
		}
	}

	if (pi >= node->prefix_size) {
		/* end of prefix */
		if (node->next[*sym] != NULL) {
			/* continue with next node */
			node = node->next[*sym];
			sym++;
			goto next_node;
		} else if (tr->db && (node->fnext[*sym] != 0)) {
			TKVDB_MEMNODE_TYPE *tmp;

			/* load subnode from disk */
			off = node->fnext[*sym];
			TKVDB_EXEC( TKVDB_IMPL_NODE_READ(trns, off, &tmp) );

			node->next[*sym] = tmp;
			node = tmp;
			sym++;
			goto next_node;
		} else {
			return TKVDB_NOT_FOUND;
		}
	}

	if (node->prefix_val_meta[pi] != *sym) {
		return TKVDB_NOT_FOUND;
	}

	sym++;
	pi++;
	goto next_byte;

	return TKVDB_OK;
}

