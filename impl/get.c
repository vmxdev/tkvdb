/*
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

/* get value for given key */
static TKVDB_RES
#ifdef TKVDB_TRIGGER
TKVDB_IMPL_GET(tkvdb_tr *trns, const tkvdb_datum *key, tkvdb_datum *val,
	const tkvdb_triggers *triggers)
#else
TKVDB_IMPL_GET(tkvdb_tr *trns, const tkvdb_datum *key, tkvdb_datum *val)
#endif
{
	const unsigned char *sym;
	unsigned char *prefix_val_meta;
	size_t pi;
	TKVDB_MEMNODE_TYPE *node = NULL;
	tkvdb_tr_data *tr = trns->data;

	if (!tr->started) {
		return TKVDB_NOT_STARTED;
	}

#ifdef TKVDB_TRIGGER
	(void)triggers;
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
		if ((pi == node->c.prefix_size)
			&& (node->c.type & TKVDB_NODE_VAL)) {
			/* exact match and node with value */
			val->size = node->c.val_size;
#ifdef TKVDB_PARAMS_ALIGN_VAL
			val->data = prefix_val_meta + node->c.prefix_size
				+ node->c.val_pad;
#else
			val->data = prefix_val_meta + node->c.prefix_size;
#endif
			return TKVDB_OK;
		} else {
			return TKVDB_NOT_FOUND;
		}
	}

	if (pi >= node->c.prefix_size) {
		/* end of prefix */
		if (node->c.type & TKVDB_NODE_LEAF) {
			return TKVDB_NOT_FOUND;
		} else if (node->next[*sym] != NULL) {
			/* continue with next node */
			node = node->next[*sym];
			sym++;
			goto next_node;
		}
#ifndef TKVDB_PARAMS_NODBFILE
		else if (tr->db && (node->fnext[*sym] != 0)) {
			TKVDB_MEMNODE_TYPE *tmp;
			uint64_t off;

			/* load subnode from disk */
			off = node->fnext[*sym];
			TKVDB_EXEC( TKVDB_IMPL_NODE_READ(trns, off, &tmp) );

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

