/*
 * Copyright (c) 2020, Vladimir Misyurov
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


/* get n-th subnode prefix, value and metadata */
static TKVDB_RES
TKVDB_IMPL_SUBNODE(tkvdb_tr *trns, void *node, int n, void **ret,
	tkvdb_datum *prefix, tkvdb_datum *val, tkvdb_datum *meta)
{
	unsigned char *prefix_val_meta;
	TKVDB_MEMNODE_TYPE *tmpnode;
	tkvdb_tr_data *tr = trns->data;

	if (!tr->started) {
		return TKVDB_NOT_STARTED;
	}

	if (node == NULL) {
		/* root node requested */
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

		tmpnode = tr->root;

		goto ok;
	}

	if ((n < 0) || (n > 255)) {
		return TKVDB_NOT_FOUND;
	}

	tmpnode = node;
	TKVDB_SKIP_RNODES(tmpnode);

	if (tmpnode->c.type & TKVDB_NODE_LEAF) {
		return TKVDB_NOT_FOUND;
	}

	tmpnode = tmpnode->next[n];
	if (tmpnode != NULL) {
		goto ok;
	}
#ifndef TKVDB_PARAMS_NODBFILE
	else if (tr->db && (tmpnode->fnext[n] != 0)) {
		uint64_t off;

		/* load subnode from disk */
		off = tmpnode->fnext[n];
		TKVDB_EXEC( TKVDB_IMPL_NODE_READ(trns, off, &tmpnode) );

		goto ok;
	}
#endif


	return TKVDB_NOT_FOUND;

ok:
	TKVDB_SKIP_RNODES(tmpnode);

	if (tmpnode->c.type & TKVDB_NODE_LEAF) {
		prefix_val_meta =
			((TKVDB_MEMNODE_TYPE_LEAF *)tmpnode)->prefix_val_meta;
	} else {
		prefix_val_meta = tmpnode->prefix_val_meta;
	}

	/* prefix */
	prefix->data = prefix_val_meta;
	prefix->size = tmpnode->c.prefix_size;

	/* value */
#ifdef TKVDB_PARAMS_ALIGN_VAL
	val->data = prefix_val_meta
		+ tmpnode->c.prefix_size + tmpnode->c.val_pad;
#else
	val->data = prefix_val_meta + tmpnode->c.prefix_size;
#endif
	val->size = tmpnode->c.val_size;

	/* metadata */
	meta->data = (char *)val->data + val->size;
	meta->size = tmpnode->c.meta_size;

	*ret = tmpnode;

	if (!(tmpnode->c.type & TKVDB_NODE_VAL)) {
		val->data = NULL;
	}

	return TKVDB_OK;
}

