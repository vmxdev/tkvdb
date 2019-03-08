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


/* reset transaction to initial state */
static void
TKVDB_IMPL_TR_RESET(tkvdb_tr *trns)
{
	tkvdb_tr_data *tr = trns->data;

	if (tr->params.tr_buf_dynalloc) {
		if (tr->root) {
			TKVDB_IMPL_NODE_FREE(tr, tr->root);
		}
	} else {
		tr->tr_buf_ptr = tr->tr_buf;
	}

	tr->root = NULL;

	tr->tr_buf_allocated = 0;
	if (!tr->params.autobegin) {
		tr->started = 0;
	}
}

static void
TKVDB_IMPL_TR_FREE(tkvdb_tr *trns)
{
	tkvdb_tr_data *tr = trns->data;

	if (tr->params.tr_buf_dynalloc) {
		TKVDB_IMPL_TR_RESET(trns);
	} else {
		free(tr->tr_buf);
	}

	free(tr->stack);
	tr->stack = NULL;

	free(tr);
	free(trns);
}

static TKVDB_RES
TKVDB_IMPL_ROLLBACK(tkvdb_tr *tr)
{
	TKVDB_IMPL_TR_RESET(tr);

	return TKVDB_OK;
}


/* compact node and put it to write buffer */
#ifndef TKVDB_PARAMS_NODBFILE
static TKVDB_RES
TKVDB_IMPL_NODE_TO_BUF(tkvdb *db, TKVDB_MEMNODE_TYPE *node,
	uint64_t transaction_off)
{
	struct tkvdb_disknode *disknode;
	uint8_t *ptr;
	uint64_t iobuf_off;

	iobuf_off = node->c.disk_off - transaction_off;

	TKVDB_EXEC(
		tkvdb_writebuf_realloc(db, iobuf_off + node->c.disk_size)
	);

	disknode = (struct tkvdb_disknode *)(db->write_buf + iobuf_off);

	disknode->size = node->c.disk_size;
	disknode->type = node->c.type;
	disknode->nsubnodes = node->c.nsubnodes;
	disknode->prefix_size = node->c.prefix_size;

	ptr = disknode->data;

	if (node->c.type & TKVDB_NODE_VAL) {
		*((uint32_t *)ptr) = node->c.val_size;
		ptr += sizeof(uint32_t);
	}
	if (node->c.type & TKVDB_NODE_META) {
		*((uint32_t *)ptr) = node->c.meta_size;
		ptr += sizeof(uint32_t);
	}

	if (node->c.type & TKVDB_NODE_LEAF) {
		TKVDB_MEMNODE_TYPE_LEAF *node_leaf;
		node_leaf = (TKVDB_MEMNODE_TYPE_LEAF *)node;
#ifdef TKVDB_PARAMS_ALIGN_VAL
		/* copy prefix */
		memcpy(ptr, node_leaf->prefix_val_meta, node->c.prefix_size);
		/* and value */
		memcpy(ptr + node_leaf->c.prefix_size,
			node_leaf->prefix_val_meta
				+ node_leaf->c.prefix_size
				+ node_leaf->c.val_pad,
			node_leaf->c.val_size);
#else
		memcpy(ptr, node_leaf->prefix_val_meta,
			node_leaf->c.prefix_size
			+ node_leaf->c.val_size
			+ node_leaf->c.meta_size);
#endif
	} else {
		if (node->c.nsubnodes > TKVDB_SUBNODES_THR) {
			memcpy(ptr, node->fnext, sizeof(uint64_t) * 256);
			ptr += sizeof(uint64_t) * 256;
		} else {
			int i;
			uint8_t *symbols;

			/* array of next symbols */
			symbols = ptr;
			ptr += node->c.nsubnodes * sizeof(uint8_t);
			for (i=0; i<256; i++) {
				if (node->fnext[i]) {
					*symbols = i;
					symbols++;

					*((uint64_t *)ptr) = node->fnext[i];
					ptr += sizeof(uint64_t);
				}
			}
		}
#ifdef TKVDB_PARAMS_ALIGN_VAL
		memcpy(ptr, node->prefix_val_meta, node->c.prefix_size);
		memcpy(ptr + node->c.prefix_size,
			node->prefix_val_meta
				+ node->c.prefix_size
				+ node->c.val_pad,
			node->c.val_size);
#else
		memcpy(ptr, node->prefix_val_meta,
			node->c.prefix_size
			+ node->c.val_size
			+ node->c.meta_size);
#endif
	}


	return TKVDB_OK;
}
#endif

/* calculate size of node on disk */
#ifndef TKVDB_PARAMS_NODBFILE
static void
TKVDB_IMPL_NODE_CALC_DISKSIZE(TKVDB_MEMNODE_TYPE *node)
{
	node->c.disk_size = sizeof(struct tkvdb_disknode) - 1;

	/* if node has value add 4 bytes for value size */
	if (node->c.type & TKVDB_NODE_VAL) {
		node->c.disk_size += sizeof(uint32_t);
	}
	/* 4 bytes for metadata size */
	if (node->c.type & TKVDB_NODE_META) {
		node->c.disk_size += sizeof(uint32_t);
	}

	/* subnodes */
	if (node->c.nsubnodes > TKVDB_SUBNODES_THR) {
		node->c.disk_size += 256 * sizeof(uint64_t);
	} else {
		node->c.disk_size += node->c.nsubnodes * sizeof(uint8_t)
			+ node->c.nsubnodes * sizeof(uint64_t);
	}

	/* prefix + value + metadata */
	node->c.disk_size += node->c.prefix_size + node->c.val_size
		+ node->c.meta_size;
}
#endif


/* commit */
#ifndef TKVDB_PARAMS_NODBFILE
static TKVDB_RES
TKVDB_IMPL_DO_COMMIT(tkvdb_tr *trns, struct tkvdb_db_info *vacdbinfo)
{
	size_t stack_size = 0;

	struct tkvdb_db_info info;

	/* offset of whole transaction in file */
	uint64_t transaction_off;
	/* offset of next node in file */
	uint64_t node_off;
	/* size of last accessed node, will be added to node_off */
	uint64_t last_node_size;
	struct tkvdb_tr_header *header_ptr;
	int append;
	tkvdb_tr_data *tr = trns->data;

	TKVDB_MEMNODE_TYPE *node;
	int off = 0;
	TKVDB_RES r = TKVDB_OK;

	if (!tr->started) {
		return TKVDB_NOT_STARTED;
	}

	if (!tr->db) {
		TKVDB_IMPL_TR_RESET(trns);
		return TKVDB_OK;
	}

	if (!tr->root) {
		/* empty transaction, rollback */
		TKVDB_IMPL_TR_RESET(trns);
		return TKVDB_OK;
	}

	/* read transaction footer before commit to make some checks */
	TKVDB_EXEC( tkvdb_info_read(tr->db->fd, &info) );

	if (info.filesize != tr->db->info.filesize) {
		/* file was modified during transaction */
		return TKVDB_MODIFIED;
	}

	if (info.filesize > 0) {
		if ((info.footer.transaction_id + 1)
			!= tr->db->info.footer.transaction_id) {

			return TKVDB_MODIFIED;
		}

		if ((info.footer.gap_end - info.footer.gap_begin)
			> tr->tr_buf_allocated) {

			/* we have enough space in vacuumed gap */
			transaction_off = info.footer.gap_begin;
			append = 0;
		} else {
			/* append transaction to the end of file */
			transaction_off = info.filesize;
			append = 1;
		}
	} else {
		/* empty data file */
		memcpy(tr->db->info.footer.signature,
			TKVDB_SIGNATURE,
			sizeof(TKVDB_SIGNATURE) - 1);

		transaction_off = 0;
		append = 1;
	}

	/* first node offset, skip transaction header */
	node_off = transaction_off + sizeof(struct tkvdb_tr_header);

	last_node_size = 0;

	/* now iterate through nodes in transaction */
	node = tr->root;

	for (;;) {
		TKVDB_MEMNODE_TYPE *next;

		TKVDB_SKIP_RNODES(node);

		if (node->c.disk_size == 0) {
			TKVDB_IMPL_NODE_CALC_DISKSIZE(node);

			node->c.disk_off = node_off;
			last_node_size = node->c.disk_size;
		}

		next = NULL;
		if (!(node->c.type & TKVDB_NODE_LEAF)) {
			/* non-leaf node */
			for (; off<256; off++) {
				if (node->next[off]) {
					/* found next subnode */
					next = node->next[off];
					break;
				}
			}
		}

		if (next) {
			TKVDB_SKIP_RNODES(next);

			node_off += last_node_size;
			node->fnext[off] = node_off;

			/* push node and position to stack */
			if ((stack_size + 1) > tr->stack_allocated) {
				struct tkvdb_visit_helper *tmpstack;

				if (!tr->params.stack_dynalloc) {
					return TKVDB_ENOMEM;
				}

				tmpstack = realloc(tr->stack, (stack_size + 1)
					* sizeof(struct tkvdb_visit_helper));
				if (!tmpstack) {
					return TKVDB_ENOMEM;
				}
				tr->stack = tmpstack;
				tr->stack_allocated = stack_size + 1;
			}
			tr->stack[stack_size].node = node;
			tr->stack[stack_size].off = off;
			stack_size++;

			node = next;
			off = 0;
		} else {
			/* no more subnodes, serialize node to memory buffer */
			r = TKVDB_IMPL_NODE_TO_BUF(tr->db, node,
				transaction_off);
			if (r != TKVDB_OK) {
				goto fail_node_to_buf;
			}

			/* pop */
			if (stack_size == 0) {
				break;
			}

			stack_size--;
			node = tr->stack[stack_size].node;
			off  = tr->stack[stack_size].off + 1;
		}
	}

	node_off += last_node_size;

	tr->db->info.footer.root_off = transaction_off
		+ sizeof(struct tkvdb_tr_header);
	tr->db->info.footer.transaction_size = node_off - transaction_off;

	/* seek */
	if (lseek(tr->db->fd, transaction_off, SEEK_SET)
		!= (off_t)transaction_off) {
		return TKVDB_IO_ERROR;
	}

	/* prepare header, footer and write */
	header_ptr = (struct tkvdb_tr_header *)tr->db->write_buf;
	header_ptr->type = TKVDB_BLOCKTYPE_TRANSACTION;
	tr->db->info.footer.type = TKVDB_BLOCKTYPE_FOOTER;
	if (vacdbinfo) {
		/* vacuum commit */
		/*tr->db->info.footer.gap_end = *gap_end_ptr;*/
	}
	if (append) {
		ssize_t wsize;
		struct tkvdb_tr_footer *footer_ptr;

		header_ptr->footer_off = node_off;

		wsize = tr->db->info.footer.transaction_size
			+ TKVDB_TR_FTRSIZE;

		/* try to append footer to buffer */
		TKVDB_EXEC( tkvdb_writebuf_realloc(tr->db, wsize) );

		footer_ptr = (struct tkvdb_tr_footer *)
			&(tr->db->write_buf[wsize - TKVDB_TR_FTRSIZE]);

		*footer_ptr = tr->db->info.footer;
		if (!tkvdb_try_write_file(tr->db->fd, tr->db->write_buf,
			wsize)) {

			return TKVDB_IO_ERROR;
		}
	} else {
		ssize_t wsize;

		wsize = tr->db->info.footer.transaction_size;
		tr->db->info.footer.gap_begin += wsize;

		header_ptr->footer_off = tr->db->info.filesize;
		if (!tkvdb_try_write_file(tr->db->fd, tr->db->write_buf,
			wsize)) {

			return TKVDB_IO_ERROR;
		}
		/* seek to end of file */
		if (lseek(tr->db->fd, tr->db->info.filesize, SEEK_SET)
			!= (off_t)tr->db->info.filesize) {

			return TKVDB_IO_ERROR;
		}
		/* write footer */
		wsize = sizeof(struct tkvdb_tr_footer);
		if (!tkvdb_try_write_file(tr->db->fd, &tr->db->info.footer,
			wsize)) {

			return TKVDB_IO_ERROR;
		}
	}

	r = TKVDB_OK;

	/* return root offset */
/*
	if (root_off) {
		*root_off = tr->db->info.footer.root_off;
	}
*/
/*
	if (sync && (fsync(tr->db->fd) < 0)) {
		return TKVDB_IO_ERROR;
	}
*/
fail_node_to_buf:
	TKVDB_IMPL_TR_RESET(trns);

	return r;
}

static TKVDB_RES
TKVDB_IMPL_COMMIT(tkvdb_tr *tr)
{
	return TKVDB_IMPL_DO_COMMIT(tr, NULL);
}
#endif


/* RAM-only */
#ifdef TKVDB_PARAMS_NODBFILE
static TKVDB_RES
TKVDB_IMPL_COMMIT(tkvdb_tr *tr)
{
	TKVDB_IMPL_TR_RESET(tr);

	return TKVDB_OK;
}
#endif
