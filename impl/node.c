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



/* get memory for node
 * memory block is taken from system using malloc()
 * when 'tr->tr_buf_dynalloc' is true
 * or from preallocated buffer
 * preallocation occurs in tkvdb_tr_create_m() */
static TKVDB_MEMNODE_TYPE *
TKVDB_IMPL_NODE_ALLOC(tkvdb_tr *trns, size_t node_size)
{
	TKVDB_MEMNODE_TYPE *node;
	tkvdb_tr_data *tr = trns->data;

	if ((tr->tr_buf_allocated + node_size) > tr->params.tr_buf_limit) {
		/* memory limit exceeded */
		return NULL;
	}

	if (tr->params.tr_buf_dynalloc) {
		node = malloc(node_size);
		if (!node) {
			return NULL;
		}
	} else {
		/* FIXME: don't hardcode! check space! */
		/* align to 16-byte boundary */
		tr->tr_buf_ptr = (uint8_t *)
			((uintptr_t)(tr->tr_buf_ptr + 16 - 1) & (-16));
		node = (TKVDB_MEMNODE_TYPE *)tr->tr_buf_ptr;
		tr->tr_buf_ptr += node_size;
	}

	tr->tr_buf_allocated += node_size;
	return node;
}

/* create new node and append prefix and value */
static TKVDB_MEMNODE_TYPE *
TKVDB_IMPL_NODE_NEW(tkvdb_tr *tr, int type, size_t prefix_size,
	const void *prefix, size_t vlen, const void *val)
{
	TKVDB_MEMNODE_TYPE *node;
	size_t node_size;

	node_size = sizeof(TKVDB_MEMNODE_TYPE) + prefix_size + vlen;
	node = TKVDB_IMPL_NODE_ALLOC(tr, node_size);
	if (!node) {
		return NULL;
	}

	node->type = type;
	node->prefix_size = prefix_size;
	node->val_size = vlen;
	node->meta_size = 0;
	node->replaced_by = NULL;
	if (node->prefix_size > 0) {
		memcpy(node->prefix_val_meta, prefix, node->prefix_size);
	}
	if (node->val_size > 0) {
		memcpy(node->prefix_val_meta + node->prefix_size,
			val, node->val_size);
	}

	memset(node->next, 0, sizeof(TKVDB_MEMNODE_TYPE *) * 256);
	memset(node->fnext, 0, sizeof(uint64_t) * 256);

	node->disk_size = 0;
	node->disk_off = 0;

	return node;
}

static void
TKVDB_IMPL_CLONE_SUBNODES(TKVDB_MEMNODE_TYPE *dst, TKVDB_MEMNODE_TYPE *src)
{
	memcpy(dst->next,  src->next, sizeof(TKVDB_MEMNODE_TYPE *) * 256);
	memcpy(dst->fnext, src->fnext, sizeof(uint64_t) * 256);
}

/* read node from disk */
static TKVDB_RES
TKVDB_IMPL_NODE_READ(tkvdb_tr *trns,
	uint64_t off, TKVDB_MEMNODE_TYPE **node_ptr)
{
	uint8_t buf[TKVDB_READ_SIZE];
	ssize_t read_res;
	struct tkvdb_disknode *disknode;
	size_t prefix_val_meta_size;
	uint8_t *ptr;
	int fd;
	tkvdb_tr_data *tr = trns->data;

	fd = tr->db->fd;

	if (lseek(fd, off, SEEK_SET) != (off_t)off) {
		return TKVDB_IO_ERROR;
	}

	read_res = read(fd, buf, TKVDB_READ_SIZE);
	if (read_res < 0) {
		return TKVDB_IO_ERROR;
	}

	disknode = (struct tkvdb_disknode *)buf;

	if (((uint32_t)read_res < disknode->size)
		&& (disknode->size < TKVDB_READ_SIZE)) {

		return TKVDB_IO_ERROR;
	}

	/* calculate size of prefix + value + metadata */
	prefix_val_meta_size = disknode->size - sizeof(struct tkvdb_disknode)
		+ sizeof(uint8_t) * 1;

	if (disknode->type & TKVDB_NODE_VAL) {
		prefix_val_meta_size -= sizeof(uint32_t);
	}
	if (disknode->type & TKVDB_NODE_META) {
		prefix_val_meta_size -= sizeof(uint32_t);
	}

	if (disknode->nsubnodes > TKVDB_SUBNODES_THR) {
		prefix_val_meta_size -= 256 * sizeof(uint64_t);
	} else {
		prefix_val_meta_size -= disknode->nsubnodes * sizeof(uint8_t);
		prefix_val_meta_size -= disknode->nsubnodes * sizeof(uint64_t);
	}

	/* allocate memnode */
	*node_ptr = TKVDB_IMPL_NODE_ALLOC(trns, sizeof(TKVDB_MEMNODE_TYPE)
		+ prefix_val_meta_size);

	if (!(*node_ptr)) {
		return TKVDB_ENOMEM;
	}

	(*node_ptr)->replaced_by = NULL;
	/* now fill memnode with values from disk node */
	(*node_ptr)->type = disknode->type;
	(*node_ptr)->prefix_size = disknode->prefix_size;

	(*node_ptr)->disk_size = 0;
	(*node_ptr)->disk_off = 0;

	ptr = disknode->data;

	(*node_ptr)->val_size = (*node_ptr)->meta_size = 0;
	if (disknode->type & TKVDB_NODE_VAL) {
		(*node_ptr)->val_size = *((uint32_t *)ptr);
		ptr += sizeof(uint32_t);
	}
	if (disknode->type & TKVDB_NODE_META) {
		(*node_ptr)->meta_size = *((uint32_t *)ptr);
		ptr += sizeof(uint32_t);
	}

	memset((*node_ptr)->next, 0, sizeof(TKVDB_MEMNODE_TYPE *) * 256);

	if (disknode->nsubnodes > TKVDB_SUBNODES_THR) {
		memcpy((*node_ptr)->fnext, ptr, 256 * sizeof(uint64_t));
		ptr += 256 * sizeof(uint64_t);
	} else {
		int i;
		uint64_t *offptr;

		offptr = (uint64_t *)(ptr
			+ disknode->nsubnodes * sizeof(uint8_t));

		memset((*node_ptr)->fnext, 0, sizeof(uint64_t) * 256);

		for (i=0; i<disknode->nsubnodes; i++) {
			(*node_ptr)->fnext[*ptr] = *offptr;
			ptr++;
			offptr++;
		}
		ptr += disknode->nsubnodes * sizeof(uint64_t);
	}

	if (disknode->size > TKVDB_READ_SIZE) {
		/* prefix + value + metadata bigger than read block */
		size_t blk_tail = disknode->size - prefix_val_meta_size;

		memcpy((*node_ptr)->prefix_val_meta, ptr,
			TKVDB_READ_SIZE - blk_tail);
		ptr += TKVDB_READ_SIZE - blk_tail;
		read(fd, ptr, disknode->size - (TKVDB_READ_SIZE - blk_tail));
	} else {
		memcpy((*node_ptr)->prefix_val_meta, ptr,
			prefix_val_meta_size);
	}

	return TKVDB_OK;
}

/* free node and subnodes */
static void
TKVDB_IMPL_NODE_FREE(TKVDB_MEMNODE_TYPE *node)
{
	size_t stack_size = 0;
	struct tkvdb_visit_helper stack[TKVDB_STACK_MAX_DEPTH];

	TKVDB_MEMNODE_TYPE *next;
	int off = 0;

	for (;;) {
		if (node->replaced_by) {
			next = node->replaced_by;
			free(node);
			node = next;
			continue;
		}

		/* search in subnodes */
		next = NULL;
		for (; off<256; off++) {
			if (node->next[off]) {
				next = node->next[off];
				break;
			}
		}

		if (next) {
			/* push */
			stack[stack_size].node = node;
			stack[stack_size].off = off;
			stack_size++;

			node = next;
			off = 0;
		} else {
			/* no more subnodes */
			if (stack_size < 1) {
				break;
			}

			free(node);
			/* get node from stack's top */
			stack_size--;
			node = stack[stack_size].node;
			off = stack[stack_size].off;
			off++;
		}
	}
	free(node);
}

