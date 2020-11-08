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



/* get memory for node
 * memory block is taken from system using malloc()
 * when 'tr->tr_buf_dynalloc' is true
 * or from preallocated buffer
 * preallocation occurs in tkvdb_tr_create() */
void *
TKVDB_IMPL_NODE_ALLOC(tkvdb_tr *trns, size_t node_size)
{
	TKVDB_MEMNODE_TYPE *node;
	tkvdb_tr_data *tr = trns->data;

	if (tr->params.tr_buf_dynalloc) {
		/* allocate node using system malloc() */
		if ((tr->tr_buf_allocated + node_size)
			> tr->params.tr_buf_limit) {

			/* memory limit exceeded */
			return NULL;
		}

		node = malloc(node_size);
		if (!node) {
			return NULL;
		}
	} else {
		uint8_t *buf_ptr;
		/* FIXME: don't hardcode! */
		/* align to 16-byte boundary */
		buf_ptr = (uint8_t *)
			((uintptr_t)(tr->tr_buf_ptr + 16 - 1) & (-16));

		/* calculate node size with alignment */
		node_size += buf_ptr - tr->tr_buf_ptr;

		if ((tr->tr_buf_allocated + node_size)
			> tr->params.tr_buf_limit) {

			/* memory limit exceeded */
			return NULL;
		}
		node = (TKVDB_MEMNODE_TYPE *)buf_ptr;
		tr->tr_buf_ptr += node_size;
	}

	tr->tr_buf_allocated += node_size;

	return node;
}

/* create new node and append prefix and value */
void *
TKVDB_IMPL_NODE_NEW(tkvdb_tr *tr, int type, size_t prefix_size,
	const void *prefix, size_t val_size, const void *val,
	size_t meta_size, void *meta)
{
	TKVDB_MEMNODE_TYPE *node;
	TKVDB_MEMNODE_TYPE_LEAF *node_leaf;
	TKVDB_MEMNODE_TYPE_COMMON *node_common;
	size_t node_size;
	void *ret;

#ifdef TKVDB_PARAMS_ALIGN_VAL
/* aligned value */
#define NODE_ALIGN (((tkvdb_tr_data *)(tr->data))->params.alignval)

#define PTR_TO_VAL(NODE) ((uintptr_t)(NODE->prefix_val_meta + prefix_size))

#define VALPADDING(NODE)                                                   \
	(((PTR_TO_VAL(NODE) + NODE_ALIGN - 1) & -NODE_ALIGN)               \
	- PTR_TO_VAL(NODE))

#define COPY_VAL(NODE)                                                     \
do {                                                                       \
	memcpy(NODE->prefix_val_meta + prefix_size + NODE->c.val_pad,      \
		val, val_size);                                            \
} while(0)

#define COPY_META(NODE)                                                    \
do {                                                                       \
	memcpy(NODE->prefix_val_meta + prefix_size + NODE->c.val_pad       \
		+ val_size, meta, meta_size);                              \
} while(0)

#else
/* non-aligned value */
#define NODE_ALIGN 0

#define COPY_VAL(NODE)                                                     \
do {                                                                       \
	memcpy(NODE->prefix_val_meta + prefix_size, val, val_size);        \
} while(0)

#define COPY_META(NODE)                                                    \
do {                                                                       \
	memcpy(NODE->prefix_val_meta + prefix_size + val_size, meta,       \
		meta_size);                                                \
} while(0)

#endif

	if (type & TKVDB_NODE_LEAF) {
		node_size = sizeof(TKVDB_MEMNODE_TYPE_LEAF)
			+ prefix_size + val_size + NODE_ALIGN + meta_size;
		node_leaf = TKVDB_IMPL_NODE_ALLOC(tr, node_size);
		if (!node_leaf) {
			return NULL;
		}
		node_common = &node_leaf->c;
	} else {
		node_size = sizeof(TKVDB_MEMNODE_TYPE)
			+ prefix_size + val_size + NODE_ALIGN + meta_size;
		node = TKVDB_IMPL_NODE_ALLOC(tr, node_size);
		if (!node) {
			return NULL;
		}
		node_common = &node->c;
	}

	node_common->type = type;
	node_common->prefix_size = prefix_size;
	node_common->val_size = val_size;
	node_common->meta_size = meta_size;
	node_common->replaced_by = NULL;
	node_common->disk_size = 0;
	node_common->disk_off = 0;


	node_common->nsubnodes = 0;

	if (type & TKVDB_NODE_LEAF) {
		if (prefix_size > 0) {
			memcpy(node_leaf->prefix_val_meta,
				prefix, prefix_size);
		}

#ifdef TKVDB_PARAMS_ALIGN_VAL
		node_leaf->c.val_pad = VALPADDING(node_leaf);
#endif
		if (val_size > 0) {
			COPY_VAL(node_leaf);
		}
		if (meta && (meta_size > 0)) {
			COPY_META(node_leaf);
		}
		ret = node_leaf;
	} else {
		if (prefix_size > 0) {
			memcpy(node->prefix_val_meta,
				prefix, prefix_size);
		}

#ifdef TKVDB_PARAMS_ALIGN_VAL
		node->c.val_pad = VALPADDING(node);
#endif
		if (val_size > 0) {
			COPY_VAL(node);
		}

		memset(node->next, 0, sizeof(TKVDB_MEMNODE_TYPE *) * 256);
#ifndef TKVDB_PARAMS_NODBFILE
		memset(node->fnext, 0, sizeof(uint64_t) * 256);
#endif
		if (meta && (meta_size > 0)) {
			COPY_META(node);
		}
		ret = node;
	}

	return ret;
#undef NODE_ALIGN
#undef PTR_TO_VAL
#undef VALPADDING
#undef COPY_VAL
#undef COPY_META
}

static void
TKVDB_IMPL_CLONE_SUBNODES(TKVDB_MEMNODE_TYPE *dst, TKVDB_MEMNODE_TYPE *src)
{
	if (dst->c.type & TKVDB_NODE_LEAF) {
		/* dst has no subnodes, nothing to do */
		return;
	}
	if (src->c.type & TKVDB_NODE_LEAF) {
		memset(dst->next, 0, sizeof(void *) * 256);
#ifndef TKVDB_PARAMS_NODBFILE
		memset(dst->fnext, 0, sizeof(uint64_t) * 256);
#endif
	} else {
		memcpy(dst->next,  src->next,
			sizeof(TKVDB_MEMNODE_TYPE *) * 256);
#ifndef TKVDB_PARAMS_NODBFILE
		memcpy(dst->fnext, src->fnext, sizeof(uint64_t) * 256);
#endif
	}
	dst->c.nsubnodes = src->c.nsubnodes;
}

/* read node from disk */
#ifndef TKVDB_PARAMS_NODBFILE
static TKVDB_RES
TKVDB_IMPL_NODE_READ(tkvdb_tr *trns,
	uint64_t off, TKVDB_MEMNODE_TYPE **node_ptr)
{
	uint8_t buf[TKVDB_READ_SIZE];
	struct tkvdb_disknode *disknode;
	size_t prefix_val_meta_size;
	uint8_t *ptr;
	int fd;
	unsigned char *prefix_val_meta;
	tkvdb_tr_data *tr = trns->data;

#ifdef TKVDB_PARAMS_ALIGN_VAL
/* aligned value */
#define NODE_ALIGN (tr->params.alignval)

#define PTR_TO_VAL(NODE) ((uintptr_t)(prefix_val_meta + NODE->c.prefix_size))

#define VALPADDING(NODE)                                                   \
	(((PTR_TO_VAL(NODE) + NODE_ALIGN - 1) & -NODE_ALIGN)               \
	- PTR_TO_VAL(NODE))

#else
#define NODE_ALIGN 0
#endif

	fd = tr->db->fd;

	if (lseek(fd, off, SEEK_SET) != (off_t)off) {
		return TKVDB_IO_ERROR;
	}

	if (!tkvdb_try_read_file(fd, buf, TKVDB_READ_SIZE, 1)) {
		return TKVDB_IO_ERROR;
	}
	disknode = (struct tkvdb_disknode *)buf;

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
	if (disknode->type & TKVDB_NODE_LEAF) {
		*node_ptr = TKVDB_IMPL_NODE_ALLOC(trns,
			sizeof(TKVDB_MEMNODE_TYPE_LEAF)
			+ prefix_val_meta_size + NODE_ALIGN);
	} else {
		*node_ptr = TKVDB_IMPL_NODE_ALLOC(trns,
			sizeof(TKVDB_MEMNODE_TYPE)
			+ prefix_val_meta_size + NODE_ALIGN);
	}

	if (!(*node_ptr)) {
		return TKVDB_ENOMEM;
	}

	(*node_ptr)->c.replaced_by = NULL;

	/* now fill memnode with values from disk node */
	(*node_ptr)->c.type = disknode->type;
	(*node_ptr)->c.prefix_size = disknode->prefix_size;

	(*node_ptr)->c.disk_size = 0;
	(*node_ptr)->c.disk_off = 0;

	(*node_ptr)->c.nsubnodes = disknode->nsubnodes;

	ptr = disknode->data;

	(*node_ptr)->c.val_size = (*node_ptr)->c.meta_size = 0;
	if (disknode->type & TKVDB_NODE_VAL) {
		(*node_ptr)->c.val_size = *((uint32_t *)ptr);
		ptr += sizeof(uint32_t);
	}
	if (disknode->type & TKVDB_NODE_META) {
		(*node_ptr)->c.meta_size = *((uint32_t *)ptr);
		ptr += sizeof(uint32_t);
	}

	if (disknode->type & TKVDB_NODE_LEAF) {
		TKVDB_MEMNODE_TYPE_LEAF *node_leaf;
		node_leaf = (TKVDB_MEMNODE_TYPE_LEAF *)(*node_ptr);
		prefix_val_meta = node_leaf->prefix_val_meta;
	} else {
		/* non-leaf node */
		memset((*node_ptr)->next, 0,
			sizeof(TKVDB_MEMNODE_TYPE *) * 256);

		if (disknode->nsubnodes > TKVDB_SUBNODES_THR) {
			memcpy((*node_ptr)->fnext, ptr,
				256 * sizeof(uint64_t));
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
		prefix_val_meta = (*node_ptr)->prefix_val_meta;
	}

	if (disknode->size > TKVDB_READ_SIZE) {
		/* prefix + value + metadata bigger than read block */
		size_t blk_tail = TKVDB_READ_SIZE
			- (disknode->size - prefix_val_meta_size);

#ifdef TKVDB_PARAMS_ALIGN_VAL
		size_t pfx_size = (*node_ptr)->c.prefix_size;
		uint8_t *val_meta_ptr;

		if (blk_tail >= pfx_size) {
			/* prefix is in buffer, copy it to node */
			memcpy(prefix_val_meta, ptr, pfx_size);

			/* copy start of value + metadata */
			(*node_ptr)->c.val_pad = VALPADDING((*node_ptr));
			val_meta_ptr = prefix_val_meta + pfx_size
				+ (*node_ptr)->c.val_pad;
			memcpy(val_meta_ptr, ptr + pfx_size,
				blk_tail - pfx_size);
			/* read rest of value + metadata */
			val_meta_ptr += blk_tail - pfx_size;

			if (!tkvdb_try_read_file(fd, val_meta_ptr,
				disknode->size - TKVDB_READ_SIZE, 0)) {

				return TKVDB_IO_ERROR;
			}
		} else {
			/* copy start of prefix */
			memcpy(prefix_val_meta, ptr, blk_tail);

			/* read rest of prefix */
			if (!tkvdb_try_read_file(fd,
				prefix_val_meta + blk_tail,
				pfx_size - blk_tail, 0)) {

				return TKVDB_IO_ERROR;
			}

			/* read value + metadata */
			(*node_ptr)->c.val_pad = VALPADDING((*node_ptr));
			val_meta_ptr = prefix_val_meta + pfx_size
				+ (*node_ptr)->c.val_pad;

			if (!tkvdb_try_read_file(fd, val_meta_ptr,
				prefix_val_meta_size - pfx_size, 0)) {

				return TKVDB_IO_ERROR;
			}
		}
#else
		memcpy(prefix_val_meta, ptr, blk_tail);
		if (!tkvdb_try_read_file(fd, prefix_val_meta + blk_tail,
			disknode->size - TKVDB_READ_SIZE, 0)) {

			return TKVDB_IO_ERROR;
		}
#endif
	} else {
#ifdef TKVDB_PARAMS_ALIGN_VAL
		/* copy prefix */
		memcpy(prefix_val_meta, ptr, (*node_ptr)->c.prefix_size);
		/* and value */
		(*node_ptr)->c.val_pad = VALPADDING((*node_ptr));
		memcpy(prefix_val_meta
			+ (*node_ptr)->c.prefix_size
			+ (*node_ptr)->c.val_pad,
			ptr + (*node_ptr)->c.prefix_size,
			prefix_val_meta_size - (*node_ptr)->c.prefix_size);
#else
		memcpy(prefix_val_meta, ptr, prefix_val_meta_size);
#endif
	}

	return TKVDB_OK;
#undef NODE_ALIGN
#undef PTR_TO_VAL
#undef VALPADDING
}

#endif
/* no NODE_READ function in RAM-only mode */

/* free node and subnodes */
static void
TKVDB_IMPL_NODE_FREE(tkvdb_tr_data *tr, TKVDB_MEMNODE_TYPE *node)
{
	size_t stack_size = 0;

	TKVDB_MEMNODE_TYPE *next;
	int off = 0;

	for (;;) {
		if (node->c.replaced_by) {
			next = node->c.replaced_by;
			free(node);
			node = next;
			continue;
		}

		if (!(node->c.type & TKVDB_NODE_LEAF)) {
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
				if ((stack_size + 1) > tr->stack_allocated) {
					tr->stack = realloc(tr->stack,
						(stack_size + 1)
						* sizeof(struct tkvdb_visit_helper));
					if (!tr->stack) {
						return;
					}
					tr->stack_allocated = stack_size + 1;
				}
				tr->stack[stack_size].node = node;
				tr->stack[stack_size].off = off;
				stack_size++;

				node = next;
				off = 0;
				continue;
			}
		}

		/* no more subnodes */
		if (stack_size < 1) {
			break;
		}

		free(node);
		/* get node from stack's top */
		stack_size--;
		node = tr->stack[stack_size].node;
		off = tr->stack[stack_size].off;
		off++;
	}
	free(node);
}

