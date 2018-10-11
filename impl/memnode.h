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


/* node in memory */
typedef struct TKVDB_MEMNODE_TYPE_COMMON
{
	int type;

	struct TKVDB_MEMNODE_TYPE *replaced_by;

	size_t prefix_size;
	size_t val_size;
	size_t meta_size;

#ifdef TKVDB_PARAMS_ALIGN_VAL
	size_t val_pad;                   /* padding for aligned value */
	size_t meta_pad;                  /* and metadata */
#endif

	uint64_t disk_size;               /* size of node on disk */
	uint64_t disk_off;                /* offset of node on disk */
	unsigned int nsubnodes;           /* number of subnodes */
} TKVDB_MEMNODE_TYPE_COMMON;

typedef struct TKVDB_MEMNODE_TYPE
{
	TKVDB_MEMNODE_TYPE_COMMON c;

	/* subnodes in memory */
	void *next[256];
#ifndef TKVDB_PARAMS_NODBFILE
	/* positions of subnodes in file */
	uint64_t fnext[256];
#endif

	unsigned char prefix_val_meta[1]; /* prefix, value and metadata */
} TKVDB_MEMNODE_TYPE;

/* no subnodes in leaf */
typedef struct TKVDB_MEMNODE_TYPE_LEAF
{
	TKVDB_MEMNODE_TYPE_COMMON c;

	unsigned char prefix_val_meta[1]; /* prefix, value and metadata */
} TKVDB_MEMNODE_TYPE_LEAF;

/* get next subnode (or load from disk) */
#ifndef TKVDB_PARAMS_NODBFILE

#define TKVDB_SUBNODE_NEXT(TR, NODE, NEXT, OFF)                           \
do {                                                                      \
	tkvdb_tr_data *trd = TR->data;                                    \
	if (NODE->c.type & TKVDB_NODE_LEAF) {                             \
		break;                                                    \
	}                                                                 \
	if (NODE->next[OFF]) {                                            \
		NEXT = node->next[OFF];                                   \
	} else if (trd->db && NODE->fnext[OFF]) {                         \
		TKVDB_MEMNODE_TYPE *tmp;                                  \
		TKVDB_EXEC( TKVDB_IMPL_NODE_READ(TR, NODE->fnext[OFF],    \
			&tmp) );                                          \
		NODE->next[OFF] = tmp;                                    \
		NEXT = tmp;                                               \
	}                                                                 \
} while (0)

#else

/* RAM-only */
#define TKVDB_SUBNODE_NEXT(TR, NODE, NEXT, OFF)                           \
do {                                                                      \
	if (NODE->c.type & TKVDB_NODE_LEAF) {                             \
		break;                                                    \
	}                                                                 \
	if (NODE->next[OFF]) {                                            \
		NEXT = node->next[OFF];                                   \
	}                                                                 \
} while (0)

#endif

#define TKVDB_SUBNODE_SEARCH(TR, NODE, NEXT, OFF, INCR)                   \
do {                                                                      \
	int lim, step;                                                    \
	NEXT = NULL;                                                      \
	if (NODE->c.type & TKVDB_NODE_LEAF) {                             \
		break;                                                    \
	}                                                                 \
	if (INCR) {                                                       \
		lim = 256;                                                \
		step = 1;                                                 \
	} else {                                                          \
		lim = -1;                                                 \
		step = -1;                                                \
	}                                                                 \
	for (; OFF!=lim; OFF+=step) {                                     \
		TKVDB_SUBNODE_NEXT(TR, NODE, NEXT, OFF);                  \
		if (next) {                                               \
			break;                                            \
		}                                                         \
	}                                                                 \
} while (0)

