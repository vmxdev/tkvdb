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
	void *val_ptr;                    /* pointer to aligned val */
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
	/* positions of subnodes in file */
	uint64_t fnext[256];

	unsigned char prefix_val_meta[1]; /* prefix, value and metadata */
} TKVDB_MEMNODE_TYPE;

/* no subnodes in leaf */
typedef struct TKVDB_MEMNODE_TYPE_LEAF
{
	TKVDB_MEMNODE_TYPE_COMMON c;

	unsigned char prefix_val_meta[1]; /* prefix, value and metadata */
} TKVDB_MEMNODE_TYPE_LEAF;

