/*
 * tkvdb
 *
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

/*
 * triggers common macros
 */

#ifdef TKVDB_PARAMS_ALIGN_VAL
#define TKVDB_VAL_ALIGN_PAD(NODE) (NODE->c.val_pad)
#else
#define TKVDB_VAL_ALIGN_PAD(NODE) 0
#endif


#ifdef TKVDB_TRIGGER

#define TKVDB_META_ADDR_LEAF(NODE)                                          \
	((TKVDB_MEMNODE_TYPE_LEAF *)NODE)->prefix_val_meta                  \
	+ NODE->c.prefix_size                                               \
	+ TKVDB_VAL_ALIGN_PAD(NODE)                                         \
	+ NODE->c.val_size

#define TKVDB_META_ADDR_NONLEAF(NODE)                                       \
	NODE->prefix_val_meta                                               \
	+ NODE->c.prefix_size                                               \
	+ TKVDB_VAL_ALIGN_PAD(NODE)                                         \
	+ NODE->c.val_size

#define TKVDB_META_ADDR(NODE)                                               \
	(NODE->c.type & TKVDB_NODE_LEAF)                                    \
	? (TKVDB_META_ADDR_LEAF(NODE)) : (TKVDB_META_ADDR_NONLEAF(NODE))

#define TKVDB_INC_VOID_PTR(P, I)                                            \
do {                                                                        \
	char *tmp = P;                                                      \
	tmp += I;                                                           \
	P = tmp;                                                            \
} while (0)

#define TKVDB_CALL_ALL_TRIGGER_FUNCTIONS(T)                                 \
do {                                                                        \
	size_t trg_idx;                                                     \
	for (trg_idx=0; trg_idx<T->n_funcs; trg_idx++) {                    \
		size_t stack_idx;                                           \
		T->info.userdata = T->funcs[trg_idx].userdata;              \
		(*T->funcs[trg_idx].func)(&(T->info));                      \
		for (stack_idx=0; stack_idx<T->stack.size; stack_idx++) {   \
			TKVDB_INC_VOID_PTR(T->stack.meta[stack_idx],        \
				T->funcs[trg_idx].meta_size);               \
		}                                                           \
		TKVDB_INC_VOID_PTR(T->info.newroot,                         \
			T->funcs[trg_idx].meta_size);                       \
		TKVDB_INC_VOID_PTR(T->info.subnode1,                        \
			T->funcs[trg_idx].meta_size);                       \
		TKVDB_INC_VOID_PTR(T->info.subnode2,                        \
			T->funcs[trg_idx].meta_size);                       \
	}                                                                   \
} while (0)


#define TKVDB_TRIGGER_NODE_PUSH(T, NODE, PVM)                               \
do {                                                                        \
	T->stack.meta[T->stack.size] = PVM                                  \
		+ NODE->c.prefix_size + TKVDB_VAL_ALIGN_PAD(NODE)           \
		+ NODE->c.val_size;                                         \
	T->stack.size++;                                                    \
} while (0)

#else

#define TKVDB_TRIGGER_NODE_PUSH(T, NODE, PVM)

#endif

