#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

enum TKVDB_RES
{
	TKVDB_OK = 0,
	TKVDB_ENOMEM
};

enum TKVDB_MNTYPE
{
	TKVDB_KEY,
	TKVDB_KEYVAL,
	TKVDB_REPLACED
};

typedef struct tkvdb_memnode
{
	enum TKVDB_MNTYPE type;
	size_t prefix_size;
	size_t val_size;

	struct tkvdb_memnode *next[256];
	off_t fnext[256];

	unsigned char prefix_and_val[1];
} tkvdb_memnode;

typedef struct tkvdb_tr
{
	tkvdb_memnode *root;
} tkvdb_tr;

typedef struct tkvdb_nodepos
{
	tkvdb_memnode *n;
	int off;
} tkvdb_nodepos;

typedef struct tkvdb_cursor
{
	size_t stacksize;
	tkvdb_nodepos *stack;

	size_t prefix_size;
	unsigned char *prefix;
} tkvdb_cursor;

tkvdb_memnode *
tkvdb_node_alloc(enum TKVDB_MNTYPE type, size_t prefix_size, const void *prefix, size_t vlen, const void *val)
{
	tkvdb_memnode *r;

	r = malloc(sizeof(tkvdb_memnode) + prefix_size + vlen);
	if (!r) {
		return NULL;
	}

	r->type = type;
	r->prefix_size = prefix_size;
	r->val_size = vlen;
	if (r->prefix_size > 0) {
		memcpy(r->prefix_and_val, prefix, r->prefix_size);
	}
	if (r->val_size > 0) {
		memcpy(r->prefix_and_val + r->prefix_size, val, r->val_size);
	}

	memset(r->next, 0, sizeof(tkvdb_memnode *) * 256);
	memset(r->fnext, 0, sizeof(off_t) * 256);

	return r;
}

int
tkvdb_put(tkvdb_tr *tr, const void *key, size_t klen, const void *val, size_t vlen)
{
	const unsigned char *sym;  /* pointer to current symbol in key */
	tkvdb_memnode *curr;       /* current node */
	size_t pi;                 /* prefix index */

	/* new root */
	if (tr->root == NULL) {
		tr->root = tkvdb_node_alloc(TKVDB_KEYVAL, klen, key, vlen, val);
		if (!tr->root) {
			return TKVDB_ENOMEM;
		}

		return TKVDB_OK;
	}

	sym = key;
	curr = tr->root;

next_node:
	if (curr->type == TKVDB_REPLACED) {
		curr = curr->next[0];
		goto next_node;
	}
	pi = 0;

next_byte:

	/* end of key */
	if (sym >= ((unsigned char *)key + klen)) {
		/* split node */
		return TKVDB_OK;
	}

	/* end of prefix */
	if (pi >= curr->prefix_size) {
		if (curr->next[*sym] != NULL) {
			/* continue with next node */
			curr = curr->next[*sym];
			sym++;
			goto next_node;
		} else {
			tkvdb_memnode *tmp;

			printf("tail\n");
			/* allocate tail */
			tmp = tkvdb_node_alloc(TKVDB_KEYVAL, klen - (sym - (unsigned char *)key) - 1, sym + 1,
				vlen, val);
			if (!tmp) return TKVDB_ENOMEM;

			curr->next[*sym] = tmp;
			return TKVDB_OK;
		}
	}

	/* node prefix don't match with corresponding part of key */
	if (curr->prefix_and_val[pi] != *sym) {
		tkvdb_memnode *newroot, *subnode_rest, *subnode_key;

		printf("splitting key at %lu, '%c'(%d) != '%c'(%d)\n",
			pi, curr->prefix_and_val[pi], curr->prefix_and_val[pi], *sym, *sym);

		/* split current node into 3 subnodes */
		newroot = tkvdb_node_alloc(TKVDB_KEYVAL, pi, curr->prefix_and_val, 0, NULL);
		if (!newroot) return TKVDB_ENOMEM;

		/* rest of prefix (skip current symbol) */
		subnode_rest = tkvdb_node_alloc(TKVDB_KEYVAL, curr->prefix_size - pi - 1, curr->prefix_and_val + pi + 1,
			curr->val_size, curr->prefix_and_val + curr->prefix_size);
		if (!subnode_rest) {
			free(newroot);
			return TKVDB_ENOMEM;
		}
		memcpy(subnode_rest->next, curr->next, sizeof(tkvdb_memnode *) * 256);
		memcpy(subnode_rest->fnext, curr->fnext, sizeof(off_t) * 256);

		/* rest of key */
		subnode_key = tkvdb_node_alloc(TKVDB_KEYVAL, klen - (sym - (unsigned char *)key) - 1, sym + 1,
			vlen, val);
		if (!subnode_key) {
			free(subnode_rest);
			free(newroot);
			return TKVDB_ENOMEM;
		}

		newroot->next[curr->prefix_and_val[pi]] = subnode_rest;
		newroot->next[*sym] = subnode_key;

		curr->type = TKVDB_REPLACED;
		curr->next[0] = newroot;
		return TKVDB_OK;
	}

	sym++;
	pi++;
	goto next_byte;

	return TKVDB_OK;
}

/* cursors */

int
tkvdb_cursor_init(tkvdb_cursor *c)
{
	c->stacksize = 0;
	c->stack = NULL;

	c->prefix_size = 0;
	c->prefix = NULL;

	return TKVDB_OK;
}

int
tkvdb_cursor_close(tkvdb_cursor *c)
{
	if (c->prefix) {
		free(c->prefix);
		c->prefix = NULL;
	}
	c->prefix_size = 0;
	return TKVDB_OK;
}

static int
tkvdb_cursor_expand_prefix(tkvdb_cursor *c, size_t n)
{
	unsigned char *tmp_pfx;

	tmp_pfx = realloc(c->prefix, c->prefix_size + n);
	if (!tmp_pfx) {
		free(c->prefix);
		c->prefix = NULL;
		return TKVDB_ENOMEM;
	}
	c->prefix = tmp_pfx;

	return TKVDB_OK;
}

int
tkvdb_first(tkvdb_cursor *c, tkvdb_tr *tr)
{
	tkvdb_memnode *curr;
	int r;

	tkvdb_cursor_init(c);

	curr = tr->root;

	for (;;) {
		unsigned int i;
		tkvdb_memnode *next;

		if (curr->type == TKVDB_REPLACED) {
			curr = curr->next[0];
			continue;
		}

		if (curr->prefix_size) {
			if ((r = tkvdb_cursor_expand_prefix(c, curr->prefix_size)) != TKVDB_OK) {
				/* error */
				return r;
			}
			/* append prefix */
			memcpy(c->prefix + c->prefix_size, curr->prefix_and_val, curr->prefix_size);
			c->prefix_size += curr->prefix_size;
			{
				char buf[100];
				memcpy(buf, c->prefix, curr->prefix_size);
				buf[curr->prefix_size] = '\0';
				printf("Prefix: (%lu) %s[%s]\n", curr->prefix_size, c->prefix, buf);
			}
		}

		next = NULL;
		for (i=0; i<256; i++) {
			if (curr->next[i]) {
				next = curr->next[i];
				break;
			}
		}

		if (next) {
			if ((r = tkvdb_cursor_expand_prefix(c, 1)) != TKVDB_OK) {
				return r;
			}
			c->prefix[c->prefix_size] = i;
			c->prefix_size++;
			curr = next;
		} else {
			/* no subnodes */
			break;
		}
	}

	return TKVDB_OK;
}

void
tkvdb_dump(tkvdb_tr *tr)
{
	tkvdb_cursor c;
	int r;

	r = tkvdb_first(&c, tr);
	if (r != TKVDB_OK) {
		return;
	}
	printf("First prefix: (%lu) %s\n", c.prefix_size, c.prefix);
}

static void
print_indent(int indent)
{
	int i;

	for (i=0; i<indent; i++) {
		fputc(' ', stdout);
	}
}

static void
tkvdb_dump_recursive(tkvdb_memnode *curr, int indent)
{
	int i;

	while (curr->type == TKVDB_REPLACED) {
		curr = curr->next[0];
	}

	{
		char buf[100];
		memcpy(buf, curr->prefix_and_val, curr->prefix_size);
		buf[curr->prefix_size] = '\0';
		print_indent(indent);
		printf("Prefix: (len %lu) '%s'\n", curr->prefix_size, buf);
		if (curr->val_size > 0) {
			memcpy(buf, curr->prefix_and_val + curr->prefix_size, curr->val_size);
			buf[curr->val_size] = '\0';
			print_indent(indent);
			printf("Val: (len %lu) '%s'\n", curr->val_size, buf);
		}
	}

	print_indent(indent);
	printf("Subnodes:\n\n");
	for (i=0; i<256; i++) {
		if (curr->next[i]) {
			print_indent(indent + 1);
			printf("Symbol '%c'[%d]\n", i, i);
			tkvdb_dump_recursive(curr->next[i], indent + 1);
		}
	}
}


int
main()
{
	tkvdb_tr tr;
	tr.root = NULL;

	tkvdb_put(&tr, "06543210", 8, "hello", 5);
	tkvdb_put(&tr, "0a1aaaaa", 8, NULL, 0);
	tkvdb_put(&tr, "01234567", 8, NULL, 0);
	tkvdb_put(&tr, "0z2zzzzz", 8, NULL, 0);

	/*tkvdb_dump(&tr);*/
	tkvdb_dump_recursive(tr.root, 0);

	printf("\n\n\n");
	tkvdb_put(&tr, "012zzzzz", 8, "!!!", 3);
	tkvdb_dump_recursive(tr.root, 0);

	return EXIT_SUCCESS;
}

