#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include "tkvdb.h"

void
tkvdb_dump(tkvdb_tr *tr)
{
	tkvdb_cursor *c;
	int r;

	c = tkvdb_cursor_create();

	r = tkvdb_first(c, tr);
	if (r != TKVDB_OK) {
		return;
	}

	{
		char buf[100];
		memcpy(buf, tkvdb_cursor_key(c), tkvdb_cursor_keysize(c));
		buf[tkvdb_cursor_keysize(c)] = '\0';
		printf("First prefix: (len %lu) '%s'\n",
			tkvdb_cursor_keysize(c), buf);
	}

	for (;;) {
		r = tkvdb_next(c);
		if (r == TKVDB_OK) {
			char buf[100];
			if (tkvdb_cursor_keysize(c)) {
				memcpy(buf, tkvdb_cursor_key(c),
					tkvdb_cursor_keysize(c));
				buf[tkvdb_cursor_keysize(c)] = '\0';
			} else {
				strcpy(buf, "(null)");
			}
			printf("Next prefix: (len %lu) '%s'\n",
				tkvdb_cursor_keysize(c), buf);
		} else {
			printf("eodb\n");
			break;
		}
	}

}

void
print_indent(int indent)
{
	int i;

	for (i=0; i<indent; i++) {
		fputc(' ', stdout);
	}
}

/*
void
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
		printf("Type: '%s'\n", curr->type == TKVDB_KEY ? "key" : "kv");
		print_indent(indent);
		printf("Prefix: (len %lu) '%s'\n", curr->prefix_size, buf);
		if (curr->val_size > 0) {
			memcpy(buf, curr->prefix_and_val + curr->prefix_size,
				curr->val_size);
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
*/
#define N 256

static int
cmp(const void *a, const void *b)
{
	const char *p1 = a;
	const char *p2 = b;

	return strcmp(p1, p2);
}

int
main()
{
	tkvdb_tr *tr = NULL;
	int i;
	char keys[N * 6];

	tr = tkvdb_tr_create(NULL);
	tkvdb_begin(tr);

	for (i=0; i<N; i++) {
		char buf[100];
		int j, size;

		size = rand() % 4 + 1;
		for (j=0; j<size; j++) {
			buf[j] = '0' + rand() % 10;
		}
		buf[size] = '\0';
		memcpy(keys + i*6, buf, size + 1);
		tkvdb_put(tr, buf, size, buf, 2);
	}

	/*tkvdb_dump_recursive(tr.root, 0);*/
	tkvdb_dump(tr);

	qsort(keys, N, 6, &cmp);
	for (i=0; i<N; i++) {
		printf("key: %s\n", keys + i*6);
	}

	return EXIT_SUCCESS;
}

