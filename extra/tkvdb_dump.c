#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <ctype.h>

#include "tkvdb.h"

static void
usage(const char *prog_name)
{
	fprintf(stderr, "Usage: %s [-r] FILE.DB\n", prog_name);
	fprintf(stderr, "  -r print in reverse order\n");
}

static void
print_kv(tkvdb_cursor *c)
{
	size_t i;
	uint8_t *buf;

	putchar('"');
	buf = tkvdb_cursor_key(c);
	for (i=0; i<tkvdb_cursor_keysize(c); i++) {
		int c = buf[i];
		if (isprint(c) && (c != '"')) {
			putchar(c);
		} else {
			printf("\\%02x", c);
		}
	}
	putchar('"');
	putchar('\n');
}

int
main(int argc, char *argv[])
{
	tkvdb *db;
	tkvdb_tr *tr;
	tkvdb_cursor *c;
	TKVDB_RES r;
	int ret = EXIT_FAILURE;

	int opt;
	int reverse = 0;

	while ((opt = getopt(argc, argv, ":r")) != -1) {
		switch (opt) {
			case 'r':
				reverse = 1;
				break;
			default:
				usage(argv[0]);
				goto fail;
		}
	}

	if (optind >= argc) {
		usage(argv[0]);
		goto fail;
	}

	db = tkvdb_open(argv[optind], NULL);
	if (!db) {
		fprintf(stderr, "Can't open %s\n", argv[optind]);
		goto fail_db;
	}
	tr = tkvdb_tr_create_m(db, 1024*1024*10, 0);

	r = tkvdb_begin(tr);
	if (r != TKVDB_OK) {
		printf("Can't start transaction, error code %d\n", r);
		goto fail_tr;
	}

	c = tkvdb_cursor_create(tr);

	for (r=reverse ? tkvdb_last(c) : tkvdb_first(c);
		r==TKVDB_OK;
		r = reverse ? tkvdb_prev(c) : tkvdb_next(c)) {

		print_kv(c);
	}

	tkvdb_cursor_free(c);

	if ((r = tkvdb_rollback(tr))!= TKVDB_OK) {
		printf("Can't rollback transaction, error code %d\n", r);
		goto fail_tr;
	}
	ret = EXIT_SUCCESS;

fail_tr:
	tkvdb_tr_free(tr);
fail_db:
	tkvdb_close(db);
fail:
	return ret;
}

