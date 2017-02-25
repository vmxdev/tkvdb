#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include "tkvdb.h"

int
main(int argc, char *argv[])
{
	tkvdb *db;
	tkvdb_tr *tr;
	tkvdb_cursor *c;
	TKVDB_RES r;
	int ret = EXIT_FAILURE;

	if (argc < 2) {
		fprintf(stderr, "Usage: %s FILE.DB\n", argv[0]);
		goto fail;
	}
	db = tkvdb_open(argv[1], NULL);
	if (!db) {
		fprintf(stderr, "Can't open %s\n", argv[1]);
		goto fail_db;
	}
	tr = tkvdb_tr_create_m(db, 1024*1024, 0);

	r = tkvdb_begin(tr);
	if (r != TKVDB_OK) {
		printf("Can't start transaction, error code %d\n", r);
		goto fail_tr;
	}

	c = tkvdb_cursor_create(tr);

	r = tkvdb_last(c);
	if (r != TKVDB_OK) {
		printf("Can't get first element, error code %d\n", r);
		goto fail_cr;
	}

	{
		char buf[100];
		memcpy(buf, tkvdb_cursor_key(c), tkvdb_cursor_keysize(c));
		buf[tkvdb_cursor_keysize(c)] = '\0';
		printf("key: %s\n", buf);
	}

	for (;;) {
		r = tkvdb_prev(c);

		if (r == TKVDB_OK) {
			char buf[100];
			if (tkvdb_cursor_keysize(c)) {
				memcpy(buf, tkvdb_cursor_key(c),
					tkvdb_cursor_keysize(c));
				buf[tkvdb_cursor_keysize(c)] = '\0';
			} else {
				strcpy(buf, "(null)");
			}
			printf("key: %s\n", buf);
		} else {
			printf("eodb\n");
			break;
		}
	}
	r = tkvdb_rollback(tr);
	ret = EXIT_SUCCESS;

fail_cr:
	tkvdb_cursor_free(c);
fail_tr:
	tkvdb_tr_free(tr);
fail_db:
	tkvdb_close(db);
fail:
	return ret;
}

