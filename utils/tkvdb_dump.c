/*
 * tkvdb-dump
 *
 * Copyright (c) 2019, Vladimir Misyurov
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
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include "tkvdb.h"

/* min transaction buffer size */
#define MIN_TR_SIZE 100000

/* transaction size, 100M by default */
#define DEF_TR_SIZE (100 * 1024 * 1024)
static size_t trsize = DEF_TR_SIZE;

static void
print_sym(FILE *out, int sym)
{
	if (sym == '\n') {
		fputs("\\n", out);
	} else if (sym == '"') {
		fputs("\\\"", out);
	} else if (sym == '\\') {
		fputs("\\\\", out);
	} else {
		fputc(sym, out);
	}
}

static void
print_kv_pair(FILE *out, tkvdb_cursor *c)
{
	size_t i;
	uint8_t *data;

	/* print key */
	data = c->key(c);
	fputc('"', out);
	for (i=0; i<c->keysize(c); i++) {
		print_sym(out, data[i]);
	}
	/* value */
	data = c->val(c);
	fputs("\":\"", out);
	for (i=0; i<c->valsize(c); i++) {
		print_sym(out, data[i]);
	}
	fputs("\"\n", out);
}

static void
print_usage(char *progname)
{
	fprintf(stderr,
		"Usage:\n %s [-o out_file] [-r] [-s size] db.tkvdb\n",
		progname);
	fprintf(stderr, " %s -h\n", progname);
	fprintf(stderr, "    out_file - name of output file "\
		"(default to stdout)\n");
	fprintf(stderr, "    size - size of transaction buffer in bytes "\
		"(default %lu, min %d)\n",
		(unsigned long int)DEF_TR_SIZE, MIN_TR_SIZE);
	fprintf(stderr, "    -r - dump in reverse order\n");
	fprintf(stderr, "    -h - print this message\n");
}

int
main(int argc, char *argv[])
{
	tkvdb *db;
	tkvdb_tr *tr;
	TKVDB_RES rc;
	tkvdb_params *params;
	tkvdb_cursor *c;

	int opt;
	int reverse = 0;
	FILE *out = stdout;
	char *outfile = NULL;
	char *db_file;
	int ret = EXIT_FAILURE;

	uint8_t *last_key = NULL;
	size_t last_key_size = 0;

	while ((opt = getopt(argc, argv, "ho:rs:")) != -1) {
		switch (opt) {
			case 'o':
				outfile = optarg;
				break;
			case 'r':
				reverse = 1;
				break;
			case 's':
				trsize = atoll(optarg);
				break;

			case 'h':
			default:
				print_usage(argv[0]);
				return EXIT_SUCCESS;
		}
	}

	if (trsize < MIN_TR_SIZE) {
		print_usage(argv[0]);
		return EXIT_SUCCESS;
	}

	if ((argc - optind) != 1) {
		/* only one non-option argument */
		print_usage(argv[0]);
		return EXIT_SUCCESS;
	}

	db_file = argv[optind];

	if (outfile) {
		out = fopen(outfile, "wb");
		if (!out) {
			fprintf(stderr, "Can't open output file '%s': %s",
				outfile, strerror(errno));
			goto fail_out;
		}
	}
#ifdef _WIN32
	else {
		/* set stdout to binary under Windows */
		setmode(fileno(stdout), O_BINARY);
	}
#endif
	/* init database parameters */
	params = tkvdb_params_create();
	if (!params) {
		fprintf(stderr, "Can't create database parameters\n");
		goto fail_params;
	}

	/* no dynamic reallocation of transaction buffer */
	tkvdb_param_set(params, TKVDB_PARAM_TR_DYNALLOC, 0);
	/* buffer size */
	tkvdb_param_set(params, TKVDB_PARAM_TR_LIMIT, trsize);
	/* don't try to create database, open read-only */
#ifndef _WIN32
	tkvdb_param_set(params, TKVDB_PARAM_DBFILE_OPEN_FLAGS, O_RDONLY);
#else
	tkvdb_param_set(params, TKVDB_PARAM_DBFILE_OPEN_FLAGS,
		O_RDONLY | O_BINARY);
#endif

	/* open database */
	db = tkvdb_open(db_file, params);
	tkvdb_params_free(params);
	if (!db) {
		int err = errno;
		fprintf(stderr, "Can't open db file '%s': %s\n",
			db_file, err ? strerror(err): "corrupted database");
		goto fail_dbopen;
	}

	/* create in-memory transaction with fixed buffer size */
	tr = tkvdb_tr_create(db, NULL);
	if (!tr) {
		fprintf(stderr, "Can't create transaction\n");
		goto fail_tr;
	}

	/* start transaction */
	tr->begin(tr);

	c = tkvdb_cursor_create(tr);
	if (!c) {
		fprintf(stderr, "Can't create cursor\n");
		goto fail_cursor;
	}

	/* seek to first (or last) k-v pair */
	rc = reverse ? c->last(c) : c->first(c);

	for (;;) {
		tkvdb_datum dtk;

		while (rc == TKVDB_OK) {
			print_kv_pair(out, c);

			/* save last key */
			dtk.size = c->keysize(c);
			if (last_key_size < dtk.size) {
				uint8_t *tmp;

				tmp = realloc(last_key, dtk.size);
				if (!tmp) {
					fprintf(stderr, "realloc() failed\n");
					goto fail_next;
				}
				last_key = tmp;
				last_key_size = dtk.size;
			}
			dtk.data = last_key;
			memcpy(last_key, c->key(c), dtk.size);

			rc = reverse ? c->prev(c) : c->next(c);
		}

		if (rc == TKVDB_ENOMEM) {
			/* transaction buffer overflow */

			/* reset transaction */
			tr->rollback(tr);
			tr->begin(tr);

			/* and start from last seen key */
			rc = c->seek(c, &dtk, TKVDB_SEEK_EQ);
			if (rc != TKVDB_OK) {
				fprintf(stderr,
					"seek() failed with code %d\n", rc);
				goto fail_seek;
			}

			rc = reverse ? c->prev(c) : c->next(c);
		} else if ((rc == TKVDB_NOT_FOUND) || (rc == TKVDB_EMPTY)) {
			/* end of data or empty database */
			break;
		} else {
			fprintf(stderr, "Error occured during dump,"\
				" code %d\n", rc);
			goto fail_next;
		}
	}

	ret = EXIT_SUCCESS;

fail_next:
fail_seek:
	free(last_key);
	c->free(c);
fail_cursor:
	tr->free(tr);
fail_tr:
	tkvdb_close(db);
fail_dbopen:
fail_params:
	if (outfile) {
		fclose(out);
	}
fail_out:
	return ret;
}

