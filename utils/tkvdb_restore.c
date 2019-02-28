/*
 * tkvdb-restore
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

#define SYM_APPEND(IN, C)            \
do {                                 \
	if (!sym_append(IN, C)) {    \
		IN->error = 1;       \
		return TOKEN_ENOMEM; \
	}                            \
} while (0)

static size_t trsize = DEF_TR_SIZE;

struct input
{
	FILE *f;
	size_t line;

	uint8_t *strtoken;
	size_t toksize;
	size_t tokallocated;

	int error;
};

enum TOKEN
{
	TOKEN_EOF,
	TOKEN_STRING,
	TOKEN_COLON,
	TOKEN_UNKNOWN,
	TOKEN_ENOMEM
};

static int
sym_append(struct input *in, int c)
{
	size_t new_size;
	uint8_t *token;

	if (in->toksize < in->tokallocated) {
		/* we have enough room for next symbol */
		in->strtoken[in->toksize] = c;
		in->toksize++;
		return 1;
	}

	new_size = in->tokallocated * 3 / 2;
	if (new_size <= in->tokallocated) {
		new_size = in->tokallocated + 1;
	}

	token = realloc(in->strtoken, new_size);
	if (!token) {
		/* XXX: free old data? */
		return 0;
	}

	in->strtoken = token;
	in->tokallocated = new_size;

	/* append */
	in->strtoken[in->toksize] = c;
	in->toksize++;
	return 1;
}

static int
scan_until_eol(struct input *in)
{
	for (;;) {
		int c;

		c = fgetc(in->f);

		if (c == '\n') {
			return 1;
		} else if (c == EOF) {
			return 0;
		}
	}
}

static enum TOKEN
scan_input(struct input *in)
{
	int c;

again:
	c = fgetc(in->f);
	if (c == EOF) {
		return TOKEN_EOF;
	} else if (c == '#') {
		/* comment */
		if (!scan_until_eol(in)) {
			return TOKEN_EOF;
		}
		in->line++;
		goto again;
	} else if ((c == ' ') || (c == '\t')) {
		/* whitespace */
		goto again;
	} else if (c == '\n') {
		/* new line */
		in->line++;
		goto again;
	} else if (c == '"') {
		/* string (key or value) */
		for (;;) {
			c = fgetc(in->f);
			if (c == EOF) {
				/* unexpected EOF */
				in->error = 1;
				return TOKEN_EOF;
			} else if (c == '"') {
				/* end of string */
				return TOKEN_STRING;
			} else if (c == '\\') {
				/* escaped symbol */
				c = fgetc(in->f);
				if (c == EOF) {
					/* unexpected EOF */
					in->error = 1;
					return TOKEN_EOF;
				} else if ((c == '\\') || (c == '"')) {
					/* append escaped symbol */
					SYM_APPEND(in, c);
				} else if (c == 'n') {
					SYM_APPEND(in, '\n');
				}
			} else {
				/* append symbol */
				SYM_APPEND(in, c);
			}
		}
	} else if (c == ':') {
		return TOKEN_COLON;
	} else {
		/* unknown token */
		in->error = 1;
		if (!scan_until_eol(in)) {
			return TOKEN_EOF;
		}
		in->line++;
		return TOKEN_UNKNOWN;
	}
}

static int
add_pair(struct input *in, tkvdb_tr *tr)
{
	enum TOKEN token;
	uint8_t *key, *val;
	size_t keysize, valsize;
	tkvdb_datum dtk, dtv;
	TKVDB_RES rc;

	token = scan_input(in);
	if (token == TOKEN_EOF) {
		if (in->error) {
			fprintf(stderr, "Unexpected EOF at line %lu\n",
				(unsigned long int)in->line);
		}
		return 0;
	} else if (token != TOKEN_STRING) {
		fprintf(stderr, "Expected quoted key at line %lu\n",
			(unsigned long int)in->line);
		return 0;
	}

	keysize = in->toksize;

	token = scan_input(in);
	if (token != TOKEN_COLON) {
		fprintf(stderr, "Expected ':' after key at line %lu\n",
			(unsigned long int)in->line);
		return 0;
	}

	token = scan_input(in);
	if (token != TOKEN_STRING) {
		fprintf(stderr, "Expected value after ':' at line %lu\n",
			(unsigned long int)in->line);
		return 0;
	}

	key = in->strtoken;
	val = in->strtoken + keysize;
	valsize = in->toksize - keysize;

	dtk.data = key;
	dtk.size = keysize;
	dtv.data = val;
	dtv.size = valsize;

	rc = tr->put(tr, &dtk, &dtv);
	if (rc == TKVDB_ENOMEM) {
		rc = tr->commit(tr);
		if (rc != TKVDB_OK) {
			fprintf(stderr, "commit() failed with code %d\n", rc);
			return 0;
		}
		tr->begin(tr);

		/* try again */
		rc = tr->put(tr, &dtk, &dtv);
	}

	if (rc != TKVDB_OK) {
		fprintf(stderr, "put() failed with code %d\n", rc);
		return 0;
	}

	in->error = 0;
	/* reset input token */
	in->toksize = 0;

	return 1;
}

static void
print_usage(char *progname)
{
	fprintf(stderr,
		"Usage:\n %s [-i in_file] [-s size] db.tkvdb\n",
		progname);
	fprintf(stderr, " %s -h\n", progname);
	fprintf(stderr, "    in_file - name of input dump file "\
		"(default stdin)\n");
	fprintf(stderr, "    size - size of transaction buffer in bytes "\
		"(default %lu, min %d)\n",
		(unsigned long int)DEF_TR_SIZE, MIN_TR_SIZE);
	fprintf(stderr, "    -h - print this message\n");
}

int
main(int argc, char *argv[])
{
	tkvdb *db;
	tkvdb_tr *tr;
	tkvdb_params *params;

	int opt;
	char *infile = NULL;
	char *db_file;
	int ret = EXIT_FAILURE;

	struct input in;

	in.f = stdin;
	in.line = 1;
	in.strtoken = NULL;
	in.toksize = in.tokallocated = 0;
	in.error = 0;

	while ((opt = getopt(argc, argv, "hi:s:")) != -1) {
		switch (opt) {
			case 'i':
				infile = optarg;
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

	if (infile) {
		in.f = fopen(infile, "rb");
		if (!in.f) {
			fprintf(stderr, "Can't open input file '%s': %s",
				infile, strerror(errno));
			goto fail_in;
		}
	}
#ifdef _WIN32
	else {
		/* set stdin to binary under Windows */
		setmode(fileno(stdin), O_BINARY);
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

	tr->begin(tr);

	while (add_pair(&in, tr)) {};

	tr->commit(tr);

	ret = EXIT_SUCCESS;

	free(in.strtoken);
	tr->free(tr);
fail_tr:
	tkvdb_close(db);
fail_dbopen:
fail_params:
	if (infile) {
		fclose(in.f);
	}
fail_in:
	return ret;
}

