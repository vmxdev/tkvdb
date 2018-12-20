/* calculate words frequency */
#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <locale.h>
#include <wchar.h>
#include <wctype.h>
#include <unistd.h>
#include <string.h>
#include <inttypes.h>

#include "tkvdb.h"

/* min transaction buffer size */
#define MIN_TR_SIZE 100000

/* transaction size, 100M by default */
static size_t trsize = 100 * 1024 * 1024;
static char *db_file = "words.tkvdb";
static int verbose = 1;
static uint64_t nwords_total = 0, nwords_db = 0, nlines = 1;

static void
debug_commit_msg()
{
	if (verbose) {
		fprintf(stderr, "Flushing transaction"
			", lines of text %"PRIu64
			", words %"PRIu64
			", words in database %"PRIu64"\n",
			nlines, nwords_total, nwords_db);
	}
}

static int
add_word(tkvdb_tr *tr, tkvdb_datum *dtk)
{
	TKVDB_RES rc;
	tkvdb_datum dtv, one;
	uint64_t one64 = 1;

	one.data = &one64;
	one.size = sizeof(one64);

	nwords_total++;

	/* check if word is in database */
	rc = tr->get(tr, dtk, &dtv);
	if (rc == TKVDB_ENOMEM) {
		/* transaction buffer overflow */
		debug_commit_msg();

		rc = tr->commit(tr);
		if (rc != TKVDB_OK) {
			fprintf(stderr, "commit() failed with code %d\n", rc);
			return 0;
		}
		tr->begin(tr);

		/* try again */
		rc = tr->get(tr, dtk, &dtv);
	}

	if (rc == TKVDB_OK) {
		/* found, increment words counter */
		uint64_t *nwords;

		nwords = (uint64_t *)dtv.data;
		(*nwords)++;
		return 1;

	} else if ((rc != TKVDB_NOT_FOUND) && (rc != TKVDB_EMPTY)) {
		fprintf(stderr, "get() failed with code %d\n", rc);
		return 0;
	}

	/* not found, try to add new word */
	if (verbose > 1) {
		fprintf(stderr, "Adding word '%ls'\n", (wchar_t *)dtk->data);
	}

	rc = tr->put(tr, dtk, &one);
	if (rc == TKVDB_ENOMEM) {
		/* transaction buffer overflow */
		debug_commit_msg();

		rc = tr->commit(tr);
		if (rc != TKVDB_OK) {
			fprintf(stderr, "commit() failed with code %d\n", rc);
			return 0;
		}
		tr->begin(tr);

		/* try again */
		rc = tr->put(tr, dtk, &one);
	}

	if (rc != TKVDB_OK) {
		/* something went wrong */
		fprintf(stderr, "put() failed with code %d\n", rc);
		return 0;
	}

	nwords_db++;
	return 1;
}

static void
print_usage(char *progname)
{
	fprintf(stderr,
		"Usage:\n %s [-f db_file] [-l] [-s size] [-v verbosity] "
		"< file.txt\n",
		progname);
	fprintf(stderr, " %s -h\n", progname);
	fprintf(stderr, "    db_file - name of database file (default '%s')\n",
		db_file);
	fprintf(stderr, "    size - size of transaction buffer (default %zu,"
		" min %d)\n", trsize, MIN_TR_SIZE);
	fprintf(stderr, "    -l - convert letters to lowercase\n");
	fprintf(stderr, "    verbosity - level of debug messages"
		" (default %d)\n", verbose);
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

	wchar_t word[256];
	size_t wordlen = 0;
	int lower = 0, opt;

	while ((opt = getopt(argc, argv, "f:hls:v:")) != -1) {
		switch (opt) {
			case 'f':
				db_file = optarg;
				break;
			case 'l':
				lower = 1;
				break;
			case 's':
				trsize = atoll(optarg);
				break;
			case 'v':
				verbose = atoi(optarg);
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

	setlocale(LC_ALL, "");

	/* remove old database file */
	unlink(db_file);

	/* init database parameters */
	params = tkvdb_params_create();
	if (!params) {
		fprintf(stderr, "Can't create database parameters\n");
		return EXIT_FAILURE;
	}

	/* we don't need dynamic reallocation of transaction buffer */
	tkvdb_param_set(params, TKVDB_PARAM_TR_DYNALLOC, 0);
	/* set buffer size */
	tkvdb_param_set(params, TKVDB_PARAM_TR_LIMIT, trsize);
	/* align values */
	tkvdb_param_set(params, TKVDB_PARAM_ALIGNVAL, sizeof(uint64_t));

	/* open database */
	db = tkvdb_open(db_file, params);
	tkvdb_params_free(params);
	if (!db) {
		fprintf(stderr, "Can't open db file '%s': %s\n",
			db_file, strerror(errno));
		return EXIT_FAILURE;
	}

	/* create in-memory transaction with fixed buffer size */
	tr = tkvdb_tr_create(db, NULL);
	if (!tr) {
		fprintf(stderr, "Can't create transaction\n");

		tkvdb_close(db);
		return EXIT_FAILURE;
	}

	/* start */
	tr->begin(tr);

	/* read words from stdin and add them to database */
	/* each unique key is a word, value - number of occurrences */
	for (;;) {
		wchar_t sym;
		int addword = 0;
		tkvdb_datum dtk;

		sym = fgetwc(stdin);

		if (sym == L'\n') {
			nlines++;
		}

		if (iswspace(sym) || iswpunct(sym) || (sym == (wchar_t)WEOF)) {
			/* meet separator */
			if (wordlen) {
				/* and we have non-empty word */
				addword = 1;
			}
		} else {
			if (lower) {
				word[wordlen] = towlower(sym);
			} else {
				word[wordlen] = sym;
			}
			wordlen++;

			if (wordlen >= (sizeof(word) / sizeof(wchar_t) - 1)) {
				/* word is too big, add it */
				addword = 1;
			}
		}

		if (addword) {
			/* append zero terminator */
			word[wordlen] = L'\0';

			dtk.data = word;
			dtk.size = (wordlen + 1) * sizeof(wchar_t);

			if (!add_word(tr, &dtk)) {
				fprintf(stderr, "Aborting\n");
				return EXIT_FAILURE;
			}

			/* reset word length */
			wordlen = 0;
		}

		if (sym == (wchar_t)WEOF) {
			if (feof(stdin)) {
				break;
			} else {
				/* invalid character, try to recover */
				fgetc(stdin);
			}
		}
	}

	rc = tr->commit(tr);
	if (rc != TKVDB_OK) {
		fprintf(stderr, "commit() failed with code %d\n", rc);

		tr->free(tr);
		tkvdb_close(db);
		return EXIT_FAILURE;
	}

	/* now iterate over database key-value pairs using cursor */
	c = tkvdb_cursor_create(tr);
	if (!c) {
		fprintf(stderr, "Can't create cursor\n");

		tr->free(tr);
		tkvdb_close(db);
		return EXIT_FAILURE;
	}

	tr->begin(tr);
	/* seek to first k-v pair */
	rc = c->first(c);

	for (;;) {
		tkvdb_datum dtk;

		while (rc == TKVDB_OK) {
			printf("%10"PRIu64"  %ls\n", *((uint64_t *)c->val(c)),
				(wchar_t *)c->key(c));

			/* store key */
			dtk.data = word;
			dtk.size = c->keysize(c);
			memcpy(word, c->key(c), dtk.size);

			rc = c->next(c);
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
				return EXIT_FAILURE;
			}

			rc = c->next(c);
		} else {
			/* probably the end of data */
			break;
		}
	}

	/* cleanup */
	c->free(c);
	tr->free(tr);
	tkvdb_close(db);

	return EXIT_SUCCESS;
}

