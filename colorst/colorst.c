#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#include "colorst.h"
#include "colorst_impl.h"


static void colorst_free(colorst *c);
static int  colorst_prepare(colorst *c, const char *query,
	char *message, size_t msgsize);

colorst *
colorst_create(const char *query, tkvdb_tr *tr,
	int *retcode, char *message, size_t msgsize)
{
	colorst *c;
	struct colorst_data *cdt;

	c = malloc(sizeof(colorst));
	if (!c) {
		goto malloc_fail;
	}

	cdt = malloc(sizeof(struct colorst_data));
	if (!cdt) {
		goto malloc_data_fail;
	}
	cdt->tr = tr;
	c->data = cdt;

	if (!colorst_prepare(c, query, message, msgsize)) {
		goto prepare_fail;
	}

	c->free = &colorst_free;
	*retcode = 1;

	return c;

prepare_fail:
malloc_data_fail:
	free(c);
malloc_fail:
	*retcode = 0;
	return NULL;
}

static void
colorst_free(colorst *c)
{
	free(c->data);
	c->data = NULL;

	free(c);
}

static int
colorst_prepare(colorst *c, const char *query, char *message, size_t msgsize)
{
	struct input i;

	/* init input */
	i.s = query;
	i.eof = 0;
	i.line = i.col = 1;

	i.error = 0;
	i.errmsg = message;
	i.msgsize = msgsize;

	i.data = c->data;

	parse_query(&i);

	if (i.error) {
		return 0;
	}

	return 1;
}

void
mkerror(struct input *i, char *msg)
{
	i->error = 1;

	if (i->line > 1) {
		snprintf(i->errmsg, i->msgsize, "Line %d, col %d: %s",
			i->line, i->col, msg);
	} else {
		snprintf(i->errmsg, i->msgsize, "Col: %d: %s", i->col, msg);
	}
}

int
colorst_create_collection(tkvdb_tr *tr, const char *coll_name, char *msg)
{
	tkvdb_datum dtk, dtv;
	char *key;
	size_t keylen;
	uint32_t cpfx = COLORST_PREFIX_COLLECTIONS;
	TKVDB_RES rc;

	keylen = sizeof(cpfx) + strlen(coll_name);

	key = alloca(keylen);

	/* search for collection with the same name */
	/* prepare search string:
	   COLORST_PREFIX_COLLECTIONS (4 bytes) + collection name */
	memcpy(key, &cpfx, sizeof(cpfx));
	memcpy(key + sizeof(cpfx), coll_name, keylen - sizeof(cpfx));

	dtk.data = key;
	dtk.size = keylen;
	rc = tr->get(tr, &dtk, &dtv);

	if (rc == TKVDB_OK) {
		strcpy(msg, "Collection already exists");
		return 0;
	}

	if ((rc == TKVDB_NOT_FOUND) || (rc == TKVDB_EMPTY)) {
		/* add new collection */
		uint32_t colid = 0, *dbcolid;
		tkvdb_cursor *c;
		tkvdb_datum dtks;

		/* iterate through collection names */
		c = tkvdb_cursor_create(tr);
		if (!c) {
			strcpy(msg, "Can't create cursor");
			return 0;
		}
		dtks.data = &cpfx;
		dtks.size = sizeof(cpfx);
		rc = c->seek(c, &dtks, TKVDB_SEEK_GE);
		while (rc == TKVDB_OK) {
			if (c->keysize(c) <= sizeof(cpfx)) {
				/* key is too short */
				break;
			}
			if (memcmp(c->key(c), &cpfx, sizeof(cpfx)) != 0) {
				/* another prefix */
				break;
			}
			if (c->valsize(c) != sizeof(colid)) {
				/* value is not collection id. error? */
				break;
			}

			dbcolid = (uint32_t *)c->val(c);
			/* looking for max collection id */
			if (*dbcolid > colid) {
				colid = *dbcolid;
			}
			rc = c->next(c);
		}
		colid++;

		/* now put collection and id */
		dtv.data = &colid;
		dtv.size = sizeof(colid);
		rc = tr->put(tr, &dtk, &dtv);
		if (rc == TKVDB_OK) {
			sprintf(msg, "CREATE COLLECTION %s, ID %d",
				coll_name, (int)colid);
			return 1;
		}
	}

	sprintf(msg, "DB error, code %d", rc);

	return 0;
}

