#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#include "colorst.h"
#include "colorst_impl.h"

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
		int *id;

		id = dtv.data;
		sprintf(msg, "Collection '%s' already exists, ID %d",
			coll_name, *id);
		return 1;
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

