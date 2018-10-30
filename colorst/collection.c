#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#include "colorst.h"
#include "colorst_impl.h"

int
colorst_create_collection(tkvdb_tr *tr, const char *coll_name,
	uint32_t *collidptr, char *msg)
{
	tkvdb_datum dtk, dtv;
	char *key;
	size_t keylen;
	uint32_t cpfx = COLORST_PREFIX_COLLECTIONS;
	TKVDB_RES rc;
	uint32_t collection_id;

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
		uint32_t *colid;

		colid = dtv.data;
		if (collidptr) {
			*collidptr = *colid;
		}
		sprintf(msg, "Collection '%s' already exists, ID %d",
			coll_name, *colid);
		return 1;
	}

	if (rc == TKVDB_EMPTY) {
		/* add autoincrement collection id
		   key (uint32_t): COLORST_PREFIX_COLLECTIONS
		   value(uint32_t): id */
		tkvdb_datum dtcol, dtcolval;

		collection_id = 1; /* start from 1 */

		dtcol.data = &cpfx;
		dtcol.size = sizeof(cpfx);

		dtcolval.data = &collection_id;
		dtcolval.size = sizeof(collection_id);

		rc = tr->put(tr, &dtcol, &dtcolval);
		if (rc != TKVDB_OK) {
			goto db_error;
		}
	} else if (rc == TKVDB_NOT_FOUND) {
		tkvdb_datum dtcol, dtcolval;

		/* fetch collection id from database */
		dtcol.data = &cpfx;
		dtcol.size = sizeof(cpfx);

		rc = tr->get(tr, &dtcol, &dtcolval);
		if (rc != TKVDB_OK) {
			goto db_error;
		}

		collection_id = *((uint32_t *)dtcolval.data);
		/* increment collection id */
		collection_id++;

		/* and put new value to database */
		*((uint32_t *)dtcolval.data) = collection_id;
	} else {
		/* error */
		goto db_error;
	}

	/* now put collection and id */
	dtv.data = &collection_id;
	dtv.size = sizeof(collection_id);

	rc = tr->put(tr, &dtk, &dtv);
	if (rc != TKVDB_OK) {
		goto db_error;
	}

	if (collidptr) {
		*collidptr = collection_id;
	}
	sprintf(msg, "CREATE COLLECTION %s, ID %d", coll_name,
		(int)collection_id);

	return 1;


db_error:
	sprintf(msg, "DB error, code %d", rc);

	return 0;
}

