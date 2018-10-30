#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#include "colorst.h"
#include "colorst_impl.h"

static TKVDB_RES
prepare_field(tkvdb_tr *tr, uint32_t *fieldidptr, uint32_t collid,
	struct field *fld)
{
	char *key, *kptr;
	size_t keylen;
	uint32_t pfx = COLORST_PREFIX_FIELDS;
	uint32_t fieldid;
	TKVDB_RES rc;
	tkvdb_datum dtk, dtv;
	tkvdb_datum dtfld, dtfldval;

	/* prefix + collection id + field name + type */
	keylen = sizeof(pfx) + sizeof(collid) + fld->namesize
		+ sizeof(fld->type);

	key = alloca(keylen);
	kptr = key;

	/* COLORST_PREFIX_FIELDS */
	memcpy(kptr, &pfx, sizeof(pfx));
	kptr += sizeof(pfx);

	/* collection id */
	memcpy(kptr, &collid, sizeof(collid));
	kptr += sizeof(collid);

	/* field name */
	memcpy(kptr, fld->name, fld->namesize);
	kptr += fld->namesize;

	/* type */
	memcpy(kptr, &fld->type, sizeof(fld->type));

	/* search for field */
	dtk.data = key;
	dtk.size = keylen;
	rc = tr->get(tr, &dtk, &dtv); 

	if (rc == TKVDB_OK) {
		/* found */
		*fieldidptr = *((uint32_t *)dtv.data);
		return TKVDB_OK;
	}

	if (rc != TKVDB_NOT_FOUND) {
		return rc;
	}

	/* add new collection field to database */

	/* get field id */
	dtfld.data = &pfx;
	dtfld.size = sizeof(pfx);

	rc = tr->get(tr, &dtfld, &dtfldval);
	if (rc == TKVDB_OK) {
		fieldid = *((uint32_t *)dtfldval.data);
		fieldid++;
		*((uint32_t *)dtfldval.data) = fieldid;
	} else if (rc == TKVDB_NOT_FOUND) {
		fieldid = COLORST_PREFIX_DATA_START;

		dtfldval.data = &fieldid;
		dtfldval.size = sizeof(fieldid);

		rc = tr->put(tr, &dtfld, &dtfldval);
		if (rc != TKVDB_OK) {
			return rc;
		}
	} else {
		/* error */
		return rc;
	}

	/* put new field */
	dtfldval.data = &fieldid;
	dtfldval.size = sizeof(fieldid);

	rc = tr->put(tr, &dtk, &dtfldval);
	if (rc == TKVDB_OK) {
		*fieldidptr = fieldid;
	}

	return rc;
}

int
colorst_prepare_insert(struct input *i)
{
	size_t fidx;
	struct colorst_data *data;
	int coll_create_res;
	char msg[TOKEN_MAX_SIZE];
	uint32_t collid;
	uint32_t countpfx = COLORST_PREFIX_COLLROWS;
	TKVDB_RES rc;
	tkvdb_datum dtk, dtv;
	tkvdb_tr *tr;
	char *key;
	size_t keylen;
	uint64_t *rowidptr; /* number of rows in collection */
	uint64_t nrowid;    /* rowid in network byte order */
	tkvdb_cursor *c;
	tkvdb_datum dtvone;
	uint64_t one = 1;

	data = i->data;
	tr = data->tr;

	c = tkvdb_cursor_create(tr);
	if (!c) {
		return 0;
	}

	/* check if collection exists and create if not */
	coll_create_res = colorst_create_collection(tr, data->collection,
		&collid, msg);

	if (!coll_create_res) {
		return 0;
	}

	/* get number of rows in collection */
	keylen = sizeof(countpfx) + sizeof(collid);
	key = alloca(keylen);

	/* COLORST_PREFIX_COLLROWS */
	memcpy(key, &countpfx, sizeof(countpfx));
	/* collection id */
	memcpy(key + sizeof(countpfx), &collid, sizeof(collid));

	dtk.data = key;
	dtk.size = keylen;

	rc = tr->get(tr, &dtk, &dtv);
	if (rc == TKVDB_OK) {
		rowidptr = dtv.data;
	} else if (rc == TKVDB_NOT_FOUND) {
		uint64_t rowid = 0;

		dtv.data = &rowid;
		dtv.size = sizeof(rowid);
		rc = tr->put(tr, &dtk, &dtv);
		if (rc != TKVDB_OK) {
			return 0;
		}

		rc = tr->get(tr, &dtk, &dtv);
		if (rc != TKVDB_OK) {
			return 0;
		}
		rowidptr = dtv.data;
	} else {
		return 0;
	}

	nrowid = htobe64(*rowidptr);

	dtvone.data = &one;
	dtvone.size = sizeof(one);

	for (fidx=0; fidx<data->fl.nfields; fidx++) {
		uint32_t fieldid;
		char *rowkey, *rowkeyptr;
		size_t rowkeylen;
		tkvdb_datum dtkrow;

/*
		printf("field: %s, type ", data->fl.fields[fidx].name);
		switch (data->fl.fields[fidx].type) {
			case COLORST_FIELD_ID:
				printf("ID");
				break;
			case COLORST_FIELD_INT:
				printf("INT");
				break;
			case COLORST_FIELD_STRING:
				printf("STRING");
				break;
			case COLORST_FIELD_OBJECT:
				printf("OBJECT");
				break;
			default:
				printf("UNKNOWN");
				break;
		}
		printf("\n");
*/

		if (data->fl.fields[fidx].type == COLORST_FIELD_OBJECT) {
			continue;
		}

		rc = prepare_field(data->tr, &fieldid, collid,
			&data->fl.fields[fidx]);

		if (rc != TKVDB_OK) {
			return 0;
		}

		/* now add row */
		/* field id + value + rowid */
		rowkeylen = sizeof(fieldid) + data->fl.fields[fidx].valsize
			+ sizeof(nrowid);

		rowkey = alloca(rowkeylen);
		rowkeyptr = rowkey;

		/* field id */
		memcpy(rowkeyptr, &fieldid, sizeof(fieldid));
		rowkeyptr += sizeof(fieldid);

		/* value */
		memcpy(rowkeyptr, data->fl.fields[fidx].val,
			data->fl.fields[fidx].valsize);
		rowkeyptr += data->fl.fields[fidx].valsize;

		/* row id in network byte order */
		memcpy(rowkeyptr, &nrowid, sizeof(nrowid));

		dtkrow.data = rowkey;
		dtkrow.size = rowkeylen;

		/* search for the same value but with lesser row id */
		rc = c->seek(c, &dtkrow, TKVDB_SEEK_LE);
		if (rc != TKVDB_OK) {
			return 0;
		}

		/* compare values */
		if (c->keysize(c) == rowkeylen) {
			int mr = memcmp(c->key(c), rowkey,
				rowkeylen - sizeof(nrowid));
			if (mr == 0) {
				/* equal values */
				uint64_t prevrowid;
				char *prevrowidptr;
				uint64_t *nrowsptr;

				/* get last 8 bytes (64 bits) of key (rowid) */
				prevrowidptr = (char *)c->key(c)
					+ rowkeylen - sizeof(nrowid);

				prevrowid =
					be64toh(*((uint64_t *)prevrowidptr));
				nrowsptr = (uint64_t *)c->val(c);

				if ((prevrowid + *nrowsptr)
					== (*rowidptr - 1)) {
					/* just increment number of rows */
					(*nrowsptr)++;
					continue;
				}
			}
		}
		/* add new value */
		rc = tr->put(tr, &dtkrow, &dtvone);
		if (rc != TKVDB_OK) {
			return rc;
		}
	}

	/* increment number of rows in collection */
	(*rowidptr)++;

	c->free(c);

	return 1;
}

