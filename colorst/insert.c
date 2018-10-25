#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#include "colorst.h"
#include "colorst_impl.h"

static TKVDB_RES
prepare_field(tkvdb_tr *tr, uint32_t collid,
	char *field_name, size_t field_size,
	uint32_t type)
{
	char *key, *kptr;
	size_t keylen;
	uint32_t pfx = COLORST_PREFIX_FIELDS;
	TKVDB_RES rc;
	tkvdb_datum dtk, dtv;

	/* prefix + collection id + type + field name */
	keylen = sizeof(pfx) + sizeof(collid) + sizeof(type) + field_size;

	key = alloca(keylen);
	kptr = key;

	/* COLORST_PREFIX_FIELDS */
	memcpy(kptr, &pfx, sizeof(pfx));
	kptr += sizeof(pfx);

	/* collection id */
	memcpy(kptr, &collid, sizeof(collid));
	kptr += sizeof(collid);

	/* type */
	memcpy(kptr, &type, sizeof(type));
	kptr += sizeof(type);

	/* field name */
	memcpy(kptr, field_name, field_size);

	/* search for field */
	dtk.data = key;
	dtk.size = keylen;
	rc = tr->get(tr, &dtk, &dtv); 

	if (rc == TKVDB_OK) {
		/* found */
		/* field_id = dtv.data; */
		return TKVDB_OK;
	}

	if (rc == TKVDB_NOT_FOUND) {
	}

	return TKVDB_OK;
}

int
colorst_prepare_insert(struct input *i)
{
	size_t fidx;
	struct colorst_data *data;
	int coll_create_res;
	char msg[TOKEN_MAX_SIZE];
	uint32_t collid;

	data = i->data;

	/* check if collection exists and create if not */
	coll_create_res = colorst_create_collection(data->tr,
		data->collection, &collid, msg);

	if (!coll_create_res) {
		return 0;
	}

	for (fidx=0; fidx<data->fl.nfields; fidx++) {
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
	}

	return 1;
}

