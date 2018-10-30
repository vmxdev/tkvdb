#include <stdio.h>
#include <string.h>
#include <stdlib.h>

#include "colorst_impl.h"

static int
accept(struct input *i, enum COLORST_TOKEN token)
{
	if (i->current_token.id == token) {
		read_token(i);
		if (i->error) {
			return 0;
		}
		return 1;
	}
	return 0;
}

static int
expect(struct input *i, enum COLORST_TOKEN token)
{
	if (accept(i, token)) {
		return 1;
	}
	/* unexpected token */
	return 0;
}

static void
json_like_value(struct input *i, int object, size_t prefixsize)
{
	struct colorst_data *data;

	data = i->data;

	for (;;) {
		char fullfieldname[TOKEN_MAX_SIZE], fieldname[TOKEN_MAX_SIZE];
		struct field *tmpfields;
		size_t fidx, fieldsize;

		fieldsize = strlen(i->current_token.str);
		memcpy(fieldname, i->current_token.str, fieldsize);

		if (prefixsize) {
			memcpy(fullfieldname, data->fl.prefix, prefixsize);
		}
		memcpy(fullfieldname + prefixsize, fieldname, fieldsize);
		fullfieldname[prefixsize + fieldsize] = '\0';

		if (!expect(i, COLORST_ID)) {
			mkerror(i,
				"Expected field name");
			return;
		}
		if (!expect(i, COLORST_COLON)) {
			mkerror(i,
				"Expected ':' after field name");
			return;
		}

		/* add field to list */
		tmpfields = realloc(data->fl.fields, sizeof(struct field)
			* (data->fl.nfields + 1));
		if (!tmpfields) {
			mkerror(i, "Insufficient memory");
			return;
		}
		data->fl.fields = tmpfields;
		fidx = data->fl.nfields;
		data->fl.nfields++;

		data->fl.fields[fidx].namesize = prefixsize + fieldsize;
		memcpy(data->fl.fields[fidx].name, fullfieldname,
			data->fl.fields[fidx].namesize);
		data->fl.fields[fidx].name[data->fl.fields[fidx].namesize]
			= '\0';

		if (accept(i, COLORST_ID)) {
			data->fl.fields[fidx].type = COLORST_FIELD_ID;
			data->fl.fields[fidx].valsize
				= strlen(i->current_token.str);
		} else if (accept(i, COLORST_INT)) {
			int64_t val;

			data->fl.fields[fidx].type = COLORST_FIELD_INT;
			data->fl.fields[fidx].valsize = sizeof(val);
			val = atol(i->current_token.str);
			memcpy(data->fl.fields[fidx].val, &val, sizeof(val));
		} else if (accept(i, COLORST_STRING)) {
			data->fl.fields[fidx].type = COLORST_FIELD_STRING;
			data->fl.fields[fidx].valsize
				= strlen(i->current_token.str);
		} else if (accept(i, COLORST_CURLY_BRACKET_OPEN)) {
			data->fl.fields[fidx].type = COLORST_FIELD_OBJECT;
			/* append prefix */
			memcpy(data->fl.prefix + prefixsize,
				fieldname, fieldsize);
			/* and dot */
			data->fl.prefix[prefixsize + fieldsize] = '.';

			json_like_value(i, 1, prefixsize + fieldsize + 1);
		} else {
			mkerror(i,
				"Expected ID, integer, string or object"
				" after ':'");
			return;
		}

		if (accept(i, COLORST_COMMA)) {
			continue;
		}

		if (object) {
			if (accept(i, COLORST_CURLY_BRACKET_CLOSE)) {
				/* end of object */
				break;
			}
		}

		if (i->eof) {
			break;
		}


		/* not eof, not comma */
		mkerror(i,
			"Expected comma or EOF after field and value");
		return;
	}
}

static void
insert(struct input *i)
{
	struct colorst_data *data;

	data = i->data;

	if (!expect(i, COLORST_INTO)) {
		mkerror(i, "Expected INTO after INSERT");
		return;
	}

	/* collection name */
	strncpy(data->collection, i->current_token.str, TOKEN_MAX_SIZE);
	if (!expect(i, COLORST_ID)) {
		mkerror(i, "Expected COLLECTION after INSERT INTO");
		return;
	}

	if (!expect(i, COLORST_VALUE)) {
		mkerror(i,
			"Expected VALUE after INSERT INTO COLLECTION");
		return;
	}

	json_like_value(i, 0, 0);
	if (i->error) {
		return;
	}

	if (!colorst_prepare_insert(i)) {
		mkerror(i, "Can't create collection");
	}
}

static void
create_collection(struct input *i)
{
	char collection[TOKEN_MAX_SIZE];
	char msg[TOKEN_MAX_SIZE];

	if (!expect(i, COLORST_COLLECTION)) {
		mkerror(i, "Expected COLLECTION after CREATE");
		return;
	}

	strncpy(collection, i->current_token.str, sizeof(collection));
	if (!expect(i, COLORST_ID)) {
		mkerror(i, "Expected collection name after CREATE COLLECTION");
		return;
	}

	colorst_create_collection(i->data->tr, collection, NULL, msg);
	printf("%s\n", msg);
}

void
parse_query(struct input *i)
{
	read_token(i);
	if (accept(i, COLORST_CREATE)) {
		create_collection(i);
	} else if (accept(i, COLORST_INSERT)) {
		insert(i);
	} else if (accept(i, COLORST_SELECT)) {
	} else if (accept(i, COLORST_UPDATE)) {
	} else {
		mkerror(i, "Unexpected token");
	}
}

