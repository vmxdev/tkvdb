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
	for (;;) {
		char fullfieldname[TOKEN_MAX_SIZE], fieldname[TOKEN_MAX_SIZE];
		struct field *tmpfields;
		size_t fidx, fieldsize;

		fieldsize = strlen(i->current_token.str);
		memcpy(fieldname, i->current_token.str, fieldsize);

		if (prefixsize) {
			memcpy(fullfieldname, i->fl.prefix, prefixsize);
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
		printf("field: %s: ", fullfieldname);
		tmpfields = realloc(i->fl.fields, sizeof(struct field)
			* (i->fl.nfields + 1));
		if (!tmpfields) {
			mkerror(i, "Insufficient memory");
			return;
		}
		i->fl.fields = tmpfields;
		fidx = i->fl.nfields;
		i->fl.nfields++;

		i->fl.fields[fidx].namesize = prefixsize + fieldsize;
		memcpy(i->fl.fields[fidx].name, fullfieldname,
			i->fl.fields[fidx].namesize);

		if (accept(i, COLORST_ID)) {
			i->fl.fields[fidx].type = COLORST_FIELD_ID;
			printf("id\n");
		} else if (accept(i, COLORST_INT)) {
			i->fl.fields[fidx].type = COLORST_FIELD_INT;
			printf("int\n");
		} else if (accept(i, COLORST_STRING)) {
			i->fl.fields[fidx].type = COLORST_FIELD_STRING;
			printf("string\n");
		} else if (accept(i, COLORST_CURLY_BRACKET_OPEN)) {
			printf("object\n");
			/* append prefix */
			memcpy(i->fl.prefix + prefixsize,
				fieldname, fieldsize);
			/* and dot */
			i->fl.prefix[prefixsize + fieldsize] = '.';

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
				printf("end of object\n");
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
	char collection[TOKEN_MAX_SIZE];

	if (!expect(i, COLORST_INTO)) {
		mkerror(i, "Expected INTO after INSERT");
		return;
	}

	/* collection name */
	strncpy(collection, i->current_token.str, sizeof(collection));
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

	colorst_prepare_insert(i);
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

	colorst_create_collection(i->data->tr, collection, msg);
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

