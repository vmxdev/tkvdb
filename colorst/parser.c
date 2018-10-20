#include <stdio.h>
#include <string.h>

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
json_like_value(struct input *i, int object)
{
	for (;;) {
		char field[TOKEN_MAX_SIZE];

		strncpy(field, i->current_token.str, sizeof(field));
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

		printf("field: %s: ", field);
		if (accept(i, COLORST_ID)) {
			printf("id\n");
		} else if (accept(i, COLORST_INT)) {
			printf("int\n");
		} else if (accept(i, COLORST_STRING)) {
			printf("string\n");
		} else if (accept(i, COLORST_CURLY_BRACKET_OPEN)) {
			printf("object\n");
			json_like_value(i, 1);
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

		printf("error! token %s\n", i->current_token.str);
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

	json_like_value(i, 0);
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

