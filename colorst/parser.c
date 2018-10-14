#include <stdio.h>
#include <string.h>

#include "colorst_impl.h"

static int
accept(struct input *i, enum COLORST_TOKEN token)
{
	if (i->current_token.id == token) {
		read_token(i);
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
}

static void
create_collection(struct input *i)
{
	char collection[TOKEN_MAX_SIZE];

	if (!expect(i, COLORST_COLLECTION)) {
		mkerror(i, "Expected COLLECTION after CREATE");
		return;
	}

	strncpy(collection, i->current_token.str, sizeof(collection));
	if (!expect(i, COLORST_ID)) {
		mkerror(i, "Expected collection name after CREATE COLLECTION");
		return;
	}
	printf("CREATE COLLECTION\n");
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
		mkerror(i, "Unexpected operator");
	}
}

