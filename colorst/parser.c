#include <stdio.h>

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

void
parse_query(struct input *i)
{
	read_token(i);
	if (accept(i, COLORST_INSERT)) {
		if (!expect(i, COLORST_INTO)) {
			i->error = 1;
			snprintf(i->errmsg, i->msgsize,
				"Expected INTO after INSERT");
			return;
		}
		if (!expect(i, COLORST_ID)) {
			i->error = 1;
			snprintf(i->errmsg, i->msgsize,
				"Expected COLLECTION after INSERT INTO");
			return;
		}
		if (!expect(i, COLORST_VALUE)) {
			i->error = 1;
			snprintf(i->errmsg, i->msgsize,
				"Expected VALUE after INSERT INTO COLLECTION");
			return;
		}

		printf("processing insert\n");
	} else if (accept(i, COLORST_SELECT)) {
	} else if (accept(i, COLORST_UPDATE)) {
	} else {
	}
}

